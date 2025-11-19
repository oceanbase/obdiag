#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

"""
@time: 2025/04/30
@file: macroblock_utilization_rate_table.py
@desc:
"""
import threading
from src.handler.checker.check_task import TaskBase


class MacroblockUtilizationRateTable(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    # [0,0.5]
    def execute(self):
        try:

            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")
            if super().check_ob_version_min("4.0.0.0") is False:
                return

            # Set query timeout to 5 minutes (300 seconds = 300000000 microseconds)
            query_timeout_us = 300000000
            try:
                self.ob_connector.execute_sql("SET SESSION ob_query_timeout={0};".format(query_timeout_us))
                self.stdio.verbose("Set ob_query_timeout to {0} microseconds (5 minutes)".format(query_timeout_us))
            except Exception as e:
                self.stdio.warn("Failed to set ob_query_timeout: {0}".format(e))

            sql = '''
            SELECT /*+READ_CONSISTENCY(WEAK)*/ b.tenant_id,d.tenant_name,b.database_name,b.table_name,sum(c.occupy_size) / 1024 / 1024 / 1024 AS data_size_gb,count(distinct(macro_block_idx)) * 2 / 1024 AS required_size_gb FROM oceanbase.cdb_ob_table_locations b INNER JOIN (SELECT svr_ip,svr_port,tenant_id,ROW_COUNT,tablet_id,occupy_size,macro_block_idx FROM oceanbase.__all_virtual_tablet_sstable_macro_info GROUP BY svr_ip,svr_port,tenant_id,tablet_id,macro_block_idx) c ON b.tenant_id = c.tenant_id AND b.tablet_id = c.tablet_id LEFT JOIN oceanbase.dba_ob_tenants d ON d.tenant_id = b.tenant_id WHERE b.tenant_id <> 1 GROUP BY tenant_id, table_id, b.tablet_id ;
            '''

            # Execute query with thread timeout to prevent hanging
            result = [None]
            exception = [None]
            query_timeout_seconds = 300  # 5 minutes

            def execute_query():
                try:
                    cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
                    result[0] = cursor.fetchall()
                except Exception as e:
                    exception[0] = e

            query_thread = threading.Thread(target=execute_query)
            query_thread.daemon = True
            query_thread.start()
            query_thread.join(timeout=query_timeout_seconds)

            if query_thread.is_alive():
                error_msg = "Query execution timeout after {0} seconds. The SQL query may be too slow or the cluster is under heavy load. Please check the cluster status or try again later. issue #1067".format(query_timeout_seconds)
                self.stdio.error(error_msg)
                return self.report.add_fail(error_msg)

            if exception[0] is not None:
                error_msg = "Query execution failed: {0}".format(exception[0])
                self.stdio.error(error_msg)
                return self.report.add_fail(error_msg)

            if result[0] is None:
                error_msg = "Query returned no results"
                self.stdio.error(error_msg)
                return self.report.add_fail(error_msg)

            all_table = []

            for row in result[0]:
                tenant_name = row.get("tenant_name") or row.get("TENANT_NAME")
                database_name = row.get("database_name") or row.get("DATABASE_NAME")
                table_name = row.get("table_name") or row.get("TABLE_NAME")
                data_size_gb = row.get("data_size_gb") or row.get("DATA_SIZE_GB")
                required_size_gb = row.get("required_size_gb") or row.get("REQUIRED_SIZE_GB")

                if required_size_gb and required_size_gb > 1:
                    table_info = {
                        "tenantName": tenant_name,
                        "databaseName": database_name,
                        "tableName": table_name,
                        "dataSizeGb": data_size_gb,
                        "requiredSizeGb": required_size_gb,
                        "ratio": round(data_size_gb / required_size_gb, 2) if required_size_gb else 0,
                    }
                    all_table.append(table_info)

            # Sort the tables by the ratio in descending order
            all_table.sort(key=lambda x: x["ratio"], reverse=True)

            # Generate a report for the top 10 tables
            top_tables = all_table[:10]
            for table in top_tables:
                if table["ratio"] < 0.5:
                    self.report.add_warning(
                        "tenant: {0}, table: {1}, ratio: {2}, {3}. need major".format(
                            table["tenantName"],
                            f'{table["databaseName"]}.{table["tableName"]}',
                            table["ratio"],
                            f'dataSize: {round(table["dataSizeGb"], 2)}G, requiredSize: {round(table["requiredSizeGb"], 2)}G',
                        )
                    )
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "macroblock_utilization_rate_table",
            "info": "Check if the ratio of actual data volume to actual disk usage is within a certain range for all tables in the OceanBase cluster. OceanBase stores data in macroblocks. Each macroblock may not be fully utilized for efficiency. If the ratio of actual data volume to actual disk usage is too low, full consolidation should be performed to improve disk utilization. This task includes query timeout protection to prevent hanging. issue #848, issue #1067",
        }


macroblock_utilization_rate_table = MacroblockUtilizationRateTable()
