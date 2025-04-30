#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
import re
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
            sql = '''
            SELECT /*+READ_CONSISTENCY(WEAK)*/ b.tenant_id,d.tenant_name,b.database_name,b.table_name,sum(c.occupy_size) / 1024 / 1024 / 1024 AS data_size_gb,count(distinct(macro_block_idx)) * 2 / 1024 AS required_size_gb FROM oceanbase.cdb_ob_table_locations b INNER JOIN (SELECT svr_ip,svr_port,tenant_id,ROW_COUNT,tablet_id,occupy_size,macro_block_idx FROM oceanbase.__all_virtual_tablet_sstable_macro_info GROUP BY svr_ip,svr_port,tenant_id,tablet_id,macro_block_idx) c ON b.tenant_id = c.tenant_id AND b.tablet_id = c.tablet_id LEFT JOIN oceanbase.dba_ob_tenants d ON d.tenant_id = b.tenant_id WHERE b.tenant_id <> 1 GROUP BY tenant_id, table_id, b.tablet_id ;
            '''
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            all_table = []

            for row in result:
                tenant_name = row.get("tenant_name") or row.get("TENANT_NAME")
                database_name = row.get("database_name") or row.get("DATABASE_NAME")
                table_name = row.get("table_name") or row.get("TABLE_NAME")
                data_size_gb = row.get("data_size_gb") or row.get("DATA_SIZE_GB")
                required_size_gb = row.get("required_size_gb") or row.get("REQUIRED_SIZE_GB")

                if required_size_gb > 1:
                    table_info = {
                        "tenantName": tenant_name,
                        "databaseName": database_name,
                        "tableName": table_name,
                        "dataSizeGb": data_size_gb,
                        "requiredSizeGb": required_size_gb,
                        "ratio": round(data_size_gb / required_size_gb, 2),
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
            "info": "Check if the ratio of actual data volume to actual disk usage is within a certain range for all tables in the OceanBase cluster. OceanBase stores data in macroblocks. Each macroblock may not be fully utilized for efficiency. If the ratio of actual data volume to actual disk usage is too low, full consolidation should be performed to improve disk utilization. issue #848",
        }


macroblock_utilization_rate_table = MacroblockUtilizationRateTable()
