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
@time: 2025/10/15
@file: auto_split_error.py
@desc: Check tables with auto split enabled in the cluster and verify if their partition data size is smaller than the split threshold
"""
from src.handler.checker.check_task import TaskBase


class AutoSplitErrorTable(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        """
        Execute auto split check task
        1. Get tables with auto split enabled in the cluster
        2. Check if the partition data size of these tables is smaller than the split threshold
        """
        try:
            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")

            if super().check_ob_version_min("4.3.5.0") is False:
                self.stdio.verbose("AutoSplitErrorTable: observer version is less than 4.3.5.0, skip auto split error check")
                return

            # Step 1: Get tables with auto split enabled in the cluster
            auto_split_tables = self._get_auto_split_tables()
            if not auto_split_tables:
                self.stdio.verbose("No tables with auto split enabled found")
                return

            self.stdio.verbose(f"Found {len(auto_split_tables)} tables with auto split enabled")

            # Step 2: Check the partition data size of each table
            for table_info in auto_split_tables:
                self._check_table_split_condition(table_info)

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _get_auto_split_tables(self):
        """
        Get tables with auto split enabled in the cluster
        """
        sql = '''
        SELECT /*+READ_CONSISTENCY(WEAK)*/ 
               t.table_id, 
               t.auto_part_size,
               t.tenant_id,
               l.database_name,
               l.table_name
        FROM oceanbase.__all_virtual_table t
        LEFT JOIN oceanbase.cdb_ob_table_locations l 
        ON t.tenant_id = l.tenant_id AND t.table_id = l.table_id
        WHERE t.auto_part_size > 0
        AND t.tenant_id != 1
        GROUP BY t.table_id, t.auto_part_size, t.tenant_id, l.database_name, l.table_name;
        '''

        try:
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            auto_split_tables = []

            for row in result:
                table_info = {
                    "table_id": row.get("table_id") or row.get("TABLE_ID"),
                    "auto_part_size": row.get("auto_part_size") or row.get("AUTO_PART_SIZE"),
                    "tenant_id": row.get("tenant_id") or row.get("TENANT_ID"),
                    "database_name": row.get("database_name") or row.get("DATABASE_NAME") or "unknown",
                    "table_name": row.get("table_name") or row.get("TABLE_NAME") or "unknown",
                }
                auto_split_tables.append(table_info)

            return auto_split_tables

        except Exception as e:
            self.stdio.error("Failed to get auto split tables: {0}".format(e))
            return []

    def _check_table_split_condition(self, table_info):
        """
        Check the split condition for a single table
        """
        table_id = table_info["table_id"]
        auto_part_size = table_info["auto_part_size"]
        tenant_id = table_info["tenant_id"]
        database_name = table_info["database_name"]
        table_name = table_info["table_name"]

        try:
            # Step 1: Get all tablet_ids for this table
            tablet_ids = self._get_tablet_ids(table_id)
            if not tablet_ids:
                self.stdio.verbose(f"Table {database_name}.{table_name} (tenant_id: {tenant_id}, table_id: {table_id}) has no tablets")
                return

            # Step 2: Check if the data size of these tablets reaches the split threshold
            oversized_tablets = self._check_tablet_sizes(tablet_ids, auto_part_size)

            if oversized_tablets:
                # Found tablets that reach the split threshold, report warnings
                total_oversized_count = len(oversized_tablets)
                max_warnings = 10

                for i, tablet_info in enumerate(oversized_tablets):
                    if i >= max_warnings:
                        # If exceeding the maximum warning count, add a summary warning
                        remaining_count = total_oversized_count - max_warnings
                        self.report.add_warning(
                            "Table {0}.{1} has {2} tablets with size >= auto_part_size {3}MB, " "showing first {4} tablets, {5} more tablets not shown".format(database_name, table_name, total_oversized_count, auto_part_size, max_warnings, remaining_count)
                        )
                        break

                    self.report.add_warning(
                        "Table {0}.{1} has tablet {2} with size {3}MB >= auto_part_size {4}MB, " "should trigger auto split but hasn't".format(database_name, table_name, tablet_info["tablet_id"], tablet_info["required_size"], auto_part_size)
                    )
            else:
                self.stdio.verbose(f"Table {database_name}.{table_name} all tablets are within split threshold")

        except Exception as e:
            self.stdio.error("Failed to check table {0}.{1}: {2}".format(database_name, table_name, e))

    def _get_tablet_ids(self, table_id):
        """
        Get all tablet_ids for the specified table
        """
        sql = '''
        SELECT  tablet_id 
        FROM oceanbase.__all_virtual_tablet_to_table_history 
        WHERE table_id = {0}
        '''.format(
            table_id
        )

        try:
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            tablet_ids = []

            for row in result:
                tablet_id = row.get("tablet_id") or row.get("TABLET_ID")
                if tablet_id:
                    tablet_ids.append(tablet_id)

            return tablet_ids

        except Exception as e:
            self.stdio.error("Failed to get tablet IDs for table {0}: {1}".format(table_id, e))
            return []

    def _check_tablet_sizes(self, tablet_ids, auto_part_size):
        """
        Check if the data size of tablets reaches the split threshold
        """
        if not tablet_ids:
            return []

        # Build IN clause
        tablet_id_list = ','.join([str(tid) for tid in tablet_ids])

        sql = '''
        SELECT tablet_id,
               required_size
        FROM oceanbase.__all_virtual_tablet_pointer_status 
        WHERE tablet_id IN ({0}) 
        AND required_size >= {1}
        '''.format(
            tablet_id_list, auto_part_size
        )

        try:
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            oversized_tablets = []

            for row in result:
                tablet_info = {"tablet_id": row.get("tablet_id") or row.get("TABLET_ID"), "required_size": row.get("required_size") or row.get("REQUIRED_SIZE")}
                oversized_tablets.append(tablet_info)

            return oversized_tablets

        except Exception as e:
            self.stdio.error("Failed to check tablet sizes: {0}".format(e))
            return []

    def get_task_info(self):
        return {
            "name": "auto_split_error_table",
            "info": "Check if tables with auto split enabled have tablets that should trigger auto split but haven't. "
            "This check identifies tables where some tablets have reached the auto_part_size threshold "
            "but auto split hasn't been triggered, which may indicate issues with the auto split mechanism.",
        }


auto_split_error = AutoSplitErrorTable()
