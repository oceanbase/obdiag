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
@time: 2025/01/08
@file: global_index_unpartitioned.py
@desc: Check for unpartitioned global indexes that may cause hotspot issues
"""

from src.handler.check.check_task import TaskBase


class GlobalIndexUnpartitionedTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                self.report.add_fail("Database connection is not available")
                return
            # check obcluster version
            if super().check_ob_version_min("4.3.5.0"):
                # check
                sql = "select * from oceanbase.GV$OB_PARAMETERS where NAME='global_index_auto_split_policy' and VALUE ='OFF';"
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
                results = cursor.fetchall()
                if len(results) == 0:
                    pass
                else:
                    tenant_ids = []
                    for row in results:
                        TENANT_ID = row['TENANT_ID']
                        tenant_ids.append(TENANT_ID)

                    self.report.add_warning(
                        "tenant_id: {0} global_index_auto_split_policy is OFF.This may cause hotspot issues during batch operations.  Consider partitioning these indexes to distribute load across multiple OBServer nodes.".format(tenant_ids)
                    )
                return

            # Query for unpartitioned global indexes
            sql = """
                SELECT tenant_id, table_name, index_type, part_num 
                FROM oceanbase.__all_virtual_table 
                WHERE index_type IN (3, 4) 
                AND tenant_id <> 1 
                AND part_num = 1
            """

            try:
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
                results = cursor.fetchall()

                self.stdio.verbose("Found {0} unpartitioned global indexes".format(len(results)))

                if results:
                    # Check if count exceeds threshold
                    if len(results) > 10:
                        self.report.add_warning(
                            "Found {0} unpartitioned global indexes (exceeds threshold of 10). "
                            "This may cause hotspot issues during batch operations. "
                            "Consider partitioning these indexes to distribute load across multiple OBServer nodes.".format(len(results))
                        )
                        self.stdio.warn("Unpartitioned global index count exceeds threshold: {0}".format(len(results)))

                    # Log details of first 10 indexes
                    for i, row in enumerate(results[:10]):
                        tenant_id = row.get('tenant_id', 'Unknown')
                        table_name = row.get('table_name', 'Unknown')
                        index_type = row.get('index_type', 'Unknown')

                        self.stdio.verbose("Unpartitioned global index {0}: tenant_id={1}, table_name={2}, index_type={3}".format(i + 1, tenant_id, table_name, index_type))

                        # Add warning for each index
                        index_type_desc = "Global Index" if index_type == 3 else "Global Unique Index"
                        self.report.add_warning("Unpartitioned {0} found: tenant_id={1}, table_name={2}. " "This may cause hotspot issues during batch operations.".format(index_type_desc, tenant_id, table_name))
                else:
                    self.stdio.verbose("No unpartitioned global indexes found")

            except Exception as e:
                self.report.add_fail("Failed to query unpartitioned global indexes: {0}".format(e))
                self.stdio.warn("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "global_index_unpartitioned",
            "info": "Check for unpartitioned global indexes that may cause hotspot issues during batch operations",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/957",
        }


global_index_unpartitioned = GlobalIndexUnpartitionedTask()
