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
@time: 2024/12/29
@file: information_schema_tables_two_data.py
@desc: Check for duplicate table records. More: https://github.com/oceanbase/obdiag/issues/390
"""

from src.handler.check.check_task import TaskBase


class InformationSchemaTablesTwoDataTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = """
                select count(0) as err_count from oceanbase.__all_virtual_table_stat 
                where table_id = partition_id 
                and (tenant_id, table_id) in (
                    select tenant_id, table_id from oceanbase.__all_virtual_table where part_level != 0
                )
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    err_count = result[0].get('err_count', 0)
                    if err_count > 0:
                        self.report.add_critical(
                            'Find have table found two records in information_schema.tables. the number of err_table_count is: {0}. '
                            'Please get more info by "select * from oceanbase.__all_virtual_table_stat where table_id = partition_id '
                            'and (tenant_id,table_id) in (select tenant_id, table_id from oceanbase.__all_virtual_table where part_level != 0);". '
                            'And you can fix by "delete from __all_table_stat where table_id=partition_id and table_id=${{partition table table_id}};" '
                            'and "delete from __all_column_stat where table_id=partition_id and table_id=${{partition table table_id}};".'.format(err_count)
                        )
            except Exception as e:
                self.report.add_fail("Failed to check table records: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "information_schema_tables_two_data",
            "info": "Check for duplicate table records in information_schema.tables",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/390",
        }


information_schema_tables_two_data = InformationSchemaTablesTwoDataTask()
