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
@file: global_indexes_too_much.py
@desc: Check whether there is a table with more than 20 global indexes
"""

from src.handler.check.check_task import TaskBase


class GlobalIndexesTooMuchTask(TaskBase):
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
                SELECT COUNT(*) as count
                FROM oceanbase.DBA_PART_INDEXES
                WHERE LOCALITY = 'LOCAL'
                GROUP BY TABLE_NAME
                ORDER BY count DESC limit 1
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    max_count = result[0].get('count', 0)
                    if max_count > 20:
                        self.report.add_warning("There is a table with more than 20 global indexes (max: {0})".format(max_count))
            except Exception as e:
                self.report.add_fail("Failed to check global indexes: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "global_indexes_too_much", "info": "Check whether there is a table with more than 20 global indexes."}


global_indexes_too_much = GlobalIndexesTooMuchTask()
