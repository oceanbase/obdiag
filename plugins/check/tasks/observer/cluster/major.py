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
@file: major.py
@desc: Check whether there is any suspended major compaction process
"""

from src.handler.check.check_task import TaskBase


class MajorTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: >= 4.0.0.0
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check for major compaction errors
            sql_error = 'select count(0) as cnt from oceanbase.CDB_OB_MAJOR_COMPACTION where IS_ERROR="YES"'
            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_error).fetchall()
                if result and len(result) > 0:
                    error_count = result[0].get('cnt', 0)
                    if error_count > 0:
                        self.report.add_critical("major have error")
                        self.stdio.warn("Found {0} major compaction errors".format(error_count))
            except Exception as e:
                self.report.add_fail("Failed to check major compaction errors: {0}".format(e))
                self.stdio.error("SQL execution error: {0}".format(e))

            # Check for major compaction holds (running > 36 hours)
            sql_hold = '''
                SELECT COUNT(*) AS cnt
                FROM (
                    SELECT START_TIME, TIMESTAMPDIFF(HOUR, START_TIME, CURRENT_TIMESTAMP) AS diff
                    FROM oceanbase.CDB_OB_MAJOR_COMPACTION
                    WHERE STATUS = "COMPACTING" AND TIMESTAMPDIFF(HOUR, START_TIME, CURRENT_TIMESTAMP) > 36
                ) AS subquery
            '''
            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_hold).fetchall()
                if result and len(result) > 0:
                    hold_count = result[0].get('cnt', 0)
                    if hold_count > 0:
                        self.report.add_critical('major have hold. please check it. And you can execute "obdiag rca run --scene=major_hold" to check it.')
                        self.stdio.warn("Found {0} major compactions running over 36 hours".format(hold_count))
            except Exception as e:
                self.report.add_fail("Failed to check major compaction holds: {0}".format(e))
                self.stdio.error("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "major",
            "info": "Check whether there is any suspended major compaction process.",
        }


major = MajorTask()
