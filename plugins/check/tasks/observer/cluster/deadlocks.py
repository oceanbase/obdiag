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
@file: deadlocks.py
@desc: Check whether there is a deadlock
"""

from src.handler.check.check_task import TaskBase


class DeadlocksTask(TaskBase):
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

            sql = 'select count(0) as deadlock_count from oceanbase.DBA_OB_DEADLOCK_EVENT_HISTORY'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    deadlock_count = result[0].get('deadlock_count', 0)
                    if deadlock_count > 0:
                        self.report.add_warning("There is a deadlock. Please check on the oceanbase.DBA_OB_DEADLOCK_EVENT_HISTORY")
                        self.stdio.warn("Found {0} deadlock events".format(deadlock_count))
                    else:
                        self.stdio.verbose("No deadlock events found")
            except Exception as e:
                self.report.add_fail("Failed to check deadlocks: {0}".format(e))
                self.stdio.error("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "deadlocks",
            "info": "Check whether there is a deadlock.",
        }


deadlocks = DeadlocksTask()
