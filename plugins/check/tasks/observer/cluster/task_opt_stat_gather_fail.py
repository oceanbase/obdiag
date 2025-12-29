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
@file: task_opt_stat_gather_fail.py
@desc: Check whether there are any failed execution results for history collection tasks
"""

from src.handler.check.check_task import TaskBase


class TaskOptStatGatherFailTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.2.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = 'SELECT count(0) as fail_count FROM oceanbase.DBA_OB_TASK_OPT_STAT_GATHER_HISTORY where STATUS<>"SUCCESS"'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    fail_count = result[0].get('fail_count', 0)
                    if fail_count > 0:
                        self.report.add_critical("task_opt_stat_gather_fail: {0} failed tasks found".format(fail_count))
            except Exception as e:
                self.report.add_fail("Failed to check task_opt_stat_gather_fail: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "task_opt_stat_gather_fail", "info": "Check whether there are any failed execution results for history collection tasks."}


task_opt_stat_gather_fail = TaskOptStatGatherFailTask()
