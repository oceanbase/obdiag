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
@file: table_history_too_many.py
@desc: Check for too many table histories which may cause schema refresh issues
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class TableHistoryTooManyTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: [4.1.0.1, 4.1.0.2]
            if not super().check_ob_version_min("4.1.0.1"):
                self.stdio.verbose("Version < 4.1.0.1, skip check")
                return

            if self.observer_version and StringUtils.compare_versions_greater(self.observer_version, "4.1.0.2"):
                self.stdio.verbose("Version > 4.1.0.2, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = 'select table_name from oceanbase.__all_virtual_table_history group by 1 having count(*) > 4000000'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    self.report.add_critical(
                        "There are too many table histories for a tenant in the cluster, and when the machine restarts, the schema refresh will continue to report -4013, resulting in the inability to refresh the corresponding tenant's schema for a particular machine."
                    )
            except Exception as e:
                self.report.add_fail("Failed to check table history: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "table_history_too_many", "info": "Check for too many table histories which may cause schema refresh issues"}


table_history_too_many = TableHistoryTooManyTask()
