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
@file: mod_too_large.py
@desc: Check whether any module is using more than 10GB of memory
"""

from src.handler.check.check_task import TaskBase


class ModTooLargeTask(TaskBase):
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
                SELECT hold/1024/1024/1024 AS hold_g, used/1024/1024/1024 AS used_g
                FROM oceanbase.__all_virtual_memory_info
                order by hold desc limit 1
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    hold_g = float(result[0].get('hold_g', 0))
                    if hold_g > 10:
                        self.report.add_warning("mod max memory over 10G ({0:.2f}G), Please check on oceanbase.__all_virtual_memory_info to find some large mod".format(hold_g))
            except Exception as e:
                self.report.add_fail("Failed to check mod memory: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "mod_too_large", "info": "Check whether any module is using more than 10GB of memory."}


mod_too_large = ModTooLargeTask()
