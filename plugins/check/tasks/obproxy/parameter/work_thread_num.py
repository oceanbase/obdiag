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
@time: 2025/07/15
@file: work_thread_num.py
@desc: Check obproxy work_thread_num parameter value
"""

from src.handler.checker.check_task import TaskBase


class WorkThreadNumTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Get obproxy work_thread_num parameter
            results = super().get_obproxy_parameter("work_thread_num")

            if not results:
                self.report.add_fail("Failed to get obproxy work_thread_num parameter")
                return

            for param in results:
                value = param.get('value', '')
                self.stdio.verbose("obproxy work_thread_num parameter value: {0}".format(value))

                # Check if the value is not the default value (128)
                if int(value) != 128:
                    self.report.add_warning(
                        "obproxy work_thread_num parameter is {0}, not the default value 128. "
                        "This may cause thread exhaustion issues under high load conditions. "
                        "Consider setting it back to 128 unless you have specific performance requirements.".format(value)
                    )
                    self.stdio.warn("work_thread_num parameter mismatch: expected 128, got {0}".format(value))
                else:
                    self.stdio.verbose("work_thread_num parameter check passed with value 128")

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "work_thread_num",
            "info": "Check obproxy work_thread_num parameter value for potential thread exhaustion issues. issue #1019",
        }


work_thread_num = WorkThreadNumTask()
