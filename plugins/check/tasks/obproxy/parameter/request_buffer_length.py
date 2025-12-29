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
@file: request_buffer_length.py
@desc: Check obproxy request_buffer_length parameter
"""

from src.handler.check.check_task import TaskBase


class RequestBufferLengthTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            results = super().get_obproxy_parameter("request_buffer_length")
            if not results:
                self.stdio.verbose("Cannot get obproxy parameter request_buffer_length")
                return

            for param in results:
                value = param.get('value', '')
                if value != "4KB":
                    self.report.add_critical("obproxy's parameter request_buffer_length is {0}, not 4KB".format(value))
                else:
                    self.stdio.verbose("request_buffer_length check passed with value 4KB")

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "request_buffer_length", "info": "Check obproxy request_buffer_length parameter."}


request_buffer_length = RequestBufferLengthTask()
