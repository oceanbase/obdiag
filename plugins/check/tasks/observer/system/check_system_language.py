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
@time: 2025/08/18
@file: check_system_language.py
@desc:
"""

from src.handler.checker.check_task import TaskBase
from src.handler.checker.check_exception import StepResultFailException


class CheckSystemLanguage(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        for node in self.observer_nodes:
            ip = node.get("ip")
            ssher = node.get("ssher")
            if not ssher:
                self.stdio.warn(f"Node {ip} has no ssher, skip check.")
                continue
            try:
                # check $LANG is en_US.UTF-8
                # cmd = f"echo $LANG | grep 'en_US.UTF-8'"
                cmd = f"""bash -l -c "echo \$LANG" | grep -iP 'en_US.utf(-|)8'"""
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                system_language_check = ssher.exec_cmd(cmd).strip()
                if not system_language_check:
                    self.report.add_critical(f"ip:{ip}, the os language($LANG) setting is not en-US.UTF-8, please manually intervene and check.")

            except Exception as e:
                self.stdio.error(f"Check system language failed on {ip}: {str(e)}")
                self.report.add_fail(f"ip:{ip}, check_system_language, Exception: {str(e)}")

    def get_task_info(self):
        return {
            "name": "check_system_language",
            "info": "Check whether $LANG is en_US.UTF-8",
        }


check_system_language = CheckSystemLanguage()
