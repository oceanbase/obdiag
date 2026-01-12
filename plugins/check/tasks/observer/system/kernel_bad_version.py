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
@time: 2025/04/30
@file: kernel_bad_version.py
@desc:
"""
import re
from src.handler.checker.check_task import TaskBase


class KernelBadVersion(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # get kernel version by ssher
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                kernel_version = ssh_client.exec_cmd("uname -r")
                if not kernel_version:
                    return self.report.add_fail("get kernel version error")
                self.stdio.verbose("node: {0} kernel version is {1}".format(ssh_client.get_name(), kernel_version))
                # check kernel version
                if re.match(r"3\.10\.\d+-\d+", kernel_version):
                    self.report.add_critical(
                        "node: {0} kernel version is {1}, There is a risk of system downtime when deploying OBServer using cgroup method on an operating system with kernel version 3.10 issue #910".format(ssh_client.get_name(), kernel_version)
                    )
                    continue
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "kernel_bad_version",
            "info": "There is a risk of system downtime when deploying OBServer using cgroup method on an operating system with kernel version 3.10. issue #910",
        }


kernel_bad_version = KernelBadVersion()
