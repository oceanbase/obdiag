#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
@time: 2025/04/8
@file: python_version.py
@desc:
"""
import re

from src.handler.checker.check_task import TaskBase


class PythonVersion(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                self._execute_node(node)
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _execute_node(self, node):
        try:
            ssh_client = node.get("ssher")
            if ssh_client is None:
                self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                return
            # check python is existed
            if super().check_command_exist(ssh_client, "python"):
                cmd = "python --version"
                result = ssh_client.exec_cmd(cmd)
                if result:
                    lines = result.split("\n")
                    for line in lines:
                        if not line.strip():
                            continue
                        parts = re.split(r'\s+', line.strip())
                        if len(parts) < 2:
                            continue
                        version = parts[1]
                        if not version.startswith("2.7"):
                            self.report.add_warning("node: {0} python version: {1}. OceanBase related scripts depend on Python 2.7. x".format(ssh_client.get_name(), version))
            else:
                self.report.add_critical("node: {0} python is not existed".format(ssh_client.get_name()))
                return
        except Exception as e:
            self.stdio.error(f"Command execution error: {e}")
            self.report.add_fail(f"Command execution error: {e}")
            return None

    def get_task_info(self):
        return {"name": "python_version", "info": "Check if the Python version installed on the host is 2.7. x, ensuring that the relevant OceanBase scripts can run properly"}


python_version = PythonVersion()
