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
@time: 2025/04/8
@file: check_command.py
@desc:
"""

from src.handler.checker.check_task import TaskBase

need_check_command = [
    {"name": "mtr", "info": "Network testing tools. Suggested installation"},
    {"name": "tar", "info": "Compressed file format. Suggested installation"},
    {"name": "curl", "info": "Data transfer tool. Suggested installation"},
    {"name": "nc", "info": "Network connection tool. Suggested installation"},
    # objdump
    {"name": "objdump", "info": "binutls binary toolset. Suggested installation"},
    # nslookup
    {"name": "nslookup", "info": "bind-utls DNS toolset. Suggested installation"},
    # tc
    {"name": "tc", "info": "iproute Network Management Toolkit. Suggested installation"},
]


class CheckCommand(TaskBase):

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
            for command in need_check_command:
                if not super().check_command_exist(ssh_client, command["name"]):
                    self.report.add_warning("node: {0} {1} is not existed. {2}".format(ssh_client.get_name(), command["name"], command["info"]))

        except Exception as e:
            self.stdio.error(f"Command execution error: {e}")
            self.report.add_fail(f"Command execution error: {e}")
            return None

    def get_task_info(self):
        return {"name": "check_command", "info": "Confirm if the dependent components exist"}


check_command = CheckCommand()
