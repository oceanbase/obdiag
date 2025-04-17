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
@file: tcp_tw_reuse.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class TcpTwReuse(TaskBase):

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
            parameter_value = super().get_system_parameter(ssh_client, "net.ipv4.tcp_tw_reuse")
            if parameter_value is None:
                self.report.add_critical("node: {0} net.ipv4.tcp_tw_reuse is not exist".format(ssh_client.get_name()))
                return
            if parameter_value != "1":
                self.report.add_warning("node: {0} net.ipv4.tcp_tw_reuse is {1}. It is recommended to set it to 1".format(ssh_client.get_name(), parameter_value))
                return

        except Exception as e:
            self.stdio.error(f"Command execution error: {e}")
            self.report.add_fail(f"Command execution error: {e}")
            return None

    def get_task_info(self):
        return {"name": "tcp_tw_reuse", "info": "Check if sockets in TIME-WAIT state (TIME-WAIT ports) are allowed to be used for new TCP connections. Need to be set to 1 to ensure system performance. issue#737"}


tcp_tw_reuse = TcpTwReuse()
