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
@file: parameter_tcp_wmem.py
@desc: Check net.ipv4.tcp_wmem kernel parameter
"""

from src.handler.check.check_task import TaskBase


class ParameterTcpWmemTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()

                try:
                    tcp_wmem = super().get_system_parameter(ssh_client, "net.ipv4.tcp_wmem")
                    if not tcp_wmem:
                        continue

                    parts = tcp_wmem.split()
                    if len(parts) >= 3:
                        tcp_wmem_min = int(parts[0])
                        tcp_wmem_default = int(parts[1])
                        tcp_wmem_max = int(parts[2])

                        if tcp_wmem_min < 4096 or tcp_wmem_min > 8192:
                            self.report.add_warning("On {0}: net.ipv4.tcp_wmem_min: {1}. recommended: 4096 ≤ min ≤ 8192".format(node_name, tcp_wmem_min))

                        if tcp_wmem_default < 65536 or tcp_wmem_default > 131072:
                            self.report.add_warning("On {0}: net.ipv4.tcp_wmem_default: {1}. recommended: 65536 ≤ default ≤ 131072".format(node_name, tcp_wmem_default))

                        if tcp_wmem_max < 8388608 or tcp_wmem_max > 16777216:
                            self.report.add_warning("On {0}: net.ipv4.tcp_wmem_max: {1}. recommended: 8388608 ≤ max ≤ 16777216".format(node_name, tcp_wmem_max))
                except Exception as e:
                    self.stdio.error("Failed to check tcp_wmem on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "parameter_tcp_wmem", "info": "Check net.ipv4.tcp_wmem kernel parameter."}


parameter_tcp_wmem = ParameterTcpWmemTask()
