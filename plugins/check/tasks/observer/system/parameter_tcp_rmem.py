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
@file: parameter_tcp_rmem.py
@desc: Check net.ipv4.tcp_rmem kernel parameter
"""

from src.handler.check.check_task import TaskBase


class ParameterTcpRmemTask(TaskBase):
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
                    tcp_rmem = super().get_system_parameter(ssh_client, "net.ipv4.tcp_rmem")
                    if not tcp_rmem or tcp_rmem == "-1":
                        self.report.add_critical("On {0}: net.ipv4.tcp_rmem is not set properly".format(node_name))
                        continue

                    parts = tcp_rmem.split()
                    if len(parts) >= 3:
                        tcp_rmem_min = int(parts[0])
                        tcp_rmem_default = int(parts[1])
                        tcp_rmem_max = int(parts[2])

                        if tcp_rmem_min < 4096 or tcp_rmem_min > 8192:
                            self.report.add_warning("On {0}: net.ipv4.tcp_rmem_min: {1}. recommended: 4096 ≤ min ≤ 8192".format(node_name, tcp_rmem_min))

                        if tcp_rmem_default < 65536 or tcp_rmem_default > 131072:
                            self.report.add_warning("On {0}: net.ipv4.tcp_rmem_default: {1}. recommended: 65536 ≤ default ≤ 131072".format(node_name, tcp_rmem_default))

                        if tcp_rmem_max < 8388608 or tcp_rmem_max > 16777216:
                            self.report.add_warning("On {0}: net.ipv4.tcp_rmem_max: {1}. recommended: 8388608 ≤ max ≤ 16777216".format(node_name, tcp_rmem_max))
                except Exception as e:
                    self.stdio.error("Failed to check tcp_rmem on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "parameter_tcp_rmem", "info": "Check net.ipv4.tcp_rmem kernel parameter."}


parameter_tcp_rmem = ParameterTcpRmemTask()
