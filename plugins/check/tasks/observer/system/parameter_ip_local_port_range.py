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
@file: parameter_ip_local_port_range.py
@desc: Check net.ipv4.ip_local_port_range kernel parameter
"""

from src.handler.check.check_task import TaskBase


class ParameterIpLocalPortRangeTask(TaskBase):
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
                    port_range = super().get_system_parameter(ssh_client, "net.ipv4.ip_local_port_range")
                    if not port_range or port_range == "-1":
                        self.report.add_critical("On {0}: net.ipv4.ip_local_port_range is not set properly".format(node_name))
                        continue

                    parts = port_range.split()
                    if len(parts) >= 2:
                        port_min = int(parts[0])
                        port_max = int(parts[1])

                        if port_min != 3500:
                            self.report.add_warning("On {0}: ip_local_port_range_min: {1}. recommended: 3500".format(node_name, port_min))

                        if port_max != 65535:
                            self.report.add_warning("On {0}: ip_local_port_range_max: {1}. recommended: 65535".format(node_name, port_max))
                except Exception as e:
                    self.stdio.error("Failed to check ip_local_port_range on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "parameter_ip_local_port_range",
            "info": "Check net.ipv4.ip_local_port_range kernel parameter.",
            "supported_os": ["linux"],
        }


parameter_ip_local_port_range = ParameterIpLocalPortRangeTask()
