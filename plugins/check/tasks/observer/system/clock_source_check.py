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
@file: clock_source_check.py
@desc:
"""
import re
from src.handler.checker.check_task import TaskBase


class ClockSourceCheck(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        self.clock_sources = {}

    def execute(self):
        try:
            cmd = "cat /etc/chrony.conf | grep -v '^#' | grep iburst"

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                node_ip = node["ip"]
                if not self.check_command_exist(ssh_client, "chronyc"):
                    self.report.add_warning("node:{0}. chronyc command does not exist.".format(ssh_client.get_name()))
                    continue
                output = ssh_client.exec_cmd(cmd)

                sources = []
                for line in output.splitlines():
                    match = re.search(r'server\s+(\S+)\s+iburst', line.strip())
                    if match:
                        sources.append(match.group(1))
                # check chronyc running
                chronyc_status = ssh_client.exec_cmd("systemctl status chronyd | grep running")
                if not chronyc_status:
                    self.report.add_warning("node:{0}. chronyc is not running.".format(ssh_client.get_name()))
                    continue

                chronyc_data = ssh_client.exec_cmd("chronyc sources -v | grep ms | awk '{print $NF}'")
                if not chronyc_data:
                    self.report.add_warning("node:{0}.Clock source is abnormal. No delay value found. Please check the clock source status.".format(ssh_client.get_name()))
                else:
                    for line in chronyc_data.splitlines():
                        # clock_delays: #{clock_delays}. Some clock delays are greater than 100ms. Please check the clock synchronization status.
                        match = re.findall(r'(\d+)ms', line.strip())
                        if match:
                            if int(match[0]) > 100:
                                self.report.add_warning("node:{0}. Some clock delays are greater than 100ms. Please check the clock synchronization status.".format(ssh_client.get_name()))
                                break

                sources_sorted = tuple(sorted(sources))
                if sources_sorted not in self.clock_sources:
                    self.clock_sources[sources_sorted] = []
                self.clock_sources[sources_sorted].append(node_ip)
            if len(self.clock_sources) == 0:
                return self.report.add_warning("No clock source configuration found.")
            most_common_config = max(self.clock_sources.items(), key=lambda x: len(x[1]))[0]
            non_compliant_nodes = []

            for config, ips in self.clock_sources.items():
                if config != most_common_config:
                    non_compliant_nodes.extend(ips)

            if non_compliant_nodes:
                nodes_str = ", ".join(non_compliant_nodes)
                return self.report.add_warning(f"Found nodes with inconsistent clock sources (expected: {list(most_common_config)}): {nodes_str}")
        except Exception as e:
            return self.report.add_fail(f"Execute error: {e}")

    def get_task_info(self):
        return {"name": "clock_source_check", "info": "It is recommended to add inspection items to check whether the configuration file server IP of the ob node clock source is consistent.issue #781 #873"}

    def get_scene_info(self):
        pass


clock_source_check = ClockSourceCheck()
