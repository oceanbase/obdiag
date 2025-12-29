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
@file: core_pattern.py
@desc: Check kernel.core_pattern
"""

from src.handler.check.check_task import TaskBase
import os


class CorePatternTask(TaskBase):
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
                    core_pattern = super().get_system_parameter(ssh_client, "kernel.core_pattern")
                    if not core_pattern:
                        continue

                    # Check if core_pattern contains spaces (which may indicate pipes or functions)
                    if " " in core_pattern:
                        self.report.add_critical("On {0}: kernel.core_pattern: {1}, is not recommended for configuring functions other than the specified core path".format(node_name, core_pattern))
                        continue

                    # Get core path directory
                    core_path = ssh_client.exec_cmd('dirname "{0}"'.format(core_pattern)).strip()

                    # Check if core path exists
                    path_exists = ssh_client.exec_cmd('[ -d "{0}" ] && echo "yes" || echo "no"'.format(core_path)).strip()
                    if path_exists != "yes":
                        self.report.add_critical("On {0}: core_path: {1} is not exist. Please create it.".format(node_name, core_path))
                        continue

                    # Check for existing core files
                    core_count = ssh_client.exec_cmd('ls {0} | grep "^core" | wc -l'.format(core_path)).strip()
                    if core_count.isdigit() and int(core_count) > 0:
                        self.report.add_critical("On {0}: The core file exists in {1}.".format(node_name, core_path))

                    # Check free space (need > 10GB = 10485760KB)
                    free_space = ssh_client.exec_cmd("df \"{0}\" | awk 'NR==2 {{print $4}}'".format(core_path)).strip()
                    if free_space.isdigit() and int(free_space) < 10485760:
                        self.report.add_critical("On {0}: core_path: {1} free_space: {2}KB need > 10485760KB (10GB)".format(node_name, core_path, free_space))

                except Exception as e:
                    self.stdio.error("Failed to check core_pattern on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "core_pattern", "info": "Check kernel.core_pattern."}


core_pattern = CorePatternTask()
