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
@time: 2025/01/23
@file: cgroup_version.py
@desc: Check cgroup version. OceanBase currently uses cgroup v1. If the customer's operating system is cgroup v2, resource isolation will not take effect.
"""

from src.handler.check.check_task import TaskBase


class CgroupVersionTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    node_ip = node.get("ip", "unknown")
                    self.report.add_fail("node: {0} ssh client is None".format(node_ip))
                    continue

                # Check cgroup version
                cgroup_version = self._check_cgroup_version(ssh_client)

                if cgroup_version is None:
                    self.stdio.warn("node: {0} failed to determine cgroup version".format(ssh_client.get_name()))
                    continue

                self.stdio.verbose("cgroup version on {0}: {1}".format(ssh_client.get_name(), cgroup_version))

                # OceanBase currently uses cgroup v1, if system uses cgroup v2, resource isolation will not take effect
                if cgroup_version == "v2":
                    self.report.add_critical(
                        "node: {0} is using cgroup v2, but OceanBase currently uses cgroup v1. "
                        "Resource isolation will not take effect. Please consider using cgroup v1 or check if OceanBase supports cgroup v2. "
                        "Reference: https://ask.oceanbase.com/t/topic/35632756/4".format(ssh_client.get_name())
                    )
                elif cgroup_version == "v1":
                    self.stdio.verbose("node: {0} is using cgroup v1, which is compatible with OceanBase".format(ssh_client.get_name()))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _check_cgroup_version(self, ssh_client):
        """Check cgroup version on the system"""
        try:
            # Method 1: Check if /sys/fs/cgroup/cgroup.controllers exists (v2 indicator)
            # This is the most reliable indicator for cgroup v2
            cmd1 = '[ -f /sys/fs/cgroup/cgroup.controllers ] && echo "v2" || echo "not_v2"'
            result1 = ssh_client.exec_cmd(cmd1).strip()
            self.stdio.verbose("check /sys/fs/cgroup/cgroup.controllers: {0}".format(result1))

            if result1 == "v2":
                return "v2"

            # Method 2: Check if /sys/fs/cgroup/unified exists (v2 indicator)
            cmd2 = '[ -d /sys/fs/cgroup/unified ] && echo "v2" || echo "not_v2"'
            result2 = ssh_client.exec_cmd(cmd2).strip()
            self.stdio.verbose("check /sys/fs/cgroup/unified: {0}".format(result2))

            if result2 == "v2":
                return "v2"

            # Method 3: Check if systemd uses cgroup v2
            # systemd uses cgroup v2 if /sys/fs/cgroup/systemd/cgroup.controllers exists
            cmd3 = '[ -f /sys/fs/cgroup/systemd/cgroup.controllers ] && echo "v2" || echo "not_v2"'
            result3 = ssh_client.exec_cmd(cmd3).strip()
            self.stdio.verbose("check /sys/fs/cgroup/systemd/cgroup.controllers: {0}".format(result3))

            if result3 == "v2":
                return "v2"

            # Method 4: Check /proc/cgroups (v1 has multiple controllers listed)
            # In v2, this file might be empty or have minimal content
            cmd4 = 'cat /proc/cgroups 2>/dev/null | wc -l'
            result4 = ssh_client.exec_cmd(cmd4).strip()
            self.stdio.verbose("check /proc/cgroups line count: {0}".format(result4))

            if result4 and result4.isdigit():
                line_count = int(result4)
                # v1 typically has multiple controllers (more than 1 line including header)
                # v2 might have 0 or 1 line
                if line_count > 1:
                    return "v1"

            # Method 5: Check for typical v1 structure (controller directories like cpu, memory)
            # In cgroup v1, there are multiple controller directories
            cmd5 = '[ -d /sys/fs/cgroup/cpu ] && [ -d /sys/fs/cgroup/memory ] && echo "v1" || echo "not_v1"'
            result5 = ssh_client.exec_cmd(cmd5).strip()
            self.stdio.verbose("check /sys/fs/cgroup/cpu and memory directories: {0}".format(result5))

            if result5 == "v1":
                return "v1"

            # If we can't determine, return None
            self.stdio.warn("Unable to determine cgroup version using standard methods")
            return None

        except Exception as e:
            self.stdio.warn("Error checking cgroup version: {0}".format(e))
            return None

    def get_task_info(self):
        return {
            "name": "cgroup_version",
            "info": "Check cgroup version. OceanBase currently uses cgroup v1. If the customer's operating system is cgroup v2, resource isolation will not take effect",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1101",
        }


cgroup_version = CgroupVersionTask()
