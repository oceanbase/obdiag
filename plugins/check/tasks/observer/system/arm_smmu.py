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
@file: arm_smmu.py
@desc: Check SMMU on ARM architecture. Issue #784
"""

from src.handler.check.check_task import TaskBase


class ArmSmmuTask(TaskBase):
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
                    # Check architecture
                    arch = ssh_client.exec_cmd("arch").strip()
                    if arch == "x86_64":
                        self.stdio.verbose("Node {0} is x86_64 architecture, skip smmu check".format(node_name))
                        continue

                    # Check for smmu stuck messages
                    smmu_stuck_count = ssh_client.exec_cmd('dmesg -T | grep "stuck for" | wc -l').strip()
                    if smmu_stuck_count.isdigit() and int(smmu_stuck_count) > 0:
                        self.report.add_critical("On {0}: found dmesg stuck for smmu, if the arch of the node is arm, Please close the smmu on bios".format(node_name))
                except Exception as e:
                    self.stdio.error("Failed to check arm_smmu on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "arm_smmu",
            "info": "Check SMMU on ARM architecture",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/784",
            "supported_os": ["linux"],
        }


arm_smmu = ArmSmmuTask()
