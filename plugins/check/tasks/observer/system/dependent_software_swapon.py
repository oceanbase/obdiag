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
@file: dependent_software_swapon.py
@desc: Check swapon status
"""

from src.handler.check.check_task import TaskBase


class DependentSoftwareSwaponTask(TaskBase):
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
                    # Check if swapon exists
                    result = ssh_client.exec_cmd('if command -v swapon &>/dev/null; then echo "exist"; fi').strip()
                    if result != "exist":
                        self.stdio.verbose("swapon command not found on {0}".format(node_name))
                        continue

                    # Check if swap is used
                    swap_status = ssh_client.exec_cmd('swapon --summary | grep -q "^" && echo "used" || echo "not used"').strip()
                    if swap_status == "used":
                        self.report.add_warning("On {0}: swapon need be closed. Now, it is used.".format(node_name))
                except Exception as e:
                    self.stdio.error("Failed to check swapon on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "dependent_software_swapon", "info": "Check swapon status."}


dependent_software_swapon = DependentSoftwareSwaponTask()
