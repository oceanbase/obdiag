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
@file: getenforce.py
@desc: Check SELinux status by getenforce
"""

from src.handler.check.check_task import TaskBase


class GetenforceTask(TaskBase):
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
                    # Check if getenforce exists
                    result = ssh_client.exec_cmd('if command -v getenforce &>/dev/null; then echo "exist"; fi').strip()
                    if result != "exist":
                        self.stdio.verbose("getenforce command not found on {0}".format(node_name))
                        continue

                    # Check SELinux status
                    selinux_status = ssh_client.exec_cmd("getenforce").strip()
                    if selinux_status != "Disabled":
                        self.report.add_warning("On {0}: SELinux need Disabled. Now, it is {1}.".format(node_name, selinux_status))
                except Exception as e:
                    self.stdio.error("Failed to check getenforce on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "getenforce", "info": "Check SELinux status by getenforce."}


getenforce = GetenforceTask()
