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
@file: dependent_software.py
@desc: Check dependent software settings
"""

from src.handler.check.check_task import TaskBase


class DependentSoftwareTask(TaskBase):
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

                # Check firewalld
                try:
                    result = ssh_client.exec_cmd("ps aux | grep firewalld | grep -v grep").strip()
                    if result:
                        self.report.add_warning("On {0}: firewalld is running. It is recommended to disable firewalld or add allow rules for each service in the OceanBase cluster to avoid firewall interception.".format(node_name))
                except Exception as e:
                    self.stdio.error("Failed to check firewalld on {0}: {1}".format(node_name, e))

                # Check crond
                try:
                    result = ssh_client.exec_cmd("ps aux | grep crond | grep -v grep").strip()
                    if not result:
                        self.report.add_warning("On {0}: crond is not running. It is recommended to enable it, mainly for setting up scheduled tasks and providing related operation and maintenance capabilities.".format(node_name))
                except Exception as e:
                    self.stdio.error("Failed to check crond on {0}: {1}".format(node_name, e))

                # Check transparent_hugepage
                try:
                    result = ssh_client.exec_cmd('cat /sys/kernel/mm/transparent_hugepage/enabled | grep "\\[never\\]"').strip()
                    if not result:
                        self.report.add_warning("On {0}: transparent_hugepage should be set to [never].".format(node_name))
                except Exception as e:
                    self.stdio.error("Failed to check transparent_hugepage on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "dependent_software", "info": "Check dependent software settings."}


dependent_software = DependentSoftwareTask()
