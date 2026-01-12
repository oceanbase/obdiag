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
@file: ulimit_parameter.py
@desc: Check ulimit parameters on observer nodes
       Reference: https://www.oceanbase.com/docs/enterprise-oceanbase-ocp-cn-1000000000125643
"""

from src.handler.check.check_task import TaskBase


class UlimitParameterTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                remote_ip = ssh_client.get_name()

                # Check ulimit -c (core file size)
                try:
                    result = ssh_client.exec_cmd("ulimit -c").strip()
                    if result != "unlimited":
                        self.report.add_warning('On ip: {0}, ulimit -c as "core file size" is {1}. recommended: unlimited.'.format(remote_ip, result))
                    self.stdio.verbose("node {0}: ulimit -c = {1}".format(remote_ip, result))
                except Exception as e:
                    self.stdio.error("Failed to check ulimit -c on {0}: {1}".format(remote_ip, e))

                # Check ulimit -u (max user processes)
                try:
                    result = ssh_client.exec_cmd("ulimit -u").strip()
                    if result != "655360":
                        self.report.add_warning('On ip: {0}, ulimit -u as "max user processes" is {1}. recommended: 655360.'.format(remote_ip, result))
                    self.stdio.verbose("node {0}: ulimit -u = {1}".format(remote_ip, result))
                except Exception as e:
                    self.stdio.error("Failed to check ulimit -u on {0}: {1}".format(remote_ip, e))

                # Check ulimit -s (stack size)
                try:
                    result = ssh_client.exec_cmd("ulimit -s").strip()
                    if result != "unlimited":
                        self.report.add_warning('On ip: {0}, ulimit -s as "stack size" is {1}. recommended: unlimited.'.format(remote_ip, result))
                    self.stdio.verbose("node {0}: ulimit -s = {1}".format(remote_ip, result))
                except Exception as e:
                    self.stdio.error("Failed to check ulimit -s on {0}: {1}".format(remote_ip, e))

                # Check ulimit -n (open files)
                try:
                    result = ssh_client.exec_cmd("ulimit -n").strip()
                    try:
                        if int(result) != 655350:
                            self.report.add_warning('On ip: {0}, ulimit -n as "open files" is {1}. recommended: 655350.'.format(remote_ip, result))
                    except ValueError:
                        if result != "unlimited":
                            self.report.add_warning('On ip: {0}, ulimit -n as "open files" is {1}. recommended: 655350.'.format(remote_ip, result))
                    self.stdio.verbose("node {0}: ulimit -n = {1}".format(remote_ip, result))
                except Exception as e:
                    self.stdio.error("Failed to check ulimit -n on {0}: {1}".format(remote_ip, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "ulimit_parameter",
            "info": "Check ulimit parameters. Reference: https://www.oceanbase.com/docs/enterprise-oceanbase-ocp-cn-1000000000125643",
        }


ulimit_parameter = UlimitParameterTask()
