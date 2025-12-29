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
@time: 2025/07/18
@file: network_write_cond_wakeup.py
@desc: Check for network write condition wakeup issues in observer logs
"""

from src.handler.check.check_task import TaskBase


class NetworkWriteCondWakeupTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(ssh_client.get_name()))
                    return

                # Get home_path from context
                home_path = node.get('home_path')
                if not home_path:
                    self.report.add_fail("node: {0} home_path is not configured".format(ssh_client.get_name()))
                    return

                # Check observer.log for "write cond wakeup" occurrences
                log_file_path = "{0}/log/observer.log".format(home_path)
                check_cmd = "grep -c 'write cond wakeup' {0} 2>/dev/null".format(log_file_path)

                result = ssh_client.exec_cmd(check_cmd).strip()
                self.stdio.verbose("Found {0} 'write cond wakeup' occurrences in {1} on {2}".format(result, log_file_path, ssh_client.get_name()))

                try:
                    wakeup_count = int(result)
                    if wakeup_count > 3:
                        self.report.add_critical(
                            "Found {0} 'write cond wakeup' occurrences in observer.log on {1}. "
                            "This indicates potential network issues between client and OBServer. "
                            "Please check network connectivity and performance.".format(wakeup_count, ssh_client.get_name())
                        )
                        self.stdio.warn("Network write condition wakeup count exceeds threshold on {0}: {1}".format(ssh_client.get_name(), wakeup_count))
                    else:
                        self.stdio.verbose("Network write condition wakeup count is normal on {0}: {1}".format(ssh_client.get_name(), wakeup_count))
                except ValueError:
                    self.report.add_fail("Failed to parse wakeup count on {0}: {1}".format(ssh_client.get_name(), result))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "network_write_cond_wakeup",
            "info": "Check for network write condition wakeup issues in observer logs",
        }


network_write_cond_wakeup = NetworkWriteCondWakeupTask()
