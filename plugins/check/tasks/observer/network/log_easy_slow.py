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
@time: 2025/01/08
@file: log_easy_slow.py
@desc: Check for network latency issues by searching "EASY SLOW" in observer logs
"""

from src.handler.checker.check_task import TaskBase

class LogEasySlowTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(ssh_client.get_name()))
                    return

                # Get home_path from node configuration
                home_path = node.get('home_path')
                if not home_path:
                    self.report.add_fail("node: {0} home_path is not configured".format(ssh_client.get_name()))
                    return

                # Check observer.log for "EASY SLOW" occurrences
                log_file_path = "{0}/log/observer.log".format(home_path)
                check_cmd = "grep -c 'EASY SLOW' {0} 2>/dev/null || echo '0'".format(log_file_path)
                
                result = ssh_client.exec_cmd(check_cmd).strip()
                self.stdio.verbose("Found {0} 'EASY SLOW' occurrences in {1} on {2}".format(
                    result, log_file_path, ssh_client.get_name()
                ))

                try:
                    easy_slow_count = int(result)
                    if easy_slow_count > 0:
                        self.report.add_critical(
                            "Found {0} 'EASY SLOW' occurrences in observer.log on {1}. "
                            "This indicates potential network latency issues. "
                            "Please check network connectivity and performance between nodes.".format(
                                easy_slow_count, ssh_client.get_name()
                            )
                        )
                        self.stdio.warn("Network latency issue detected on {0}: {1} EASY SLOW occurrences".format(
                            ssh_client.get_name(), easy_slow_count
                        ))
                    else:
                        self.stdio.verbose("No network latency issues detected on {0}".format(ssh_client.get_name()))
                except ValueError:
                    self.report.add_fail("Failed to parse EASY SLOW count on {0}: {1}".format(
                        ssh_client.get_name(), result
                    ))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "log_easy_slow",
            "info": "Check for network latency issues by searching 'EASY SLOW' in observer logs",
        }

log_easy_slow = LogEasySlowTask()
