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
@time: 2026/04/22
@file: server_load_error.py
@desc: Check observer.log for server load balancing error records ("fail to get loads by server")
"""

from src.handler.check.check_task import TaskBase

# Scan only the last N lines to avoid slow grep on large files and filter out stale history
TAIL_LINES = 5000
CRITICAL_THRESHOLD = 50


class ServerLoadErrorTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                home_path = node.get("home_path", "")
                if not home_path:
                    self.report.add_fail("node: {0} home_path is not configured".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()
                log_file_path = "{0}/log/observer.log".format(home_path)
                check_cmd = "tail -n {0} {1} 2>/dev/null | grep -c 'fail to get loads by server' || true".format(TAIL_LINES, log_file_path)

                result = ssh_client.exec_cmd(check_cmd).strip()
                self.stdio.verbose("node {0}: 'fail to get loads by server' count (last {1} lines) = {2}".format(node_name, TAIL_LINES, result))

                try:
                    hit_count = int(result) if result else 0
                    if hit_count >= CRITICAL_THRESHOLD:
                        self.report.add_critical(
                            "Found {0} 'fail to get loads by server' record(s) in the last {1} lines of observer.log on {2}. "
                            "Persistent load balancing errors may block server deletion or unit migration. "
                            "Run 'obdiag rca run --scene=delete_server_error' for detailed root cause analysis.".format(hit_count, TAIL_LINES, node_name)
                        )
                    elif hit_count > 0:
                        self.report.add_warning(
                            "Found {0} 'fail to get loads by server' record(s) in the last {1} lines of observer.log on {2}. "
                            "This indicates load balancing errors that may block server deletion or unit migration. "
                            "Run 'obdiag rca run --scene=delete_server_error' for detailed root cause analysis.".format(hit_count, TAIL_LINES, node_name)
                        )
                    else:
                        self.stdio.verbose("No server load errors found in observer.log on {0}".format(node_name))
                except ValueError:
                    self.report.add_fail("Failed to parse grep result on {0}: {1}".format(node_name, result))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "server_load_error",
            "info": "Check observer.log for server load balancing error records ('fail to get loads by server')",
        }


server_load_error = ServerLoadErrorTask()
