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
@file: diagnosis_connection_error.py
@desc: Check obproxy_diagnosis.log for abnormal connection error records.
       CLIENT_VC_TRACE entries are excluded as they represent normal client-side
       connection closes (EOS) and would generate excessive noise.
"""

from src.handler.check.check_task import TaskBase

# Scan only the last N lines to avoid slow grep on large files and filter out stale history
TAIL_LINES = 5000
CRITICAL_THRESHOLD = 100


class DiagnosisConnectionErrorTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.obproxy_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                home_path = node.get("home_path", "")
                if not home_path:
                    self.report.add_fail("node: {0} home_path is not configured".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()
                log_file_path = "{0}/log/obproxy_diagnosis.log".format(home_path)

                # Exclude CLIENT_VC_TRACE: these are normal client-side connection closes (EOS)
                # and would generate excessive noise in busy environments.
                # Focus on LOGIN_TRACE, SERVER_VC_TRACE, TIMEOUT_TRACE, PROXY_INTERNAL_TRACE.
                check_cmd = ("tail -n {0} {1} 2>/dev/null" " | grep 'CONNECTION](trace_type'" " | grep -cv 'CLIENT_VC_TRACE'" " || true").format(TAIL_LINES, log_file_path)

                result = ssh_client.exec_cmd(check_cmd).strip()
                self.stdio.verbose("node {0}: abnormal connection error count (last {1} lines, excluding CLIENT_VC_TRACE) = {2}".format(node_name, TAIL_LINES, result))

                try:
                    hit_count = int(result) if result else 0
                    if hit_count >= CRITICAL_THRESHOLD:
                        self.report.add_critical(
                            "Found {0} abnormal connection error record(s) (excluding CLIENT_VC_TRACE) "
                            "in the last {1} lines of obproxy_diagnosis.log on {2}. "
                            "High-frequency connection errors detected (LOGIN/SERVER/TIMEOUT/PROXY_INTERNAL). "
                            "Run 'obdiag rca run --scene=disconnection' for detailed root cause analysis.".format(hit_count, TAIL_LINES, node_name)
                        )
                    elif hit_count > 0:
                        self.report.add_warning(
                            "Found {0} abnormal connection error record(s) (excluding CLIENT_VC_TRACE) "
                            "in the last {1} lines of obproxy_diagnosis.log on {2}. "
                            "Run 'obdiag rca run --scene=disconnection' for detailed root cause analysis.".format(hit_count, TAIL_LINES, node_name)
                        )
                    else:
                        self.stdio.verbose("No abnormal connection errors found in obproxy_diagnosis.log on {0}".format(node_name))
                except ValueError:
                    self.report.add_fail("Failed to parse obproxy_diagnosis.log grep result on {0}: {1}".format(node_name, result))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "diagnosis_connection_error",
            "info": "Check obproxy_diagnosis.log for abnormal connection errors (LOGIN/SERVER/TIMEOUT/PROXY_INTERNAL trace types, excluding normal CLIENT_VC_TRACE closes)",
        }


diagnosis_connection_error = DiagnosisConnectionErrorTask()
