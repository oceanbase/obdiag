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
@file: network_offset.py
@desc: Check cluster info about network clockdiff offset
"""

from src.handler.check.check_task import TaskBase
import subprocess


class NetworkOffsetTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            # Check if clockdiff exists locally
            try:
                result = subprocess.run(['which', 'clockdiff'], capture_output=True, text=True)
                if result.returncode != 0:
                    self.stdio.verbose("clockdiff is not installed locally")
                    return
            except Exception:
                self.stdio.verbose("Cannot check clockdiff availability")
                return

            for node in self.observer_nodes:
                remote_ip = node.get("ip")
                node_name = node.get("ssher").get_name() if node.get("ssher") else remote_ip

                try:
                    # Run clockdiff
                    result = subprocess.run(['clockdiff', '-o', remote_ip], capture_output=True, text=True, timeout=30)
                    output = result.stdout.strip()

                    if "is down" in output:
                        self.report.add_critical("node: {0} can not get clock offset by 'clockdiff -o {0}', doc: https://www.oceanbase.com/knowledge-base/ocp-ee-1000000000346970?back=kb".format(remote_ip))
                        continue

                    # Parse offset from output
                    parts = output.split()
                    if len(parts) >= 2:
                        try:
                            offset = int(parts[1])
                            if offset > 50:
                                self.report.add_critical("node: {0} clock offset is {1}ms, it is over 50ms, issue: https://github.com/oceanbase/obdiag/issues/701".format(remote_ip, offset))
                        except ValueError:
                            self.stdio.error("Cannot parse offset for {0}: {1}".format(remote_ip, output))

                except subprocess.TimeoutExpired:
                    self.stdio.error("clockdiff timeout for {0}".format(remote_ip))
                except Exception as e:
                    self.stdio.error("Failed to check clock offset for {0}: {1}".format(remote_ip, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "network_offset", "info": "Check cluster info about network clockdiff offset."}


network_offset = NetworkOffsetTask()
