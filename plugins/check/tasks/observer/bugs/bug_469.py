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
@file: bug_469.py
@desc: Check glibc version - must be less than 2.34
       GitHub issue: https://github.com/oceanbase/obdiag/issues/469
"""

from src.handler.check.check_task import TaskBase


class Bug469Task(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: >= 3.0.0
            if not super().check_ob_version_min("3.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()

                try:
                    # Get glibc major version
                    cmd_major = "ldd --version | head -n 1 | awk '{print $NF}' | cut -d. -f1"
                    major_version = ssh_client.exec_cmd(cmd_major).strip()

                    # Get glibc minor version
                    cmd_minor = "ldd --version | head -n 1 | awk '{print $NF}' | cut -d. -f2"
                    minor_version = ssh_client.exec_cmd(cmd_minor).strip()

                    self.stdio.verbose("node {0}: glibc version = {1}.{2}".format(node_name, major_version, minor_version))

                    try:
                        major = int(major_version)
                        minor = int(minor_version)

                        # Check if version >= 2.34
                        if major > 2 or (major == 2 and minor >= 34):
                            self.report.add_critical("On {0}: glibc version {1}.{2} >= 2.34. This may cause observer crash. More information: https://github.com/oceanbase/obdiag/issues/469".format(node_name, major, minor))
                            self.stdio.warn("glibc version {0}.{1} on {2} may cause issues".format(major, minor, node_name))
                    except ValueError:
                        self.stdio.error("Failed to parse glibc version on {0}: {1}.{2}".format(node_name, major_version, minor_version))

                except Exception as e:
                    self.stdio.error("Failed to check glibc version on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "bug_469",
            "info": "Check glibc version - must be less than 2.34",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/469",
        }


bug_469 = Bug469Task()
