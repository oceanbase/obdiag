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
@file: core_file_find.py
@desc: Check whether the core file exists
"""

from src.handler.check.check_task import TaskBase


class CoreFileFindTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()
                home_path = node.get("home_path", "")

                if not home_path:
                    self.stdio.verbose("No home_path configured for node {0}".format(node_name))
                    continue

                try:
                    cmd = 'ls {0} | grep "^core" | wc -l'.format(home_path)
                    result = ssh_client.exec_cmd(cmd).strip()
                    core_count = int(result) if result.isdigit() else 0

                    if core_count > 0:
                        self.report.add_critical("The core file exists on node {0}. Found {1} core file(s).".format(node_name, core_count))
                    else:
                        self.stdio.verbose("No core files found on node {0}".format(node_name))
                except Exception as e:
                    self.stdio.error("Failed to check core files on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "core_file_find", "info": "Check whether the core file exists."}


core_file_find = CoreFileFindTask()
