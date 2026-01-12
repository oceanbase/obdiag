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
@file: bad_version.py
@desc: Check observer version - Some versions are not recommended
"""

from src.handler.check.check_task import TaskBase


class BadVersionTask(TaskBase):
    # Known bad revision numbers
    BAD_REVISIONS = ["100000192023032010", "103000072023081111", "104000032023092120"]

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
                home_path = node.get("home_path", "")

                if not home_path:
                    self.stdio.verbose("No home_path configured for node {0}".format(node_name))
                    continue

                try:
                    cmd = "export LD_LIBRARY_PATH={0}/lib && {0}/bin/observer --version 2>&1 | grep -oP 'REVISION: \\K\\d+'".format(home_path)
                    revision = ssh_client.exec_cmd(cmd).strip()

                    self.stdio.verbose("node {0}: observer revision = {1}".format(node_name, revision))

                    if revision in self.BAD_REVISIONS:
                        self.report.add_critical("On node {0}: the version revision is {1}, the observer is not recommended".format(node_name, revision))
                except Exception as e:
                    self.stdio.error("Failed to check observer version on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "bad_version", "info": "Check observer version - Some versions are not recommended."}


bad_version = BadVersionTask()
