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
@file: xfs_repair.py
@desc: Check dmesg for xfs_repair log. Issue #451
"""

from src.handler.check.check_task import TaskBase


class XfsRepairTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.2.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()

                try:
                    result = ssh_client.exec_cmd('dmesg -T | grep -m 1 "xfs_repair"').strip()
                    if result:
                        self.report.add_critical("On {0}: xfs need repair. Please check disk. xfs_repair_log: {1}".format(node_name, result))
                except Exception as e:
                    self.stdio.error("Failed to check xfs_repair on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "xfs_repair",
            "info": "Check dmesg for xfs_repair log",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/451",
        }


xfs_repair = XfsRepairTask()
