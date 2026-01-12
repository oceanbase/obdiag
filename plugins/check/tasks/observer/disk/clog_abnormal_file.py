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
@file: clog_abnormal_file.py
@desc: Check if there are files in the clog folder that do not belong to the observer
"""

from src.handler.check.check_task import TaskBase


class ClogAbnormalFileTask(TaskBase):
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
                redo_dir = node.get("redo_dir", "")

                if not redo_dir:
                    self.stdio.verbose("No redo_dir configured for node {0}".format(node_name))
                    continue

                try:
                    # Find clog directory
                    log_dir_path = ssh_client.exec_cmd('find {0}/ -name "clog"'.format(redo_dir)).strip()
                    if not log_dir_path:
                        self.report.add_critical("On {0}: log_dir_path is null. Please check your nodes.redo_dir need absolute Path".format(node_name))
                        continue

                    # Find abnormal files
                    abnormal_files = ssh_client.exec_cmd("find {0}/ -type f -name '*[^0-9]*' ! -name '*.tmp' ! -name '*.flashback' ! -name 'meta'".format(log_dir_path)).strip()
                    if abnormal_files:
                        self.report.add_critical("On {0}: Users are not allowed to modify or create in the clog folder, It will be causing observer startup failure. Files need be checked: {1}".format(node_name, abnormal_files))

                except Exception as e:
                    self.stdio.error("Failed to check clog files on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "clog_abnormal_file", "info": "Check if there are files in the clog folder that do not belong to the observer."}


clog_abnormal_file = ClogAbnormalFileTask()
