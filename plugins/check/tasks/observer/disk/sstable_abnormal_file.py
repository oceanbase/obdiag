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
@file: sstable_abnormal_file.py
@desc: Check if there are files in the sstable folder that do not belong to the observer
"""

from src.handler.check.check_task import TaskBase


class SstableAbnormalFileTask(TaskBase):
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
                data_dir = node.get("data_dir", "")

                if not data_dir:
                    self.stdio.verbose("No data_dir configured for node {0}".format(node_name))
                    continue

                try:
                    # Find sstable directory
                    sstable_path = ssh_client.exec_cmd('find {0}/ -name "sstable"'.format(data_dir)).strip()
                    if not sstable_path:
                        self.report.add_critical("On {0}: sstable_dir_path is null. Please check your nodes.data_dir need absolute Path".format(node_name))
                        continue

                    # Find abnormal files (not block_file)
                    abnormal_files = ssh_client.exec_cmd('find {0}/ -type f ! -name block_file'.format(sstable_path)).strip()
                    if abnormal_files:
                        self.report.add_critical("On {0}: Users are not allowed to modify or create in the dir_path folder, It will be causing observer startup failure. Files need be checked: {1}".format(node_name, abnormal_files))

                except Exception as e:
                    self.stdio.error("Failed to check sstable files on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sstable_abnormal_file", "info": "Check if there are files in the sstable folder that do not belong to the observer."}


sstable_abnormal_file = SstableAbnormalFileTask()
