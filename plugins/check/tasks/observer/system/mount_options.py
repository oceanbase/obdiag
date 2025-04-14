#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
@time: 2025/04/8
@file: mount_options.py
@desc:
"""

from src.handler.checker.check_task import TaskBase

need_check_options = ["nfsvers=4.1", "sync", "lookupcache=positive", "hard"]


class MountOptions(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                self._execute_node(node)
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _execute_node(self, node):
        try:
            ssh_client = node.get("ssher")
            if ssh_client is None:
                self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                return
            output = ssh_client.exec_cmd("mount -l")
            for line in output.splitlines():
                if ' type nfs' not in line:
                    continue  # 跳过非NFS挂载
                parts = line.split()
                if len(parts) < 5 or parts[1] != 'on' or parts[3] != 'type':
                    continue  # 格式不匹配
                # 提取关键字段
                source = parts[0]
                mount_point = parts[2].rstrip(' ')  # 挂载点路径
                options_str = parts[-1].strip('()')
                for opt in need_check_options:
                    if opt not in options_str:
                        self.report.add_critical(f"node: {ssh_client.get_name()} {mount_point} mount option {opt} is not exist")

        except Exception as e:
            self.stdio.error(f"Command execution error: {e}")
            self.report.add_fail(f"Command execution error: {e}")
            return None

    def get_task_info(self):
        return {"name": "mount_options", "info": "When mounting NFS, it is necessary to ensure that the parameters of the backup mounting environment include nfsvers=4.1, sync, lookupcache=positive, and hard. issue#611 "}


mount_options = MountOptions()
