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
@time: 2024/6/24
@file: base.py
@desc:
"""
from stdio import SafeStdio


class SsherClient(SafeStdio):
    def __init__(self, context, node):
        super().__init__()
        self.context = context
        if context is not None:
            self.stdio = self.context.stdio
        else:
            self.stdio = None
        self.node = node
        self.ssh_type = node.get("ssh_type") or "remote"
        self.client = None

    def exec_cmd(self, cmd):
        raise Exception("the client type is not support exec_cmd")

    def download(self, remote_path, local_path):
        raise Exception("the client type is not support download")

    def upload(self, remote_path, local_path):
        raise Exception("the client type is not support upload")

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        raise Exception("the client type is not support ssh invoke shell switch user")

    def ssh_close(self):
        return

    def get_name(self):
        return "not defined"

    def get_ip(self):
        return self.client.get_ip()
