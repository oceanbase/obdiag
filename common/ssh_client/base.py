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
import sys

from stdio import SafeStdio


class SsherClient(SafeStdio):
    def __init__(self, context, node):
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

    def progress_bar(self, transferred, to_be_transferred, suffix=''):
        bar_len = 20
        filled_len = int(round(bar_len * transferred / float(to_be_transferred)))
        percents = round(20.0 * transferred / float(to_be_transferred), 1)
        bar = '\033[32;1m%s\033[0m' % '=' * filled_len + '-' * (bar_len - filled_len)
        print_percents = round((percents * 5), 1)
        sys.stdout.flush()
        sys.stdout.write('Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m%s\033[0m' % print_percents, '% [', self.translate_byte(transferred), ']', suffix))
        if transferred == to_be_transferred:
            sys.stdout.write('Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m%s\033[0m' % print_percents, '% [', self.translate_byte(transferred), ']', suffix))
            print()

    def translate_byte(self, B):
        if B < 0:
            B = -B
            return '-' + self.translate_byte(B)
        if B == 0:
            return '0B'
        units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
        k = 1024
        i = 0
        while B >= k and i < len(units) - 1:
            B /= k
            i += 1
        return f"{B:.2f} {units[i]}"
