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
@file: local_client.py
@desc:
"""

from common.ssh_client.base import SsherClient
import subprocess32 as subprocess
import shutil


class LocalClient(SsherClient):
    def __init__(self, context=None, node=None):
        super().__init__(context, node)

    def exec_cmd(self, cmd):
        try:
            self.stdio.verbose("[local host] run cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, executable='/bin/bash')
            stdout, stderr = out.communicate()
            if stderr:
                return stderr.decode('utf-8')
            return stdout.decode('utf-8')
        except Exception as e:
            self.stdio.error("run cmd = [{0}] on localhost, Exception = [{1}]".format(cmd, e))
            raise Exception("[localhost] Execute Shell command failed, command=[{0}]  Exception = [{1}]".format(cmd, e))

    def download(self, remote_path, local_path):
        try:
            shutil.copy(remote_path, local_path)
        except Exception as e:
            self.stdio.error("download file from localhost, remote_path=[{0}], local_path=[{1}], error=[{2}]".format(remote_path, local_path, str(e)))
            raise Exception("download file from localhost, remote_path=[{0}], local_path=[{1}], error=[{2}]".format(remote_path, local_path, str(e)))

    def upload(self, remote_path, local_path):
        try:
            shutil.copy(local_path, remote_path)
        except Exception as e:
            self.stdio.error("upload file to localhost, remote_path =[{0}], local_path=[{1}], error=[{2}]".format(remote_path, local_path, str(e)))
            raise Exception("[local] upload file to localhost, remote _path =[{0}], local _path=[{1}], error=[{2}]".format(remote_path, local_path, str(e)))

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        try:
            cmd = "su - {0} -c '{1}'".format(new_user, cmd)
            self.stdio.verbose("[local host] ssh_invoke_shell_switch_user cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
            stdout, stderr = out.communicate()
            if stderr:
                return stderr.decode('utf-8')
            return stdout.decode('utf-8')
        except:
            self.stdio.error("run cmd = [{0}] on localhost".format(cmd))
        raise Exception("the client type is not support ssh invoke shell switch user")

    def get_name(self):
        return "local"

    def get_ip(self):
        return self.client.get_ip()
