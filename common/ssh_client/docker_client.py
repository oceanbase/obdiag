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
@file: docker_client.py
@desc:
"""

from common.obdiag_exception import OBDIAGShellCmdException
from common.ssh_client.base import SsherClient


class DockerClient(SsherClient):
    def __init__(self, context=None, node=None):
        super().__init__(context, node)
        self.container_name = self.node.get("container_name")

    def exec_cmd(self, cmd):
        try:
            self.stdio.verbose("ssh_exec_cmd docker {0} cmd: {1}".format(self.container_name, cmd))
            client_result = self.client.containers.get(self.container_name)
            result = client_result.exec_run(
                cmd=["bash", "-c", cmd],
                detach=False,
                stdout=True,
                stderr=True,
            )
            if result.exit_code != 0:
                raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.container_name, cmd, result.output.decode('utf-8')))
        except Exception as e:
            self.stdio.error("sshHelper ssh_exec_cmd docker Exception: {0}".format(e))
            raise Exception("sshHelper ssh_exec_cmd docker Exception: {0}".format(e))
        return result.output.decode('utf-8')

    def download(self, remote_path, local_path):
        try:
            self.stdio.verbose("remote_path: {0}:{1} to local_path:{2}".format(self.node["container_name"], remote_path, local_path))
            client_result = self.client.containers.get(self.node["container_name"])
            data, stat = client_result.get_archive(remote_path)
            with open(local_path, "wb") as f:
                for chunk in data:
                    f.write(chunk)

        except Exception as e:
            self.stdio.error("sshHelper download docker Exception: {0}".format(e))
            raise Exception("sshHelper download docker Exception: {0}".format(e))

    def upload(self, remote_path, local_path):
        try:
            self.stdio.verbose(" local_path:{0} to remote_path:{1}:{2}".format(local_path, self.node["container_name"], remote_path))
            self.client.containers.get(self.node["container_name"]).put_archive(remote_path, local_path)
            return
        except Exception as e:
            self.stdio.error("sshHelper upload docker Exception: {0}".format(e))
            raise Exception("sshHelper upload docker Exception: {0}".format(e))

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        try:
            exec_id = self.client.exec_create(container=self.node["container_name"], command=['su', '- ' + new_user])
            response = self.client.exec_start(exec_id)
            return response
        except Exception as e:
            self.stdio.error("sshHelper ssh_invoke_shell_switch_user docker Exception: {0}".format(e))
            raise Exception("sshHelper ssh_invoke_shell_switch_user docker Exception: {0}".format(e))

    def get_name(self):
        return "docker: {0}".format(self.container_name)
