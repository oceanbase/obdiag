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
@file: __init__.py
@desc:
"""
import socket
from common.ssh_client.docker_client import DockerClient
from common.ssh_client.kubernetes_client import KubernetesClient
from common.ssh_client.local_client import LocalClient
from common.ssh_client.remote_client import RemoteClient
from stdio import SafeStdio


class SshHelper(SafeStdio):
    def __init__(self, context=None, node=None):
        if node is None:
            raise Exception("SshHelper init error: node is None")
        self.node = node
        self.context = context
        self.ssh_type = node.get("ssh_type") or "remote"
        self.client = None
        self.init()

    def local_ip(self):
        local_ip_list = []
        hostname = socket.gethostname()
        addresses = socket.getaddrinfo(hostname, None)
        for address in addresses:
            local_ip_list.append(address[4][0])
        local_ip_list.append('127.0.0.1')
        return list(set(local_ip_list))

    def init(self):
        try:
            self.ssh_type = self.node.get("ssh_type") or "remote"
            # where ssh_type is remote, maybe use local client.
            if self.ssh_type == 'remote':
                node_ip = self.node.get("ip") or ""
                if node_ip == "":
                    raise Exception("the node ip is None")
                if node_ip in self.local_ip():
                    self.ssh_type = "local"
            if self.ssh_type == 'local':
                self.client = LocalClient(self.context, self.node)
            elif self.ssh_type == "remote":
                self.client = RemoteClient(self.context, self.node)
            elif self.ssh_type == 'docker':
                self.client = DockerClient(self.context, self.node)
            elif self.ssh_type == 'kubernetes':
                self.client = KubernetesClient(self.context, self.node)
            else:
                raise Exception("the ssh type is not support: {0}".format(self.ssh_type))
        except Exception as e:
            raise Exception("init ssh client error: {}".format(e))

    def exec_cmd(self, cmd):
        return self.client.exec_cmd(cmd)

    def download(self, remote_path, local_path):
        return self.client.download(remote_path, local_path)

    def upload(self, remote_path, local_path):
        return self.client.upload(remote_path, local_path)

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        return self.client.ssh_invoke_shell_switch_user(new_user, cmd, time_out)


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
