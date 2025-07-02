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
@file: ssh.py
@desc:
"""
import os
import re
import socket
import signal
import threading
import time
from src.common.ssh_client.docker_client import DockerClient
from src.common.ssh_client.kubernetes_client import KubernetesClient
from src.common.ssh_client.local_client import LocalClient
from src.common.ssh_client.remote_client import RemoteClient
from src.common.stdio import SafeStdio


class TimeoutException(Exception):
    """Timeout exception"""

    pass


class SshClient(SafeStdio):
    # some not safe command will be filter
    filter_cmd_list = ["rm -rf /", ":(){:|:&};:", "reboot", "shutdown"]
    filter_cmd_re_list = [r'kill -9 (\d+)']

    def __init__(self, context=None, node=None):
        if node is None:
            raise Exception("SshHelper init error: node is None")
        self.node = node
        self.context = context
        self.stdio = None
        if self.context is not None:
            self.stdio = self.context.stdio
        self.ssh_type = node.get("ssh_type") or "remote"
        self.client = None
        self.init()
        if self.context is not None:
            self.cmd_exec_timeout = self.context.inner_config.get("obdiag", {}).get("ssh_client", {}).get("cmd_exec_timeout", 180)
        else:
            self.cmd_exec_timeout = 180

    def local_ip(self):
        local_ip_list = []
        try:
            hostname = socket.gethostname()
            addresses = socket.getaddrinfo(hostname, None)
            for address in addresses:
                local_ip_list.append(address[4][0])
        except Exception as e:
            if self.stdio is not None:
                self.stdio.warn("get local ip warn: {} . Set local_ip Is 127.0.0.1".format(e))
        local_ip_list.append('127.0.0.1')
        return list(set(local_ip_list))

    def init(self):
        try:
            self.ssh_type = self.node.get("ssh_type") or "remote"
            # where ssh_type is remote, maybe use local client.
            if self.ssh_type == 'remote' or self.ssh_type == 'ssh':
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
            if self.stdio is not None:
                self.stdio.error("init ssh client error: {}".format(e))
            raise Exception("init ssh client error: {}".format(e))

    def _exec_cmd_with_timeout(self, cmd, timeout=None):
        """Execute command with timeout detection"""
        if timeout is None:
            timeout = self.cmd_exec_timeout

        result = [None]
        exception = [None]

        def target():
            try:
                result[0] = self.client.exec_cmd(cmd)
            except Exception as e:
                exception[0] = e

        thread = threading.Thread(target=target)
        thread.daemon = True
        thread.start()
        thread.join(timeout)

        if thread.is_alive():
            # Thread is still running, indicating timeout
            if self.stdio is not None:
                self.stdio.error("Command execution timeout after {} seconds: {}".format(timeout, cmd))
            raise TimeoutException("Command execution timeout after {} seconds: {}".format(timeout, cmd))

        if exception[0] is not None:
            raise exception[0]

        return result[0]

    def exec_cmd(self, cmd, timeout=None):
        """
        Execute command with timeout detection

        Args:
            cmd: Command to execute
            timeout: Timeout in seconds, default 3 minutes

        Returns:
            Command execution result

        Raises:
            TimeoutException: Raised when command execution times out
        """
        self.__cmd_filter(cmd)
        return self._exec_cmd_with_timeout(cmd, timeout)

    def download(self, remote_path, local_path):
        self.stdio.verbose("download file: {} to {}".format(remote_path, local_path))
        try:
            self.stdio.verbose("download file mkdir local dir {0}".format(os.path.dirname(local_path)))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
        except Exception as e:
            self.stdio.warn("mkdir local dir {0} error: {1}".format(os.path.dirname(local_path), e))
            pass
        return self.client.download(remote_path, local_path)

    def upload(self, remote_path, local_path):
        return self.client.upload(remote_path, local_path)

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        return self.client.ssh_invoke_shell_switch_user(new_user, cmd, time_out)

    def ssh_close(self):
        return self.client.ssh_close()

    def ssh_reconnect(self):
        self.client = None
        self.init()
        return

    def run(self, cmd):
        return self.exec_cmd(cmd)

    def get_name(self):
        return self.client.get_name().replace(":", "_").replace(".", "_")

    def get_ip(self):
        return self.client.get_ip()

    def __cmd_filter(self, cmd):
        cmd = cmd.strip()
        if cmd in self.filter_cmd_list:
            self.stdio.error("cmd is not safe: {}".format(cmd))
            raise Exception("cmd is not safe: {}".format(cmd))
        # support regular expression
        for filter_cmd in self.filter_cmd_re_list:
            if re.match(filter_cmd, cmd):
                self.stdio.error("cmd is not safe: {}".format(cmd))
                raise Exception("cmd is not safe: {}".format(cmd))
