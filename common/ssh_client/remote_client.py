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
@file: remote_client.py
@desc:
"""


import os
import sys
import time
import paramiko
from paramiko.ssh_exception import SSHException, AuthenticationException
from common.obdiag_exception import OBDIAGShellCmdException, OBDIAGSSHConnException
from common.ssh_client.base import SsherClient

ENV_DISABLE_RSA_ALGORITHMS = 0


def dis_rsa_algorithms(state=0):
    """
    Disable RSA algorithms in OpenSSH server.
    """
    global ENV_DISABLE_RSA_ALGORITHMS
    ENV_DISABLE_RSA_ALGORITHMS = state


class RemoteClient(SsherClient):
    def __init__(self, context, node):
        super().__init__(context, node)
        self._sftp_client = None
        self._disabled_rsa_algorithms = None
        self.host_ip = self.node.get("ip")
        self.username = self.node.get("ssh_username")
        self.ssh_port = self.node.get("ssh_port")
        self.need_password = True
        self.password = self.node.get("ssh_password")
        self.key_file = self.node.get("ssh_key_file")
        self.key_file = os.path.expanduser(self.key_file)
        self._ssh_fd = None
        self._sftp_client = None
        # remote_client_sudo
        self.remote_client_sudo = bool(self.context.inner_config.get("obdiag").get("ssh_client").get("remote_client_sudo"))
        # remote_client_disable_rsa_algorithms
        DISABLED_ALGORITHMS = dict(pubkeys=["rsa-sha2-512", "rsa-sha2-256"])
        remote_client_disable_rsa_algorithms = bool(self.context.inner_config.get("obdiag").get("basic").get("dis_rsa_algorithms"))
        if remote_client_disable_rsa_algorithms:
            self._disabled_rsa_algorithms = DISABLED_ALGORITHMS
        self.ssh_type = "remote"
        if len(self.key_file) > 0:
            try:
                self._ssh_fd = paramiko.SSHClient()
                self._ssh_fd.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
                self._ssh_fd.load_system_host_keys()
                self._ssh_fd.connect(hostname=self.host_ip, username=self.username, key_filename=self.key_file, port=self.ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)
            except AuthenticationException:
                self.password = input("Authentication failed, Input {0}@{1} password:\n".format(self.username, self.host_ip))
                self.need_password = True
                self._ssh_fd.connect(hostname=self.host_ip, username=self.username, password=self.password, port=self.ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)
            except Exception as e:
                raise OBDIAGSSHConnException("ssh {0}@{1}: failed, exception:{2}".format(self.host_ip, self.ssh_port, e))
        else:
            self._ssh_fd = paramiko.SSHClient()
            self._ssh_fd.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
            self._ssh_fd.load_system_host_keys()
            self.need_password = True
            self._ssh_fd.connect(hostname=self.host_ip, username=self.username, password=self.password, port=self.ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)

    def exec_cmd(self, cmd):
        stdin, stdout, stderr = None, None, None
        try:
            if self.remote_client_sudo:
                # check sudo without password
                self.stdio.verbose("use remote_client_sudo")
                stdin, stdout, stderr = self._ssh_fd.exec_command("sudo -n true")
                if stderr:
                    if len(stderr.read().decode('utf-8').strip()) > 0:
                        raise Exception(stderr.read().decode('utf-8'))
                cmd = "sudo {0}".format(cmd)
                cmd = cmd.replace("&&", "&& sudo ")
            self.stdio.verbose('Execute Shell command on server {0}:{1}'.format(self.host_ip, cmd))
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            err_text = stderr.read()
            if len(err_text):
                return err_text.decode('utf-8')
            return stdout.read().decode('utf-8')
        except UnicodeDecodeError as e:
            self.stdio.warn("[remote] Execute Shell command UnicodeDecodeError, command=[{0}]  Exception = [{1}]".format(cmd, e))
            if stderr:
                return str(stderr)
            if stdout:
                return str(stdout)
            return ""
        except SSHException as e:
            raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))

    def download(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self.stdio.verbose('Download {0}:{1}'.format(self.host_ip, remote_path))
        self._sftp_client.get(remote_path, local_path, callback=self.progress_bar)
        self._sftp_client.close()

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

    def upload(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self._sftp_client.put(local_path, remote_path)
        self._sftp_client.close()

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        try:
            ssh = self._ssh_fd.invoke_shell()
            ssh.send('su {0}\n'.format(new_user))
            ssh.send('{}\n'.format(cmd))
            time.sleep(time_out)
            self._ssh_fd.close()
            result = ssh.recv(65535)
        except SSHException as e:
            raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))
        return result.decode('utf-8')

    def get_name(self):
        return "remote_{0}".format(self.host_ip)
