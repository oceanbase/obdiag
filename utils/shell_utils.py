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
@time: 2022/6/22
@file: shell_utils.py
@desc:
"""
import sys

import paramiko
import time

from paramiko import AuthenticationException
from paramiko import SSHException

from common.obdiag_exception import OBDIAGSSHConnException
from common.obdiag_exception import OBDIAGShellCmdException


class SshHelper(object):
    def __init__(self, is_ssh, host_ip, username, password, ssh_port, key_file):
        self.is_ssh = is_ssh
        self.host_ip = host_ip
        self.username = username
        self.ssh_port = ssh_port
        self.need_password = True
        self.password = password
        self.key_file = key_file
        self._ssh_fd = None
        self._sftp_client = None
        if self.is_ssh:
            if len(self.key_file) > 0:
                try:
                    self._ssh_fd = paramiko.SSHClient()
                    self._ssh_fd.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    self._ssh_fd.load_system_host_keys()
                    key = paramiko.RSAKey.from_private_key_file(key_file)
                    self._ssh_fd.connect(hostname=host_ip, username=username, pkey=key, port=ssh_port)
                except AuthenticationException:
                    self.password = input("Authentication failed, Input {0}@{1} password:\n".format(username, host_ip))
                    self.need_password = True
                    self._ssh_fd.connect(hostname=host_ip, username=username, password=password, port=ssh_port)
                except Exception as e:
                    raise OBDIAGSSHConnException("ssh {0}@{1}: failed, exception:{2}".format(username, host_ip, e))
            else:
                self._ssh_fd = paramiko.SSHClient()
                self._ssh_fd.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
                self._ssh_fd.load_system_host_keys()
                self.need_password = True
                self._ssh_fd.connect(hostname=host_ip, username=username, password=password, port=ssh_port)

    def ssh_exec_cmd(self, cmd):
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            err_text = stderr.read()
            if len(err_text):
                raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, "
                                           "command=[{1}], exception:{2}".format(self.host_ip, cmd, err_text))
        except SSHException as e:
            raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, "
                                       "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))
        return stdout.read().decode('utf-8')

    def ssh_exec_cmd_ignore_err(self, cmd):
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            return stdout.read().decode('utf-8')
        except SSHException as e:
            print("Execute Shell command on server {0} failed,command=[{1}], exception:{2}".format(self.host_ip, cmd, e))

    def ssh_exec_cmd_ignore_exception(self, cmd):
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            return stderr.read().decode('utf-8')
        except SSHException as e:
            pass

    def ssh_exec_cmd_get_stderr(self, cmd):
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            return stderr.read().decode('utf-8')
        except SSHException as e:
            pass

    def progress_bar(self, transferred, to_be_transferred, suffix=''):
        bar_len = 20
        filled_len = int(round(bar_len * transferred / float(to_be_transferred)))
        percents = round(20.0 * transferred / float(to_be_transferred), 1)
        bar = '\033[32;1m%s\033[0m' % '=' * filled_len + '-' * (bar_len - filled_len)
        print_percents = round((percents * 5), 1)
        sys.stdout.write('Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m%s\033[0m' % print_percents, '% [', self.translate_byte(transferred), ']',  suffix))
        sys.stdout.flush()

    def download(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        print('Download {0}:{1}'.format(self.host_ip,remote_path))
        self._sftp_client.get(remote_path, local_path, callback=self.progress_bar)
        self._sftp_client.close()

    def translate_byte(self, B):
        B = float(B)
        KB = float(1024)
        MB = float(KB ** 2)
        GB = float(MB ** 2)
        TB = float(GB ** 2)
        if B < KB:
            return '{} {}'.format(B, 'bytes' if B > 1 else "byte")
        elif KB < B < MB:
            return '{:.2f} KB'.format(B / KB)
        elif MB < B < GB:
            return '{:.2f} MB'.format(B / MB)
        elif GB < B < TB:
            return '{:.2f} GB'.format(B / GB)
        else:
            return '{:.2f} TB'.format(B / TB)

    def upload(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self._sftp_client.put(remote_path, local_path)
        self._sftp_client.close()

    def ssh_close(self):
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None

    def __del__(self):
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        try:
            ssh = self._ssh_fd.invoke_shell()
            ssh.send('su {0}\n'.format(new_user))
            ssh.send('{}\n'.format(cmd))
            time.sleep(time_out)
            self._ssh_fd.close()
            result = ssh.recv(65535)
        except SSHException as e:
            raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, "
                                       "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))
        return result