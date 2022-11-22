#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/22
@file: shell_utils.py
@desc:
"""
import paramiko

from paramiko import AuthenticationException
from paramiko import SSHException

from common.odg_exception import ODGSSHConnException
from common.odg_exception import ODGShellCmdException


class SshHelper(object):
    def __init__(self, host_ip, username, password, ssh_port, key_file):
        self.host_ip = host_ip
        self.username = username
        self.ssh_port = ssh_port
        self.need_password = True
        self.password = password
        self._ssh_fd = None
        self._sftp_client = None
        if len(key_file) > 0:
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
                raise ODGSSHConnException("ssh {0}@{1}: failed, exception:{2}".format(username, host_ip, e))
        else:
            self._ssh_fd = paramiko.SSHClient()
            self._ssh_fd.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self._ssh_fd.load_system_host_keys()
            self.need_password = True
            self._ssh_fd.connect(hostname=host_ip, username=username, password=password, port=ssh_port)

    def ssh_exec_cmd(self, cmd):
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            err_text = stderr.read()
            if len(err_text):
                raise ODGShellCmdException("Execute Shell command on server {0} failed, "
                                           "command=[{1}], exception:{2}".format(self.host_ip, cmd, err_text))
        except SSHException as e:
            raise ODGShellCmdException("Execute Shell command on server {0} failed, "
                                       "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))
        return stdout.read().decode('utf-8')

    def download(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self._sftp_client.get(remote_path, local_path)
        self._sftp_client.close()

    def ssh_close(self):
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None

    def __del__(self):
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None
