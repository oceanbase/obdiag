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
import paramiko
import time

from paramiko import AuthenticationException
from paramiko import SSHException

from common.obdiag_exception import OBDIAGSSHConnException
from common.obdiag_exception import OBDIAGShellCmdException


class SshHelper(object):
    def __init__(self, host_ip, username, password, ssh_port, key_file):
        self.host_ip = host_ip
        self.username = username
        self.ssh_port = ssh_port
        self.need_password = True
        self.password = password
        self.key_file = key_file
        self._ssh_fd = None
        self._sftp_client = None
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
            self._ssh_fd.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
            err_text = stderr.read()
            if len(err_text):
                print("Execute Shell command on server {0} failed, "
                      "command=[{1}], exception:{2}".format(self.host_ip, cmd, err_text))
        except SSHException as e:
            print("Execute Shell command on server {0} failed, "
                  "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))
        return stdout.read().decode('utf-8')

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

    def download(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self._sftp_client.get(remote_path, local_path)
        self._sftp_client.close()

    def upload(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self._sftp_client.put(local_path, remote_path)
        self._sftp_client.close()

    def ssh_close(self):
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None

    def delete_file_force(self, file_name):
        rm_cmd = "rm -rf {0}".format(file_name)
        self.ssh_exec_cmd(rm_cmd)

    def delete_empty_file(self, file_path):
        rm_cmd = "find  {file_path} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(file_path=file_path)
        self.ssh_exec_cmd(rm_cmd)

    def get_file_size(self, file_path):
        get_file_size_cmd = "ls -nl %s | awk '{print $5}'" % file_path
        file_size = self.ssh_exec_cmd(get_file_size_cmd)
        return file_size

    def is_empty_dir(self, dir_path):
        cmd = "ls -A {dir_path}|wc -w".format(dir_path=dir_path)
        file_num = self.ssh_exec_cmd(cmd)
        if int(file_num) == 0:
            return True
        else:
            return False

    def is_empty_file(self, file_path):
        file_size = self.get_file_size(file_path)
        if int(file_size) == 0:
            return True
        else:
            return False

    def ssh_mkdir_if_not_exist(self, dir_path):
        mkdir_cmd = "mkdir -p {0}".format(dir_path)
        self.ssh_exec_cmd(mkdir_cmd)

    def zip_rm_dir(self, upper_dir, zip_dir):
        zip_cmd = "cd {upper_dir} && zip {zip_dir}.zip -rm {zip_dir}".format(
            upper_dir=upper_dir,
            zip_dir=zip_dir)
        self.ssh_exec_cmd(zip_cmd)

    def zip_encrypt_rm_dir(self, upper_dir, zip_dir, password):
        zip_cmd = "cd {upper_dir} && zip --password {password} {zip_dir}.zip -rm {zip_dir}".format(
            upper_dir=upper_dir,
            password=password,
            zip_dir=zip_dir)
        self.ssh_exec_cmd(zip_cmd)

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
