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
@time: 2024/07/25
@file: test_remote_client.py
@desc:
"""

from io import StringIO
import paramiko
import unittest
from unittest.mock import patch, MagicMock
from common.ssh_client.remote_client import RemoteClient
from context import HandlerContext
from common.obdiag_exception import OBDIAGShellCmdException, OBDIAGSSHConnException


class TestRemoteClient(unittest.TestCase):
    def setUp(self):
        self.context = HandlerContext()
        self.node = {
            "ip": "127.0.0.1",
            "ssh_username": "root",
            "ssh_port": 22,
            "ssh_password": "K8S0",
            "ssh_key_file": "",
        }
        self.remote_client = RemoteClient(context=self.context, node=self.node)
        self.remote_client._ssh_fd = MagicMock()
        self.remote_client._sftp_client = MagicMock()
        self.remote_client.stdio = MagicMock()

    @patch('common.ssh_client.remote_client.paramiko.SSHClient')
    def test_init_with_password(self, mock_ssh_client):
        """Tests SSH connection using password"""
        self._disabled_rsa_algorithms = None
        remote_client = RemoteClient(self.context, self.node)
        mock_ssh_client.assert_called_once()
        mock_ssh_client().connect.assert_called_once_with(hostname=self.node['ip'], username=self.node['ssh_username'], password=self.node['ssh_password'], port=self.node['ssh_port'], disabled_algorithms=self._disabled_rsa_algorithms)

    @patch('common.ssh_client.remote_client.paramiko.SSHClient')
    def test_init_with_key_file(self, mock_ssh_client):
        """Tests SSH connections using key files"""
        self._disabled_rsa_algorithms = None
        self.node['ssh_key_file'] = '/path/to/keyfile'
        remote_client = RemoteClient(self.context, self.node)
        mock_ssh_client.assert_called_once()
        mock_ssh_client().connect.assert_called_once_with(hostname=self.node['ip'], username=self.node['ssh_username'], key_filename=self.node['ssh_key_file'], port=self.node['ssh_port'], disabled_algorithms=self._disabled_rsa_algorithms)

    @patch('common.ssh_client.remote_client.paramiko.SSHClient')
    def test_init_with_authentication_exception(self, mock_ssh_client):
        """Test when authentication fails"""
        mock_ssh_client.return_value.connect.side_effect = paramiko.AuthenticationException
        with self.assertRaises(paramiko.AuthenticationException):
            RemoteClient(self.context, self.node)

    @patch('common.ssh_client.remote_client.paramiko.SSHClient')
    def test_init_with_connection_exception(self, mock_ssh_client):
        """Tests whether an exception is thrown when the connection fails"""
        mock_ssh_client().connect.side_effect = Exception("Connection failed")
        with self.assertRaises(Exception) as context:
            RemoteClient(self.context, self.node)
        self.assertIn("Connection failed", str(context.exception))

    def test_exec_cmd_success(self):
        """Tests successfully execution of the command"""
        self.remote_client._ssh_fd.exec_command.return_value = (MagicMock(), MagicMock(read=MagicMock(return_value=b'success')), MagicMock(read=MagicMock(return_value=b'')))
        result = self.remote_client.exec_cmd('ls')
        self.assertEqual(result, 'success')

    def test_exec_cmd_failure(self):
        """Tests unsuccessfully execution of the command"""
        self.remote_client._ssh_fd.exec_command.return_value = (MagicMock(), MagicMock(read=MagicMock(return_value=b'')), MagicMock(read=MagicMock(return_value=b'error')))
        result = self.remote_client.exec_cmd('invalid_command')
        self.assertEqual(result, 'error')

    def test_exec_cmd_ssh_exception(self):
        """Tests SSH exceptions"""
        self.remote_client._ssh_fd.exec_command.side_effect = paramiko.SSHException('SSH error')
        with self.assertRaises(OBDIAGShellCmdException) as context:
            self.remote_client.exec_cmd('ls')
        self.assertIn('Execute Shell command on server 127.0.0.1 failed', str(context.exception))

    @patch('paramiko.SFTPClient.from_transport')
    def test_download(self, mock_sftp_client):
        """Tests download"""
        # Sets the return value of the mock object
        mock_transport = MagicMock()
        self.remote_client._ssh_fd.get_transport.return_value = mock_transport
        mock_sftp_client.return_value = self.remote_client._sftp_client

        # Call the function under test
        remote_path = '/remote/file.txt'
        local_path = '/local/file.txt'
        self.remote_client.download(remote_path, local_path)

        # Verify that the method is called correctly
        self.remote_client._ssh_fd.get_transport.assert_called_once()
        mock_sftp_client.assert_called_once_with(mock_transport)
        self.remote_client.stdio.verbose.assert_called_once_with('Download 127.0.0.1:/remote/file.txt')
        self.remote_client._sftp_client.get.assert_called_once_with(remote_path, local_path, callback=self.remote_client.progress_bar)
        self.remote_client._sftp_client.close.assert_called_once()

    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_bar(self, mock_stdout):
        """Tests progress bar"""
        transferred = 1024  # 1KB
        to_be_transferred = 1048576  # 1MB
        suffix = 'test_suffix'
        bar_len = 20
        filled_len = int(round(bar_len * transferred / float(to_be_transferred)))
        bar = '\033[32;1m%s\033[0m' % '=' * filled_len + '-' * (bar_len - filled_len)

        # Call the function under test
        self.remote_client.progress_bar(transferred, to_be_transferred, suffix)
        mock_stdout.flush()

        # Verify that the method is called correctly
        expected_output = 'Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m0.0\033[0m', '% [', self.remote_client.translate_byte(transferred), ']', suffix)
        self.assertIn(expected_output, mock_stdout.getvalue())

    @patch('sys.stdout', new_callable=StringIO)
    def test_progress_bar_complete(self, mock_stdout):
        """Tests progress bar complete"""
        transferred = 1048576  # 1MB
        to_be_transferred = 1048576  # 1MB
        suffix = 'test_suffix'
        bar_len = 20
        filled_len = int(round(bar_len * transferred / float(to_be_transferred)))
        bar = '\033[32;1m%s\033[0m' % '=' * filled_len + '-' * (bar_len - filled_len)

        # Call the function under test
        self.remote_client.progress_bar(transferred, to_be_transferred, suffix)
        mock_stdout.flush()

        # Verify that the method is called correctly
        expected_output = 'Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m100.0\033[0m', '% [', self.remote_client.translate_byte(transferred), ']', suffix)
        self.assertIn(expected_output, mock_stdout.getvalue())
        self.assertIn('\r\n', mock_stdout.getvalue())

    @patch('common.ssh_client.remote_client.paramiko.SFTPClient.from_transport')
    def test_upload(self, mock_sftp_client):
        """Tests upload"""
        # Sets the return value of the mock object
        mock_transport = MagicMock()
        self.remote_client._ssh_fd.get_transport.return_value = mock_transport
        mock_sftp_client.return_value = MagicMock()

        # Call the function under test
        remote_path = '/remote/path/file.txt'
        local_path = '/local/path/file.txt'
        self.remote_client.upload(remote_path, local_path)

        # Verify that the method is called correctly
        self.remote_client._ssh_fd.get_transport.assert_called_once()
        mock_sftp_client.assert_called_once_with(mock_transport)
        mock_sftp_client.return_value.put.assert_called_once_with(local_path, remote_path)
        self.assertIsNotNone(self.remote_client._sftp_client)
        mock_sftp_client.return_value.close.assert_called_once()

    def test_ssh_invoke_shell_switch_user_success(self):
        """Tests the ssh_invoke_shell_switch_user command successfully and returns standard output"""
        # Simulate the return value under normal conditions
        self.remote_client._ssh_fd.invoke_shell.return_value.send.return_value = None
        self.remote_client._ssh_fd.invoke_shell.return_value.recv.return_value = b'successful output'

        new_user = 'new_user'
        cmd = 'ls'
        time_out = 1

        # Call the function under test
        result = self.remote_client.ssh_invoke_shell_switch_user(new_user, cmd, time_out)

        # Verify that the method is called correctly
        self.assertEqual(result, 'successful output')
        self.remote_client._ssh_fd.invoke_shell.assert_called_once()
        self.remote_client._ssh_fd.invoke_shell.return_value.send.assert_any_call('su {0}\n'.format(new_user))
        self.remote_client._ssh_fd.invoke_shell.return_value.send.assert_any_call('{}\n'.format(cmd))
        self.remote_client._ssh_fd.close.assert_called_once()

    def test_ssh_invoke_shell_switch_user_exception(self):
        """Tests the ssh_invoke_shell_switch_user command exceptionally"""
        self.remote_client._ssh_fd.invoke_shell.side_effect = paramiko.SSHException("SSH error")

        new_user = 'new_user'
        cmd = 'ls'
        time_out = 1

        with self.assertRaises(OBDIAGShellCmdException) as context:
            self.remote_client.ssh_invoke_shell_switch_user(new_user, cmd, time_out)

        # Verify that the method is called correctly
        self.assertIn("Execute Shell command on server 127.0.0.1 failed", str(context.exception))
        self.assertIn("command=[ls]", str(context.exception))
        self.assertIn("SSH error", str(context.exception))

    def test_get_name(self):
        """Tests get name"""
        name = self.remote_client.get_name()
        self.assertEqual(name, "remote_127.0.0.1")


if __name__ == '__main__':
    unittest.main()
