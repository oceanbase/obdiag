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
@time: 2024/07/22
@file: test_local_client.py
@desc:
"""

import unittest
import subprocess32 as subprocess
from unittest.mock import patch, MagicMock
from common.ssh_client.local_client import LocalClient
from context import HandlerContext


class TestLocalClient(unittest.TestCase):
    def test_init_with_context_and_node(self):
        """Test the initialization when you pass context and node"""
        context = HandlerContext()
        node = {}
        client = LocalClient(context=context, node=node)
        self.assertEqual(client.context, context)
        self.assertEqual(client.node, node)

    def test_init_with_only_node(self):
        """Test the initialization when you pass context and node"""
        node = {}
        client = LocalClient(context=None, node=node)
        self.assertIsNone(client.context)
        self.assertEqual(client.node, node)

    def test_init_with_only_context(self):
        """Tests initialization only when context is passed in"""
        context = HandlerContext()
        self.assertRaises(AttributeError, LocalClient, context, None)

    def test_init_with_no_args(self):
        """Tests initialization without passing any parameters"""
        self.assertRaises(AttributeError, LocalClient, None, None)

    def setUp(self):
        context = HandlerContext()
        node = {}
        self.local_client = LocalClient(context=context, node=node)
        self.local_client.stdio = MagicMock()
        self.local_client.client = MagicMock()

    @patch('subprocess.Popen')
    def test_exec_cmd_success(self, mock_popen):
        """Tests the exec_cmd command successfully and returns standard output"""
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"stdout output", b"")
        mock_popen.return_value = mock_process

        # Act
        result = self.local_client.exec_cmd("echo 'Hello World'")

        # Assert
        self.assertEqual(result, "stdout output")
        self.local_client.stdio.verbose.assert_called_with("[local host] run cmd = [echo 'Hello World'] on localhost")

    @patch('subprocess.Popen')
    def test_exec_cmd_failure(self, mock_popen):
        """Tests the exec_cmd command unsuccessfully and returns stderr output"""
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"stderr output")
        mock_popen.return_value = mock_process

        # Act
        result = self.local_client.exec_cmd("exit 1")

        # Assert
        self.assertEqual(result, "stderr output")
        self.local_client.stdio.verbose.assert_called_with("[local host] run cmd = [exit 1] on localhost")

    @patch('subprocess.Popen')
    def test_exec_cmd_exception(self, mock_popen):
        """Tests the exec_cmd command exceptionally"""
        mock_popen.side_effect = Exception("Popen error")

        # Act
        with self.assertRaises(Exception) as context:
            self.local_client.exec_cmd("exit 1")

        # Assert
        self.assertIn("Execute Shell command failed", str(context.exception))
        self.local_client.stdio.error.assert_called_with("run cmd = [exit 1] on localhost, Exception = [Popen error]")

    @patch('common.ssh_client.local_client.shutil.copy')
    def test_download_success(self, mock_copy):
        """Tests the download command successfully"""
        remote_path = "/path/to/remote/file"
        local_path = "/path/to/local/file"

        # Act
        self.local_client.download(remote_path, local_path)

        # Assert
        mock_copy.assert_called_once_with(remote_path, local_path)
        self.local_client.stdio.error.assert_not_called()

    @patch('common.ssh_client.local_client.shutil.copy')
    def test_download_failure(self, mock_copy):
        """Tests the download command unsuccessfully"""
        mock_copy.side_effect = Exception('copy error')
        remote_path = "/path/to/remote/file"
        local_path = "/path/to/local/file"

        # Act & Assert
        with self.assertRaises(Exception) as context:
            self.local_client.download(remote_path, local_path)

        self.assertTrue("download file from localhost" in str(context.exception))
        self.local_client.stdio.error.assert_called_once()

    @patch('common.ssh_client.local_client.shutil.copy')
    def test_upload_success(self, mock_copy):
        """Tests the upload command successfully"""
        remote_path = '/tmp/remote_file.txt'
        local_path = '/tmp/local_file.txt'

        # Act
        self.local_client.upload(remote_path, local_path)

        # Assert
        mock_copy.assert_called_once_with(local_path, remote_path)
        self.local_client.stdio.error.assert_not_called()

    @patch('common.ssh_client.local_client.shutil.copy')
    def test_upload_failure(self, mock_copy):
        """Tests the upload command unsuccessfully"""
        mock_copy.side_effect = Exception('copy error')
        remote_path = '/tmp/remote_file.txt'
        local_path = '/tmp/local_file.txt'

        # Act & Assert
        with self.assertRaises(Exception) as context:
            self.local_client.upload(remote_path, local_path)

        self.assertIn('upload file to localhost', str(context.exception))
        self.local_client.stdio.error.assert_called_once()

    @patch('subprocess.Popen')
    def test_ssh_invoke_shell_switch_user_success(self, mock_popen):
        """Tests the ssh_invoke_shell_switch_user command successfully and returns standard output"""
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"successful output", b"")
        mock_popen.return_value = mock_process

        # Act
        result = self.local_client.ssh_invoke_shell_switch_user("new_user", 'echo "Hello World"', 10)

        # Assert
        self.assertEqual(result, "successful output")
        self.local_client.stdio.verbose.assert_called_once()
        mock_popen.assert_called_once_with("su - new_user -c 'echo \"Hello World\"'", stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')

    @patch('subprocess.Popen')
    def test_ssh_invoke_shell_switch_user_failure(self, mock_popen):
        """Tests the ssh_invoke_shell_switch_user command unsuccessfully and returns standard output"""
        mock_process = MagicMock()
        mock_process.communicate.return_value = (b"", b"error output")
        mock_popen.return_value = mock_process

        # Act
        result = self.local_client.ssh_invoke_shell_switch_user("new_user", 'echo "Hello World"', 10)

        # Assert
        self.assertEqual(result, "error output")
        self.local_client.stdio.verbose.assert_called_once()
        mock_popen.assert_called_once_with("su - new_user -c 'echo \"Hello World\"'", stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')

    @patch('subprocess.Popen')
    def test_ssh_invoke_shell_switch_user_exception(self, mock_popen):
        """Tests the ssh_invoke_shell_switch_user command exceptionally"""
        mock_popen.side_effect = Exception("Popen error")

        # Act
        with self.assertRaises(Exception) as context:
            self.local_client.ssh_invoke_shell_switch_user("new_user", "echo 'Hello World'", 10)

        # Assert
        self.assertTrue("the client type is not support ssh invoke shell switch user" in str(context.exception))
        self.local_client.stdio.error.assert_called_once()

    def test_get_name(self):
        """Tests get name of ssh client"""
        name = self.local_client.get_name()
        # Assert
        self.assertEqual(name, "local")

    def test_get_ip(self):
        """Tests get ip of ssh client"""
        expected_ip = '127.0.0.1'
        self.local_client.client.get_ip.return_value = expected_ip

        # Act
        ip = self.local_client.get_ip()

        # Assert
        self.assertEqual(ip, expected_ip)
        self.local_client.client.get_ip.assert_called_once()


if __name__ == '__main__':
    unittest.main()
