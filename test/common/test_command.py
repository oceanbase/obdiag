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
@time: 2024/08/06
@file: test_command.py
@desc: 测试到command的delete_file_in_folder方法
"""
import unittest
from unittest.mock import Mock, patch
import subprocess
from src.common.command import LocalClient, delete_file_in_folder, rm_rf_file, upload_file, download_file


class TestLocalClient(unittest.TestCase):
    def setUp(self):
        self.stdio = Mock()
        self.local_client = LocalClient(stdio=self.stdio)
        self.ssh_client = Mock()

    @patch('subprocess.Popen')
    def test_run_success(self, mock_popen):
        # 模拟命令成功执行
        mock_process = Mock()
        mock_process.communicate.return_value = (b'success', None)
        mock_popen.return_value = mock_process

        cmd = 'echo "hello"'
        result = self.local_client.run(cmd)

        # 验证 verbose 和 Popen 调用
        self.stdio.verbose.assert_called_with("[local host] run cmd = [echo \"hello\"] on localhost")
        mock_popen.assert_called_with(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')

        # 验证结果
        self.assertEqual(result, b'success')

    @patch('subprocess.Popen')
    def test_run_failure(self, mock_popen):
        # 模拟命令执行失败
        mock_process = Mock()
        mock_process.communicate.return_value = (b'', b'error')
        mock_popen.return_value = mock_process

        cmd = 'echo "hello"'
        result = self.local_client.run(cmd)

        # 验证 verbose 和 Popen 调用
        self.stdio.verbose.assert_called_with("[local host] run cmd = [echo \"hello\"] on localhost")
        mock_popen.assert_called_with(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')

        # 验证错误处理
        self.stdio.error.assert_called_with("run cmd = [echo \"hello\"] on localhost, stderr=[b'error']")
        self.assertEqual(result, b'')

    @patch('subprocess.Popen')
    def test_run_exception(self, mock_popen):
        # 模拟命令执行时抛出异常
        mock_popen.side_effect = Exception('Test exception')

        cmd = 'echo "hello"'
        result = self.local_client.run(cmd)

        # 验证 verbose 调用和异常处理
        self.stdio.verbose.assert_called_with("[local host] run cmd = [echo \"hello\"] on localhost")
        self.stdio.error.assert_called_with("run cmd = [echo \"hello\"] on localhost")
        self.assertIsNone(result)

    @patch('subprocess.Popen')
    def test_run_get_stderr_success(self, mock_popen):
        # 模拟命令成功执行
        mock_process = Mock()
        mock_process.communicate.return_value = (b'success', b'')
        mock_popen.return_value = mock_process

        cmd = 'echo "hello"'
        result = self.local_client.run_get_stderr(cmd)

        # 验证 verbose 和 Popen 调用
        self.stdio.verbose.assert_called_with("run cmd = [echo \"hello\"] on localhost")
        mock_popen.assert_called_with(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')

        # 验证结果
        self.assertEqual(result, b'')

    @patch('subprocess.Popen')
    def test_run_get_stderr_failure(self, mock_popen):
        # 模拟命令执行失败
        mock_process = Mock()
        mock_process.communicate.return_value = (b'', b'error')
        mock_popen.return_value = mock_process

        cmd = 'echo "hello"'
        result = self.local_client.run_get_stderr(cmd)

        # 验证 verbose 和 Popen 调用
        self.stdio.verbose.assert_called_with("run cmd = [echo \"hello\"] on localhost")
        mock_popen.assert_called_with(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')

        # 验证错误处理
        # 因为 stdout 和 stderr 都是 b''，stderr 应该是 b'error'
        self.assertEqual(result, b'error')

        # 检查 error 方法是否被调用，且调用内容是否正确
        # 注意：在正常情况下 error 方法不应该被调用，只有异常情况才会被调用。
        # 确保 error 方法在异常情况下被调用
        self.stdio.error.assert_not_called()

    @patch('subprocess.Popen')
    def test_run_get_stderr_exception(self, mock_popen):
        # 模拟命令执行时抛出异常
        mock_popen.side_effect = Exception('Test exception')

        cmd = 'echo "hello"'
        result = self.local_client.run_get_stderr(cmd)

        # 验证 verbose 调用和异常处理
        self.stdio.verbose.assert_called_with("run cmd = [echo \"hello\"] on localhost")
        self.stdio.error.assert_called_with(f"run cmd = [{cmd}] on localhost")
        self.assertIsNone(result)

    def test_download_file_success(self):
        remote_path = "/remote/path/file.txt"
        local_path = "/local/path/file.txt"

        result = download_file(self.ssh_client, remote_path, local_path, self.stdio)

        self.ssh_client.download.assert_called_once_with(remote_path, local_path)
        self.assertEqual(result, local_path)
        self.stdio.error.assert_not_called()
        self.stdio.verbose.assert_not_called()

    def test_download_file_failure(self):
        remote_path = "/remote/path/file.txt"
        local_path = "/local/path/file.txt"

        self.ssh_client.download.side_effect = Exception("Simulated download exception")

        result = download_file(self.ssh_client, remote_path, local_path, self.stdio)

        self.ssh_client.download.assert_called_once_with(remote_path, local_path)
        self.assertEqual(result, local_path)
        self.stdio.error.assert_called_once_with("Download File Failed error: Simulated download exception")
        self.stdio.verbose.assert_called_once()

    def test_upload_file_success(self):
        local_path = "/local/path/file.txt"
        remote_path = "/remote/path/file.txt"
        self.ssh_client.get_name.return_value = "test_server"

        upload_file(self.ssh_client, local_path, remote_path, self.stdio)

        self.ssh_client.upload.assert_called_once_with(remote_path, local_path)
        self.stdio.verbose.assert_called_once_with("Please wait a moment, upload file to server test_server, local file path /local/path/file.txt, remote file path /remote/path/file.txt")
        self.stdio.error.assert_not_called()

    def test_rm_rf_file_success(self):
        dir_path = "/path/to/delete"

        rm_rf_file(self.ssh_client, dir_path, self.stdio)

        self.ssh_client.exec_cmd.assert_called_once_with("rm -rf /path/to/delete")

    def test_rm_rf_file_empty_dir(self):
        dir_path = ""

        rm_rf_file(self.ssh_client, dir_path, self.stdio)

        self.ssh_client.exec_cmd.assert_called_once_with("rm -rf ")

    def test_rm_rf_file_special_chars(self):
        dir_path = "/path/to/delete; echo 'This is a test'"

        rm_rf_file(self.ssh_client, dir_path, self.stdio)

        self.ssh_client.exec_cmd.assert_called_once_with("rm -rf /path/to/delete; echo 'This is a test'")

    def test_delete_file_in_folder_success(self):
        file_path = "/path/to/gather_pack"

        delete_file_in_folder(self.ssh_client, file_path, self.stdio)

        self.ssh_client.exec_cmd.assert_called_once_with("rm -rf /path/to/gather_pack/*")

    def test_delete_file_in_folder_none_path(self):
        file_path = None

        with self.assertRaises(Exception) as context:
            delete_file_in_folder(self.ssh_client, file_path, self.stdio)

        self.assertTrue("Please check file path, None" in str(context.exception))

    def test_delete_file_in_folder_invalid_path(self):
        file_path = "/path/to/invalid_folder"

        with self.assertRaises(Exception) as context:
            delete_file_in_folder(self.ssh_client, file_path, self.stdio)

        self.assertTrue("Please check file path, /path/to/invalid_folder" in str(context.exception))

    def test_delete_file_in_folder_special_chars(self):
        file_path = "/path/to/gather_pack; echo 'test'"

        delete_file_in_folder(self.ssh_client, file_path, self.stdio)

        self.ssh_client.exec_cmd.assert_called_once_with("rm -rf /path/to/gather_pack; echo 'test'/*")


if __name__ == '__main__':
    unittest.main()
