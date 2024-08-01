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

import unittest
from unittest.mock import MagicMock
from common.command import *

# 测试类，用于测试common.command模块中的函数
class TestCommonCommand(unittest.TestCase):

    # 准备测试环境，创建模拟的SSH客户端和标准输入输出对象
    def setUp(self):
        self.ssh_client = MagicMock()
        self.stdio = MagicMock()

    # 测试下载文件功能
    def test_download_file(self):
        # 设置远程和本地文件路径
        remote_path = "/remote/path/file.txt"
        local_path = "/local/path/file.txt"

        # 执行下载文件操作
        result = download_file(self.ssh_client, remote_path, local_path, self.stdio)

        # 验证是否正确调用了SSH客户端的下载方法，并检查返回结果是否为本地文件路径
        self.ssh_client.download.assert_called_once_with(remote_path, local_path)
        self.assertEqual(result, local_path)

    # 测试上传文件功能
    def test_upload_file(self):
        # 设置本地和远程文件路径
        local_path = "/local/path/file.txt"
        remote_path = "/remote/path/file.txt"

        # 执行上传文件操作
        upload_file(self.ssh_client, local_path, remote_path, self.stdio)

        # 验证是否正确调用了SSH客户端的上传方法
        self.ssh_client.upload.assert_called_once_with(remote_path, local_path)

    # 测试远程删除文件夹功能
    def test_rm_rf_file(self):
        # 设置远程文件夹路径
        dir = "/remote/path"

        # 执行删除操作
        rm_rf_file(self.ssh_client, dir, self.stdio)

        # 验证是否正确调用了SSH客户端的执行命令方法，并传入了正确的删除命令
        self.ssh_client.exec_cmd.assert_called_once_with(f"rm -rf {dir}")

    # 测试删除文件夹中文件的功能
    def test_delete_file_in_folder(self):
        # 设置远程文件夹路径
        file_path = "/remote/path/folder"

        # 执行删除操作并期望抛出异常
        with self.assertRaises(Exception) as context:
            delete_file_in_folder(self.ssh_client, file_path, self.stdio)

        # 验证异常中是否包含特定的字符串
        self.assertTrue("gather_pack" in str(context.exception))

    # 测试判断远程目录是否为空
    def test_is_empty_dir(self):
        # 设置远程目录路径
        dir_path = "/remote/path/dir"

        # 执行判断操作，并设置返回值为"1"，表示目录不为空
        self.ssh_client.exec_cmd.return_value = "1"
        result = is_empty_dir(self.ssh_client, dir_path, self.stdio)

        # 验证是否正确执行了命令，并判断结果是否为False
        self.ssh_client.exec_cmd.assert_called_once_with(f"ls -A {dir_path}|wc -w")
        self.assertFalse(result)

        # 执行判断操作，并设置返回值为"0"，表示目录为空
        self.ssh_client.exec_cmd.return_value = "0"
        result = is_empty_dir(self.ssh_client, dir_path, self.stdio)

        # 判断结果是否为True
        self.assertTrue(result)

    # 测试获取文件起始时间的功能
    def test_get_file_start_time(self):
        # 设置文件名和远程目录路径
        file_name = "file.log"
        dir = "/remote/path"

        # 执行获取起始时间操作，并设置返回值为特定的时间字符串
        self.ssh_client.exec_cmd.return_value = "2022-01-01 00:00:00"
        result = get_file_start_time(self.ssh_client, file_name, dir, self.stdio)

        # 验证是否正确执行了命令，并检查返回结果是否为预期的时间字符串
        self.ssh_client.exec_cmd.assert_called_once_with(f"head -n 1 {dir}/{file_name}")
        self.assertEqual(result, "2022-01-01 00:00:00")


if __name__ == '__main__':
    unittest.main()
