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
@time: 2024/8/6
@file: test_config_helper.py
@desc: 测试config_helper的 get_old_configuration ~ input_choice_default 方法
"""
import unittest
from unittest import mock
from src.common.config_helper import ConfigHelper


class TestConfigHelper(unittest.TestCase):
    @mock.patch('src.common.config_helper.YamlUtils.write_yaml_data')
    @mock.patch('src.common.config_helper.DirectoryUtil.mkdir')
    @mock.patch('src.common.config_helper.os.path.expanduser')
    @mock.patch('src.common.config_helper.TimeUtils.timestamp_to_filename_time')
    def test_save_old_configuration(self, mock_timestamp_to_filename_time, mock_expanduser, mock_mkdir, mock_write_yaml_data):
        # 模拟时间戳生成函数，返回一个特定的值
        mock_timestamp_to_filename_time.return_value = '20240806_123456'

        # 模拟路径扩展函数
        def mock_expanduser_path(path):
            return {'~/.obdiag/config.yml': '/mock/config.yml', '~/mock/backup/dir': '/mock/backup/dir'}.get(path, path)  # 默认返回原路径

        mock_expanduser.side_effect = mock_expanduser_path

        # 模拟目录创建函数
        mock_mkdir.return_value = None

        # 模拟YAML数据写入函数
        mock_write_yaml_data.return_value = None

        # 创建一个模拟的上下文对象
        context = mock.MagicMock()
        context.inner_config = {"obdiag": {"basic": {"config_backup_dir": "~/mock/backup/dir"}}}

        # 初始化ConfigHelper对象
        config_helper = ConfigHelper(context)

        # 定义一个示例配置
        sample_config = {'key': 'value'}

        # 调用需要测试的方法
        config_helper.save_old_configuration(sample_config)

        # 验证路径扩展是否被正确调用
        mock_expanduser.assert_any_call('~/.obdiag/config.yml')
        mock_expanduser.assert_any_call('~/mock/backup/dir')

        # 验证目录创建是否被正确调用
        mock_mkdir.assert_called_once_with(path='/mock/backup/dir')

        # 验证YAML数据写入是否被正确调用
        expected_backup_path = '/mock/backup/dir/config_backup_20240806_123456.yml'
        mock_write_yaml_data.assert_called_once_with(sample_config, expected_backup_path)

    # 测试带有默认值输入的方法
    @mock.patch('builtins.input')
    def test_input_with_default(self, mock_input):
        # 创建一个模拟的上下文对象（虽然该方法并不需要它）
        context = mock.Mock()
        config_helper = ConfigHelper(context)

        # 测试用户输入为空的情况
        mock_input.return_value = ''
        result = config_helper.input_with_default('username', 'default_user')
        self.assertEqual(result, 'default_user')

        # 测试用户输入为'y'的情况（应该返回默认值）
        mock_input.return_value = 'y'
        result = config_helper.input_with_default('username', 'default_user')
        self.assertEqual(result, 'default_user')

        # 测试用户输入为'yes'的情况（应该返回默认值）
        mock_input.return_value = 'yes'
        result = config_helper.input_with_default('username', 'default_user')
        self.assertEqual(result, 'default_user')

        # 测试用户输入为其他值的情况（应该返回用户输入）
        mock_input.return_value = 'custom_user'
        result = config_helper.input_with_default('username', 'default_user')
        self.assertEqual(result, 'custom_user')

    # 测试带有默认值的密码输入方法
    @mock.patch('src.common.config_helper.pwinput.pwinput')
    def test_input_password_with_default(self, mock_pwinput):
        # 创建一个模拟的上下文对象
        context = mock.MagicMock()
        config_helper = ConfigHelper(context)

        # 测试密码输入为空的情况，应该返回默认值
        mock_pwinput.return_value = ''
        result = config_helper.input_password_with_default("password", "default_password")
        self.assertEqual(result, "default_password")

        # 测试密码输入为'y'的情况，应该返回默认值
        mock_pwinput.return_value = 'y'
        result = config_helper.input_password_with_default("password", "default_password")
        self.assertEqual(result, "default_password")

        # 测试密码输入为'yes'的情况，应该返回默认值
        mock_pwinput.return_value = 'yes'
        result = config_helper.input_password_with_default("password", "default_password")
        self.assertEqual(result, "default_password")

        # 测试密码输入为其他值的情况，应该返回输入值
        mock_pwinput.return_value = 'custom_password'
        result = config_helper.input_password_with_default("password", "default_password")
        self.assertEqual(result, "custom_password")

    # 测试带有默认选项的选择输入方法
    @mock.patch('src.common.config_helper.input')
    def test_input_choice_default(self, mock_input):
        # 创建一个模拟的上下文对象
        context = mock.MagicMock()
        config_helper = ConfigHelper(context)

        # 测试输入为'y'的情况，应该返回True
        mock_input.return_value = 'y'
        result = config_helper.input_choice_default("choice", "N")
        self.assertTrue(result)

        # 测试输入为'yes'的情况，应该返回True
        mock_input.return_value = 'yes'
        result = config_helper.input_choice_default("choice", "N")
        self.assertTrue(result)

        # 测试输入为'n'的情况，应该返回False
        mock_input.return_value = 'n'
        result = config_helper.input_choice_default("choice", "N")
        self.assertFalse(result)

        # 测试输入为'no'的情况，应该返回False
        mock_input.return_value = 'no'
        result = config_helper.input_choice_default("choice", "N")
        self.assertFalse(result)

        # 测试输入为空字符串的情况，应该返回False
        mock_input.return_value = ''
        result = config_helper.input_choice_default("choice", "N")
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
