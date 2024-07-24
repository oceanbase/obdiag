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

    @patch('subprocess.Popen')
    def test_exec_cmd_success(self, mock_popen):
        """Tests the exec_cmd command successfully and returns standard output"""
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("stdout output".encode("utf-8"), "")
        mock_popen.return_value = mock_process

        # execute the test
        result = self.local_client.exec_cmd("echo 'Hello World'")

        # assert
        self.assertEqual(result, "stdout output")
        self.local_client.stdio.verbose.assert_called_with("[local host] run cmd = [echo 'Hello World'] on localhost")

    @patch('subprocess.Popen')
    def test_exec_cmd_failure(self, mock_popen):
        """Tests the exec_cmd command unsuccessfully and returns stderr output"""
        mock_process = MagicMock()
        mock_process.communicate.return_value = ("", "stderr output".encode("utf-8"))
        mock_popen.return_value = mock_process

        # execute the test
        result = self.local_client.exec_cmd("exit 1")

        # assert
        self.assertEqual(result, "stderr output")
        self.local_client.stdio.verbose.assert_called_with("[local host] run cmd = [exit 1] on localhost")

    @patch('subprocess.Popen')
    def test_exec_cmd_exception(self, mock_popen):
        """Tests the exec_cmd command exceptionally"""
        mock_popen.side_effect = Exception("Popen error")

        # execute the test
        with self.assertRaises(Exception) as context:
            self.local_client.exec_cmd("exit 1")

        # assert
        self.assertIn("Execute Shell command failed", str(context.exception))
        self.local_client.stdio.error.assert_called_with("run cmd = [exit 1] on localhost, Exception = [Popen error]")


if __name__ == '__main__':
    unittest.main()
