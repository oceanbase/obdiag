#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
@time: 2025/2/19
@file: test_fill_sql_with_params.py
@desc: Test cases for fill_sql_with_params method
"""

import unittest
from unittest.mock import Mock
from src.common.tool import StringUtils


class TestFillSQLWithParams(unittest.TestCase):

    def setUp(self):
        self.string_utils = StringUtils()
        self.string_utils.stdio = Mock()

    def test_basic_string_params(self):
        """Test basic string parameter replacement"""
        sql = "SELECT * FROM users WHERE name = ? AND age = ?"
        params = "'John', 25"
        result = self.string_utils.fill_sql_with_params(sql, params, self.string_utils.stdio)
        expected = "SELECT * FROM users WHERE name = 'John' AND age = 25"
        self.assertEqual(result, expected)

    def test_quoted_string_params(self):
        """Test quoted string parameters"""
        sql = "INSERT INTO users (name, email) VALUES (?, ?)"
        params = "'Alice', 'alice@example.com'"
        result = self.string_utils.fill_sql_with_params(sql, params, self.string_utils.stdio)
        expected = "INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')"
        self.assertEqual(result, expected)

    def test_numeric_params(self):
        """Test numeric parameters"""
        sql = "SELECT * FROM products WHERE price > ? AND stock < ?"
        params = "100, 50"
        result = self.string_utils.fill_sql_with_params(sql, params, self.string_utils.stdio)
        expected = "SELECT * FROM products WHERE price > 100 AND stock < 50"
        self.assertEqual(result, expected)

    def test_mixed_params(self):
        """Test mixed type parameters"""
        sql = "UPDATE users SET status = ?, last_login = ? WHERE id = ?"
        params = "'active', '2025-01-01', 123"
        result = self.string_utils.fill_sql_with_params(sql, params, self.string_utils.stdio)
        expected = "UPDATE users SET status = 'active', last_login = '2025-01-01' WHERE id = 123"
        self.assertEqual(result, expected)

    def test_empty_params(self):
        """Test empty parameters"""
        sql = "SELECT * FROM users"
        params = ""
        result = self.string_utils.fill_sql_with_params(sql, params, self.string_utils.stdio)
        self.assertEqual(result, sql)

    def test_no_placeholders(self):
        """Test SQL without placeholders"""
        sql = "SELECT * FROM users WHERE id = 1"
        params = "100"
        result = self.string_utils.fill_sql_with_params(sql, params, self.string_utils.stdio)
        self.assertEqual(result, sql)

    def test_parameter_mismatch(self):
        """Test parameter count mismatch"""
        sql = "SELECT * FROM users WHERE id = ? AND name = ?"
        params = "100"
        result = self.string_utils.fill_sql_with_params(sql, params, self.string_utils.stdio)
        expected = "SELECT * FROM users WHERE id = 100 AND name = ?"
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()
