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
@time: 2026/6/8
@file: test_sql_syntax_handler.py
@desc:
"""

import unittest
from unittest.mock import MagicMock

import pymysql as mysql

from src.common.result_type import ObdiagResult
from src.handler.tools.sql_syntax_handler import SqlSyntaxHandler, is_ddl_statement, normalize_sql_for_syntax_check, quote_sql_literal


class _Stdio:
    def print(self, *args, **kwargs):
        pass

    def verbose(self, *args, **kwargs):
        pass

    def warn(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class TestSqlSyntaxHandler(unittest.TestCase):
    def setUp(self):
        context = MagicMock()
        context.stdio = _Stdio()
        context.options = {}
        self.handler = SqlSyntaxHandler(context)

    def test_normalize_accepts_single_statement(self):
        sql, err = normalize_sql_for_syntax_check("  SELECT 1;; ")
        self.assertIsNone(err)
        self.assertEqual(sql, "SELECT 1")

    def test_normalize_rejects_multiple_statements(self):
        sql, err = normalize_sql_for_syntax_check("SELECT 1; SELECT 2")
        self.assertIsNotNone(err)
        self.assertIsNone(sql)

    def test_detects_ddl_statement(self):
        self.assertTrue(is_ddl_statement("CREATE TABLE t1(id int)"))
        self.assertTrue(is_ddl_statement("/* comment */ ALTER TABLE t1 ADD c1 int"))
        self.assertTrue(is_ddl_statement("  /* a */ /* b */ DROP TABLE t1"))
        self.assertFalse(is_ddl_statement("SELECT * FROM t1"))
        self.assertFalse(is_ddl_statement("/* unfinished comment CREATE TABLE t1(id int)"))

    def test_quote_sql_literal_escapes_backslash_and_quote(self):
        self.assertEqual(quote_sql_literal("CREATE TABLE `a\\b` (c varchar(10) default 'x')"), "CREATE TABLE `a\\\\b` (c varchar(10) default ''x'')")

    def test_dml_uses_explain(self):
        connector = MagicMock()
        result = self.handler._check_syntax(connector, "SELECT * FROM t1")
        self.assertEqual(result.code, ObdiagResult.SUCCESS_CODE)
        self.assertEqual(result.data["result"], "VALID")
        connector.execute_sql.assert_called_once_with("EXPLAIN SELECT * FROM t1")

    def test_ddl_uses_prepare_without_execute(self):
        connector = MagicMock()
        result = self.handler._check_syntax(connector, "CREATE TABLE t1(id int)")
        self.assertEqual(result.code, ObdiagResult.SUCCESS_CODE)
        self.assertEqual(result.data["result"], "VALID")
        self.assertEqual(result.data["method"], "PREPARE")
        calls = [call.args[0] for call in connector.execute_sql.call_args_list]
        self.assertEqual(calls[0], "PREPARE obdiag_sql_syntax_stmt FROM 'CREATE TABLE t1(id int)'")
        self.assertEqual(calls[1], "DEALLOCATE PREPARE obdiag_sql_syntax_stmt")

    def test_ddl_syntax_error_does_not_deallocate_unprepared_statement(self):
        connector = MagicMock()
        connector.execute_sql.side_effect = mysql.Error(1064, "syntax error")
        result = self.handler._check_syntax(connector, "CREATE TABLE t1(")
        self.assertEqual(result.data["result"], "SYNTAX_ERROR")
        connector.execute_sql.assert_called_once()

    def test_non_1064_error_is_reported_as_semantic_error(self):
        connector = MagicMock()
        connector.execute_sql.side_effect = mysql.Error(1146, "table does not exist")
        result = self.handler._check_syntax(connector, "SELECT * FROM missing_table")
        self.assertEqual(result.data["result"], "SEMANTIC_ERROR")
        self.assertEqual(result.data["error_code"], 1146)


if __name__ == '__main__':
    unittest.main()
