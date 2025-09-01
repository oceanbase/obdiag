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
@time: 2024/06/05
@file: test_is_null_rule.py
@desc:
"""
import unittest
from src.handler.analyzer.sql.rules.review.is_null import IsNullRule
from sqlgpt_parser.parser.oceanbase_parser import parser
from src.handler.analyzer.sql.rules.level import Level


class TestIsNullRule(unittest.TestCase):

    def setUp(self):
        self.rule = IsNullRule()

    def test_improper_null_comparison(self):
        # 测试不当的NULL值比较
        sqls = ["SELECT * FROM table1 WHERE column1 = NULL", "SELECT * FROM table1 WHERE column1 <> NULL", "SELECT * FROM table1 WHERE NULL = column1", "SELECT * FROM table1 WHERE NULL <> column1"]

        for sql in sqls:
            parsed_stmt = parser.parse(sql)
            self.assertTrue(self.rule.match(parsed_stmt), f"Expected to match for SQL: {sql}")
            suggestion = self.rule.suggestion(parsed_stmt)
            self.assertEqual(suggestion.level, Level.WARN)

    def test_proper_null_check(self):
        # 测试正确的NULL值检查
        proper_sqls = ["SELECT * FROM table1 WHERE column1 IS NULL", "SELECT * FROM table1 WHERE column1 IS NOT NULL"]

        for sql in proper_sqls:
            parsed_stmt = parser.parse(sql)
            self.assertFalse(self.rule.match(parsed_stmt), f"Should not match for SQL: {sql}")
            suggestion = self.rule.suggestion(parsed_stmt)
            self.assertEqual(suggestion.level, Level.OK)

    def test_mixed_query(self):
        # 混合了适当与不适当的NULL比较
        sql = "SELECT * FROM table1 WHERE column1 IS NULL OR column2 = NULL"
        parsed_stmt = parser.parse(sql)
        self.assertTrue(self.rule.match(parsed_stmt), "Expected to match due to improper NULL comparison")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.WARN)


if __name__ == '__main__':
    unittest.main()
