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
@time: 2024/06/05
@file: test_large_in_clause_rule.py
@desc:
"""

import unittest
from obdiag.handler.analyzer.sql.rules.review.large_in_clause import LargeInClauseAdjustedRule
from sqlgpt_parser.parser.oceanbase_parser import parser
from obdiag.handler.analyzer.sql.rules.level import Level


class TestLargeInClauseAdjustedRule(unittest.TestCase):

    def setUp(self):
        self.rule = LargeInClauseAdjustedRule()

    def test_large_in_clause(self):
        # 构建一个超过200个元素的IN子句的SQL语句
        large_in_clause_sql = "SELECT * FROM table1 WHERE id IN (" + ','.join(['?'] * 201) + ")"
        parsed_stmt = parser.parse(large_in_clause_sql)

        self.assertTrue(self.rule.match(parsed_stmt), "Expected to match for SQL with over 200 IN elements")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.WARN)

    def test_small_in_clause(self):
        # 构建一个少于200个元素的IN子句的SQL语句
        small_in_clause_sql = "SELECT * FROM table1 WHERE id IN (" + ','.join(['?'] * 199) + ")"
        parsed_stmt = parser.parse(small_in_clause_sql)

        self.assertFalse(self.rule.match(parsed_stmt), "Should not match for SQL within the limit of 200 IN elements")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_no_in_clause(self):
        # 构建一个不包含IN子句的SQL语句
        no_in_clause_sql = "SELECT * FROM table1 WHERE column = 'value'"
        parsed_stmt = parser.parse(no_in_clause_sql)

        self.assertFalse(self.rule.match(parsed_stmt), "Should not match for SQL without an IN clause")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)


if __name__ == '__main__':
    unittest.main()
