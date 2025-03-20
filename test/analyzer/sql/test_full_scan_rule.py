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
@file: test_full_scan_rule.py
@desc:
"""
import unittest
from src.handler.analyzer.sql.rules.review.full_scan import FullScanRule
from sqlgpt_parser.parser.oceanbase_parser import parser
from src.handler.analyzer.sql.rules.level import Level


class TestFullScanRule(unittest.TestCase):

    def setUp(self):
        self.rule = FullScanRule()

    def test_full_scan_with_negation_but_filtered(self):
        # SQL查询示例，包含否定条件，预期是全表扫描
        sql_filtered_negation = "SELECT * FROM users WHERE NOT (id BETWEEN 1 AND 10)"
        parsed_stmt = parser.parse(sql_filtered_negation)
        print(parsed_stmt)
        self.assertTrue(self.rule.match(parsed_stmt))

    def test_full_scan_with_like_pattern_full(self):
        # SQL查询示例，使用LIKE且模式为%，预期是全表扫描
        sql_like_full = "SELECT * FROM users WHERE username LIKE '%zhangsan'"
        parsed_stmt = parser.parse(sql_like_full)
        print(parsed_stmt)
        self.assertTrue(self.rule.match(parsed_stmt))
        # suggestion = self.rule.suggestion(parsed_stmt)
        # self.assertEqual(suggestion.level, Level.WARN)

    def test_not_in_doesnt_hide_full_scan(self):
        # SQL查询示例，使用NOT IN，预期可能为全表扫描
        sql_not_in = "SELECT * FROM orders WHERE customerId NOT IN (SELECT customerId FROM active_customers)"
        parsed_stmt = parser.parse(sql_not_in)
        self.assertFalse(self.rule.match(parsed_stmt))

    def test_optimized_not_conditions(self):
        # SQL查询示例，使用NOT条件
        sql_optimized_not = "SELECT * FROM users WHERE age NOT BETWEEN 18 AND 25"
        parsed_stmt = parser.parse(sql_optimized_not)
        self.assertTrue(self.rule.match(parsed_stmt))
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.WARN)


if __name__ == '__main__':
    unittest.main()
