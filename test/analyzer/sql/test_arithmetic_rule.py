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
@file: test_arithmetic_rule.py
@desc:
"""
import unittest
from src.handler.analyzer.sql.rules.review.arithmetic import ArithmeticRule
from sqlgpt_parser.parser.oceanbase_parser import parser
from src.handler.analyzer.sql.rules.level import Level


class TestArithmeticRuleWithRealSQL(unittest.TestCase):

    def setUp(self):
        self.rule = ArithmeticRule()
        self.parser = parser

    def test_arithmetic_operation_detected(self):
        # SQL语句包含算术运算
        sql_with_arithmetic = "SELECT * FROM table1 WHERE column1 + 1 > 2"
        parsed_stmt = self.parser.parse(sql_with_arithmetic)
        result = self.rule.match(parsed_stmt, None)
        self.assertTrue(result)

    def test_no_arithmetic_operation(self):
        # SQL语句不包含算术运算
        sql_no_arithmetic = "SELECT * FROM table1 WHERE column1 > 2"
        parsed_stmt = self.parser.parse(sql_no_arithmetic)
        result = self.rule.match(parsed_stmt, None)
        self.assertFalse(result)

    def test_suggestion_for_arithmetic_operation(self):
        sql_with_arithmetic = "SELECT * FROM table1 WHERE column1 + 1 > 2"
        parsed_stmt = self.parser.parse(sql_with_arithmetic)
        result = self.rule.suggestion(parsed_stmt, None)
        self.assertEqual(result.level, Level.NOTICE)

    def test_suggestion_without_arithmetic_operation(self):
        sql_no_arithmetic = "SELECT * FROM table1 WHERE column1 > 2"
        parsed_stmt = self.parser.parse(sql_no_arithmetic)
        result = self.rule.suggestion(parsed_stmt, None)
        self.assertEqual(result.level, Level.OK)

    def test_complex_arithmetic_operation_detected(self):
        # 复杂SQL包含算术运算，并且嵌套在子查询中
        sql_complex = """
        SELECT t1.id
        FROM table1 t1
        JOIN (
            SELECT id, column1 - column2 + 1 AS derived_col
            FROM table2
            WHERE column3 * 2 < 10
        ) t2 ON t1.id = t2.id
        WHERE t2.derived_col > 5
        """
        parsed_stmt = self.parser.parse(sql_complex)
        result = self.rule.match(parsed_stmt, None)
        self.assertTrue(result, "Should detect arithmetic operation in complex SQL statement.")

    def test_complex_no_arithmetic_operation(self):
        # 复杂SQL，无算术运算，包含JOIN和子查询
        sql_complex_no_arithmetic = """
        SELECT t1.id
        FROM table1 t1
        JOIN (
            SELECT id, column1
            FROM table2
            WHERE column3 < 10
        ) t2 ON t1.id = t2.id
        WHERE t2.column1 > 5
        """
        parsed_stmt = self.parser.parse(sql_complex_no_arithmetic)
        result = self.rule.match(parsed_stmt, None)
        self.assertFalse(result, "Should not detect arithmetic operation in complex SQL statement.")


if __name__ == '__main__':
    unittest.main()
