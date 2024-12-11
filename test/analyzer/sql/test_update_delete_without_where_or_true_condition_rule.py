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
@file: test_update_delete_multi_table_rule.py
@desc:
"""
import unittest
from src.handler.analyzer.sql.rules.review.update_delete_without_where_or_true_condition import UpdateDeleteWithoutWhereOrTrueConditionRule
from sqlgpt_parser.parser.oceanbase_parser import parser
from src.handler.analyzer.sql.rules.level import Level


class TestUpdateDeleteWithoutWhereConditionRule(unittest.TestCase):

    def setUp(self):
        self.rule = UpdateDeleteWithoutWhereOrTrueConditionRule()

    def test_update_without_where(self):
        sql_without_where_update = "UPDATE table1 SET column1 = 'new_value'"
        parsed_stmt = parser.parse(sql_without_where_update)
        result = self.rule.match(parsed_stmt)
        self.assertTrue(result, "Should detect UPDATE without WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.CRITICAL)

    def test_delete_without_where(self):
        sql_without_where_delete = "DELETE FROM table1"
        parsed_stmt = parser.parse(sql_without_where_delete)
        result = self.rule.match(parsed_stmt)
        self.assertTrue(result, "Should detect DELETE without WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.CRITICAL)

    def test_update_with_always_true_where(self):
        sql_always_true_update = "UPDATE table1 SET column1 = 'new_value' WHERE 1 = 1"
        parsed_stmt = parser.parse(sql_always_true_update)
        result = self.rule.match(parsed_stmt)
        self.assertTrue(result, "Should detect UPDATE with always-true WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.CRITICAL)

    def test_delete_with_always_true_where(self):
        sql_always_true_delete = "DELETE FROM table1 WHERE 1 = 1"
        parsed_stmt = parser.parse(sql_always_true_delete)
        result = self.rule.match(parsed_stmt)
        self.assertTrue(result, "Should detect DELETE with always-true WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.CRITICAL)

    def test_valid_update_with_where(self):
        sql_valid_update = "UPDATE table1 SET column1 = 'new_value' WHERE id = 1"
        parsed_stmt = parser.parse(sql_valid_update)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not detect a valid UPDATE with WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_valid_delete_with_where(self):
        sql_valid_delete = "DELETE FROM table1 WHERE id = 1"
        parsed_stmt = parser.parse(sql_valid_delete)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not detect a valid DELETE with WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_update_with_nested_subquery(self):
        # 更新语句中使用了嵌套子查询，但依然有有效的WHERE条件
        sql_nested_update = """
        UPDATE table1
        SET column = (SELECT MAX(sub_col) FROM table2 WHERE table2.id = table1.id)
        WHERE EXISTS(SELECT 1 FROM table3 WHERE table3.table1_id = table1.id)
        """
        parsed_stmt = parser.parse(sql_nested_update)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not flag an UPDATE with a nested subquery and a valid WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_delete_with_function_in_where(self):
        # 删除语句中WHERE子句使用了函数，但不是恒真条件
        sql_function_delete = "DELETE FROM table1 WHERE DATE(column) = CURDATE()"
        parsed_stmt = parser.parse(sql_function_delete)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not flag a DELETE with a function in WHERE clause that's not always true.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_complex_delete_with_multi_level_joins(self):
        # 复杂的多层JOIN删除，但有合理的WHERE条件限制
        sql_complex_delete = """
        DELETE t1
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.t1_id
        JOIN table3 t3 ON t2.id = t3.t2_id
        WHERE t3.status = 'active'
        """
        parsed_stmt = parser.parse(sql_complex_delete)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not flag a DELETE with multi-level JOINs and a specific WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_update_with_case_expression(self):
        # UPDATE语句使用CASE表达式设置列值，同时有WHERE条件
        sql_case_update = """
        UPDATE table1
        SET column = CASE WHEN column2 = 'value' THEN 'new_val' ELSE column END
        WHERE column3 IS NOT NULL
        """
        parsed_stmt = parser.parse(sql_case_update)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not flag an UPDATE using CASE expression and a WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_delete_with_false_condition(self):
        # DELETE语句有一个永远为假的WHERE条件
        sql_false_delete = "DELETE FROM table1 WHERE 1 = 0"
        parsed_stmt = parser.parse(sql_false_delete)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should flag a DELETE with a never-true WHERE clause.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_update_with_multiple_conditions(self):
        # UPDATE语句带有多个AND/OR连接的条件
        sql_multiple_conditions_update = """
        UPDATE table1
        SET column = 'new_value'
        WHERE column1 = 'value1' AND column2 = 'value2' OR column3 IN ('value3', 'value4')
        """
        parsed_stmt = parser.parse(sql_multiple_conditions_update)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not flag an UPDATE with multiple, combined WHERE conditions.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)


if __name__ == '__main__':
    unittest.main()
