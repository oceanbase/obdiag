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
from obdiag.handler.analyzer.sql.rules.review.update_delete_multi_table import UpdateDeleteMultiTableRule
from sqlgpt_parser.parser.oceanbase_parser import parser
from obdiag.handler.analyzer.sql.rules.level import Level


class TestUpdateDeleteMultiTableRule(unittest.TestCase):

    def setUp(self):
        self.rule = UpdateDeleteMultiTableRule()

    def test_update_multi_table_detected(self):
        # 假设这个SQL包含了多表更新
        sql_with_multi_table_update = """
        UPDATE table1
        INNER JOIN table2 ON table1.id = table2.table1_id
        SET table1.column = 'new_value'
        """
        parsed_stmt = parser.parse(sql_with_multi_table_update)
        result = self.rule.match(parsed_stmt)
        self.assertTrue(result, "Should detect multi-table UPDATE operation.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.WARN)

    def test_delete_multi_table_detected(self):
        # 假设这个SQL包含了多表删除
        sql_with_multi_table_delete = """
        DELETE table1
        FROM table1
        INNER JOIN table2 ON table1.id = table2.table1_id
        """
        parsed_stmt = parser.parse(sql_with_multi_table_delete)
        result = self.rule.match(parsed_stmt)
        self.assertTrue(result, "Should detect multi-table DELETE operation.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.WARN)

    def test_delete_with_subquery_and_join(self):
        """测试包含子查询和联接的多表删除"""
        complex_delete_sql = """
        DELETE table1
        FROM table1
        INNER JOIN (
            SELECT table1_id
            FROM table2
            WHERE some_column = 'some_value'
            GROUP BY table1_id
            HAVING COUNT(*) > 1
        ) subquery ON table1.id = subquery.table1_id
        """
        parsed_stmt = parser.parse(complex_delete_sql)
        self.assertTrue(self.rule.match(parsed_stmt), "Should detect complex multi-table DELETE operation.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.WARN)

    def test_single_table_operation(self):
        # 单表更新操作，应不触发警告
        sql_single_table_update = "UPDATE table1 SET column = 'value' WHERE id = 1"
        parsed_stmt = parser.parse(sql_single_table_update)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not detect single-table UPDATE operation as an issue.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

        # 单表删除操作，同样不应触发警告
        sql_single_table_delete = "DELETE FROM table1 WHERE id = 1"
        parsed_stmt = parser.parse(sql_single_table_delete)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not detect single-table DELETE operation as an issue.")
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)


if __name__ == '__main__':
    unittest.main()
