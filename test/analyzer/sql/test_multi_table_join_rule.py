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
@file: test_multi_table_join_rule.py
@desc:
"""
import unittest
from handler.analyzer.sql.rules.review.multi_table_join import MultiTableJoinRule
from sqlgpt_parser.parser.oceanbase_parser import parser
from handler.analyzer.sql.rules.level import Level


class TestMultiTableJoinRule(unittest.TestCase):

    def setUp(self):
        self.rule = MultiTableJoinRule()

    def test_excessive_joins_detected(self):
        # 假设这个SQL有超过5个JOIN
        sql_with_excessive_joins = """
        SELECT * 
        FROM table1
        JOIN table2 ON table1.id = table2.table1_id
        JOIN table3 ON table2.id = table3.table2_id
        JOIN table4 ON table3.id = table4.table3_id
        JOIN table5 ON table4.id = table5.table4_id
        JOIN table6 ON table5.id = table6.table5_id
        JOIN table7 ON table6.id = table7.table6_id
        """
        parsed_stmt = parser.parse(sql_with_excessive_joins)
        result = self.rule.match(parsed_stmt)
        self.assertTrue(result, "Should detect excessive joins in SQL statement.")

    def test_no_excessive_joins(self):
        # 正常SQL，少于等于5个JOIN
        sql_no_excessive_joins = """
        SELECT * 
        FROM table1
        JOIN table2 ON table1.id = table2.table1_id
        JOIN table3 ON table2.id = table3.table2_id
        """
        parsed_stmt = parser.parse(sql_no_excessive_joins)
        result = self.rule.match(parsed_stmt)
        self.assertFalse(result, "Should not detect excessive joins in SQL statement.")

    def test_complex_query_with_subqueries_no_excessive_joins(self):
        # Complex query with subqueries but not exceeding join limit (e.g., 7 tables but only 4 joins)
        sql_complex = """
        SELECT t1.*, t2.col
        FROM table1 t1
        JOIN (
            SELECT t2.id, t3.col
            FROM table2 t2
            JOIN table3 t3 ON t2.id = t3.table2_id
            WHERE t3.col IN (SELECT col FROM table4 WHERE condition)
        ) t2 ON t1.id = t2.id
        JOIN table5 t5 ON t1.id = t5.table1_id
        JOIN table6 t6 ON t5.id = t6.table5_id;
        """
        parsed_stmt = parser.parse(sql_complex)
        self.assertFalse(self.rule.match(parsed_stmt))  # Assuming subqueries don't increment join count
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.OK)

    def test_complex_query_with_excessive_joins_and_subqueries(self):
        # Complex query exceeding join limit due to multiple explicit joins and possibly join in subqueries
        sql_complex_excessive = """
        SELECT t1.*, t2.col
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.table1_id
        JOIN table3 t3 ON t2.id = t3.table2_id
        JOIN table4 t4 ON t3.id = t4.table3_id
        JOIN table5 t5 ON t4.id = t5.table4_id
        JOIN (
            SELECT t6.id, t7.col
            FROM table6 t6
            JOIN table7 t7 ON t6.id = t7.table6_id
        ) subquery ON t5.id = subquery.id;
        """
        parsed_stmt = parser.parse(sql_complex_excessive)
        self.assertTrue(self.rule.match(parsed_stmt))
        suggestion = self.rule.suggestion(parsed_stmt)
        self.assertEqual(suggestion.level, Level.WARN)


if __name__ == '__main__':
    unittest.main()
