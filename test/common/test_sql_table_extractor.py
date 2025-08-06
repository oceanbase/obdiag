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
@file: test_sql_table_extractor.py
@desc: 
"""

import unittest
from src.common.tool import SQLTableExtractor


class TestSQLTableExtractor(unittest.TestCase):

    def setUp(self):
        self.parser = SQLTableExtractor()

    def test_select_with_db(self):
        result = self.parser.parse("SELECT * FROM db1.table1 WHERE id > 10;")
        self.assertEqual(result, [('db1', 'table1')])

    def test_insert_with_db(self):
        result = self.parser.parse("INSERT INTO db2.table2 (id, name) VALUES (1, 'test')")
        self.assertEqual(result, [('db2', 'table2')])

    def test_update_with_db(self):
        result = self.parser.parse("UPDATE db3.table3 SET status='active' WHERE id=1;")
        self.assertEqual(result, [('db3', 'table3')])

    def test_delete_with_db(self):
        result = self.parser.parse("DELETE FROM db4.table4 WHERE created_at < '2025-01-01';")
        self.assertEqual(result, [('db4', 'table4')])

    def test_no_db_name(self):
        result = self.parser.parse("SELECT * FROM table_no_db;")
        self.assertEqual(result, [(None, 'table_no_db')])

    def test_complex_query_with_aliases(self):
        sql = "SELECT t1.id, t2.name FROM db5.table1 AS t1 JOIN db5.table2 AS t2 ON t1.id = t2.user_id;"
        result = self.parser.parse(sql)
        self.assertEqual(result, [('db5', 'table1'), ('db5', 'table2')])

    def test_unsupported_statement(self):
        result = self.parser.parse("DROP TABLE IF EXISTS db12.table11;")
        self.assertEqual(result, [])

    def test_subquery(self):
        sql = """
        SELECT * FROM db6.main_table 
        WHERE id IN (SELECT user_id FROM db6.sub_table WHERE status = 'active');
        """
        result = self.parser.parse(sql)
        self.assertEqual(result, [('db6', 'main_table'), ('db6', 'sub_table')])

    def test_multiple_joins(self):
        sql = """
        SELECT a.name, b.title, c.content 
        FROM db7.articles a 
        JOIN db7.authors b ON a.author_id = b.id 
        LEFT JOIN db7.comments c ON a.id = c.article_id;
        """
        result = self.parser.parse(sql)
        self.assertEqual(result, [('db7', 'articles'), ('db7', 'authors'), ('db7', 'comments')])

    def test_union_query(self):
        sql = """
        SELECT id, name FROM db8.table1 
        UNION 
        SELECT id, name FROM db8.table2;
        """
        result = self.parser.parse(sql)
        self.assertEqual(result, [('db8', 'table1'), ('db8', 'table2')])

    def test_mixed_case_sql(self):
        sql = "SeLeCt * FrOm Db10.TaBlE1 wHeRe Id > 10;"
        result = self.parser.parse(sql)
        self.assertEqual(result, [('Db10', 'TaBlE1')])

    def test_multiple_statements(self):
        sql = """
        SELECT * FROM db11.table1;
        INSERT INTO db11.table2 VALUES (1, 'test');
        UPDATE db11.table3 SET status = 'done';
        """
        result = self.parser.parse(sql)
        self.assertEqual(result, [('db11', 'table1'), ('db11', 'table2'), ('db11', 'table3')])

    def test_complex_nested_subquery(self):
        sql = """
        SELECT * FROM db12.outer_table 
        WHERE id IN (
            SELECT user_id FROM db12.middle_table 
            WHERE group_id IN (
                SELECT id FROM db12.inner_table WHERE status = 'active'
            )
        );
        """
        result = self.parser.parse(sql)
        self.assertEqual(result, [('db12', 'outer_table'), ('db12', 'middle_table'), ('db12', 'inner_table')])

    def test_cross_join(self):
        sql = "SELECT * FROM db13.table1 CROSS JOIN db13.table2;"
        result = self.parser.parse(sql)
        self.assertEqual(result, [('db13', 'table1'), ('db13', 'table2')])

    def test_full_outer_join(self):
        sql = """
        SELECT * FROM db14.left_table 
        FULL OUTER JOIN db14.right_table 
        ON left_table.id = right_table.id;
        """
        result = self.parser.parse(sql)
        self.assertEqual(result, [('db14', 'left_table'), ('db14', 'right_table')])

    def test_empty_query(self):
        result = self.parser.parse("")
        self.assertEqual(result, [])

    def test_invalid_sql(self):
        result = self.parser.parse("SELECT * FROM;")
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()
