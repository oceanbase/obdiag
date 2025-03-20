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


if __name__ == '__main__':
    unittest.main()
