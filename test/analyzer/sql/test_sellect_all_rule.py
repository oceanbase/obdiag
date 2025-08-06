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
@file: test_sellect_all_rule.py
@desc:
"""
import unittest
from sqlgpt_parser.parser.oceanbase_parser import parser
from src.handler.analyzer.sql.rules.review.select_all import SelectAllRule


class TestSelectAllCase(unittest.TestCase):
    def test_select_all_rule_true(self):
        statement = parser.parse("SELECT * FROM T1")
        result_match = SelectAllRule().match(statement)
        self.assertTrue(result_match)
        result_suggestion = SelectAllRule().suggestion(statement)
        print(result_suggestion)
        # self.assertIsNotNone(result_suggestion)

    def test_select_all_rule_false(self):
        statement = parser.parse("SELECT 1 FROM T1")
        result_match = SelectAllRule().match(statement)
        self.assertFalse(result_match)
        result_suggestion = SelectAllRule().suggestion(statement)
        self.assertIsNotNone(result_suggestion)

    def test_select_all_rule_false_1(self):
        statement = parser.parse("SELECT count(*) FROM T1")
        result_match = SelectAllRule().match(statement)
        self.assertFalse(result_match)
        result_suggestion = SelectAllRule().suggestion(statement)
        self.assertIsNotNone(result_suggestion)

    def test_select_all_rule_true_1(self):
        sql = '''
            SELECT *
            FROM Employees e
            JOIN Departments d ON e.DepartmentID = d.DepartmentID
            LEFT JOIN (
                SELECT EmployeeID, ProjectID, COUNT(*) AS NumberOfProjects
                FROM Projects_Employees_Pivot
                GROUP BY EmployeeID, ProjectID
            ) pe ON e.EmployeeID = pe.EmployeeID
            WHERE d.DepartmentName = 'Sales'
            ORDER BY e.EmployeeName
        '''
        statement = parser.parse(sql)
        result_match = SelectAllRule().match(statement)
        self.assertTrue(result_match)
        result_suggestion = SelectAllRule().suggestion(statement)
        self.assertIsNotNone(result_suggestion)


if __name__ == '__main__':
    unittest.main()
