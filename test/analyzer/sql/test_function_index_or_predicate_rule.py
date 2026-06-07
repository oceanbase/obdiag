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
@time: 2026/6/8
@file: test_function_index_or_predicate_rule.py
@desc:
"""

import unittest

from sqlgpt_parser.parser.oceanbase_parser import parser
from src.handler.analyzer.sql.rule_manager import SQLReviewRuleManager
from src.handler.analyzer.sql.rules.level import Level
from src.handler.analyzer.sql.rules.review.function_index_or_predicate import FunctionIndexOrPredicateRule


class TestFunctionIndexOrPredicateRule(unittest.TestCase):
    def setUp(self):
        self.rule = FunctionIndexOrPredicateRule()

    def test_or_connected_function_predicates_detected(self):
        sql = "SELECT count(*) FROM cross_record WHERE RIGHT(tx_sender, 13) = 'xxx' OR RIGHT(tx_receiver, 13) = 'xxx'"
        parsed_stmt = parser.parse(sql)
        self.assertTrue(self.rule.match(parsed_stmt))

    def test_and_connected_function_predicates_not_detected(self):
        sql = "SELECT count(*) FROM cross_record WHERE RIGHT(tx_sender, 13) = 'xxx' AND RIGHT(tx_receiver, 13) = 'xxx'"
        parsed_stmt = parser.parse(sql)
        self.assertFalse(self.rule.match(parsed_stmt))

    def test_plain_or_predicates_not_detected(self):
        sql = "SELECT * FROM cross_record WHERE tx_sender = 'xxx' OR tx_receiver = 'xxx'"
        parsed_stmt = parser.parse(sql)
        self.assertFalse(self.rule.match(parsed_stmt))

    def test_suggestion_level_for_or_connected_function_predicates(self):
        sql = "SELECT count(*) FROM cross_record WHERE RIGHT(tx_sender, 13) = 'xxx' OR RIGHT(tx_receiver, 13) = 'xxx'"
        parsed_stmt = parser.parse(sql)
        result = self.rule.suggestion(parsed_stmt)
        self.assertEqual(result.level, Level.NOTICE)

    def test_rule_registered_in_sql_review_manager(self):
        registered_rules = SQLReviewRuleManager().manager._registered_rules
        self.assertIn(FunctionIndexOrPredicateRule.rule_name, registered_rules)


if __name__ == '__main__':
    unittest.main()
