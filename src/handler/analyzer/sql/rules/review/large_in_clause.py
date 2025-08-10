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
@time: 2024/5/24
@file: large_in_clause_adjusted.py
@desc:
"""
from src.handler.analyzer.sql.rules.level import Level
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from src.handler.analyzer.sql.rules.abstract_rule import AbstractRule
from src.handler.analyzer.sql.rules.result import Result
from sqlgpt_parser.parser.tree.statement import Statement


class LargeInClauseAdjustedRule(AbstractRule):
    rule_name = "large_in_clause_rule_adjusted"
    rule_description = """
        Avoid using IN clauses with more than 200 elements as it may lead to performance issues.
        """

    MAX_IN_ELEMENTS = 200

    def match(self, root: Statement, catalog=None) -> bool:
        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False

            def visit_in_predicate(self, node, context):
                # Assuming node.values holds the list of values directly or indirectly; adjust based on actual implementation
                if hasattr(node, 'value_list'):
                    if len(node.value_list.values) > LargeInClauseAdjustedRule.MAX_IN_ELEMENTS:
                        self.match = True
                        return self.match
                return self.match

        try:
            visitor = Visitor()
            visitor.process(root, None)
        except Exception as e:
            pass

        return visitor.match

    def suggestion(self, root: Statement, catalog=None):
        if self.match(root, catalog):
            return Result(
                self.rule_name,
                Level.WARN,
                f"The IN clause contains more than {LargeInClauseAdjustedRule.MAX_IN_ELEMENTS} elements, which may degrade query performance. " "Consider alternative strategies like breaking the query into smaller chunks or using EXISTS/JOIN clauses.",
                self.rule_description,
            )
        else:
            return Result(
                self.rule_name,
                Level.OK,
                "The IN clause does not exceed the recommended number of elements.",
                self.rule_description,
            )
