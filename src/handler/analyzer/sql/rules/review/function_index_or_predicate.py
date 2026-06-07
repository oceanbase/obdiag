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
@file: function_index_or_predicate.py
@desc:
"""

from sqlgpt_parser.parser.tree.expression import FunctionCall, LogicalBinaryExpression
from sqlgpt_parser.parser.tree.statement import Statement
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from src.handler.analyzer.sql.rules.abstract_rule import AbstractRule
from src.handler.analyzer.sql.rules.level import Level
from src.handler.analyzer.sql.rules.result import Result


class FunctionIndexOrPredicateRule(AbstractRule):
    rule_name = "function_index_or_predicate_rule"
    rule_description = """
        OceanBase may fail to infer function indexes for OR-connected function predicates.
        """

    def match(self, root: Statement, catalog=None) -> bool:
        class FunctionCallVisitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False

            def visit_function_call(self, node, context):
                self.match = True
                return None

        class OrPredicateVisitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False

            @staticmethod
            def _has_function_call(node):
                if isinstance(node, FunctionCall):
                    return True
                visitor = FunctionCallVisitor()
                visitor.process(node, None)
                return visitor.match

            def visit_logical_binary_expression(self, node, context):
                if isinstance(node, LogicalBinaryExpression) and node.type == 'OR':
                    if self._has_function_call(node.left) or self._has_function_call(node.right):
                        self.match = True
                        return None
                self.process(node.left, context)
                self.process(node.right, context)
                return None

        try:
            visitor = OrPredicateVisitor()
            visitor.process(root, None)
            return visitor.match
        except Exception:
            return False

    def suggestion(self, root: Statement, catalog=None):
        if self.match(root, catalog):
            suggestion_text = (
                "Detected OR-connected function predicates. OceanBase may not infer function indexes for this pattern; " "consider rewriting the OR branches as UNION ALL, or use the generated hidden column that backs the function index when appropriate."
            )
            return Result(self.rule_name, Level.NOTICE, suggestion_text, self.rule_description)
        return Result(self.rule_name, Level.OK, "No OR-connected function predicates detected.", self.rule_description)
