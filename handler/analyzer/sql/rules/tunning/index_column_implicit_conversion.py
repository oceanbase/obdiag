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
@time: 2024/5/23
@file: index_column_implicit_conversion.py
@desc:
"""
from handler.analyzer.sql.rules.level import Level
from sqlgpt_parser.parser.tree.expression import QualifiedNameReference
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from handler.analyzer.sql.rules.abstract_rule import AbstractRule
from handler.analyzer.sql.rules.result import Result
from sqlgpt_parser.parser.tree.statement import Statement


class IndexColumnImplicitConversionRule(AbstractRule):
    rule_name = "index_column_implicit_conversion_rule"
    rule_description = """
        Detect potential implicit type conversions on indexed columns in query conditions 
        due to comparison or arithmetic operations with different types, which may degrade index efficiency.
        """

    def match(self, root: Statement, catalog=None) -> bool:
        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False
                self.conversion_warnings = []

            def visit_comparison_expression(self, node, context):
                if catalog:
                    left_is_col = isinstance(node.left, QualifiedNameReference) and catalog.is_column_indexed(str(node.left.name))
                    right_is_col = isinstance(node.right, QualifiedNameReference) and catalog.is_column_indexed(str(node.right.name))

                    if left_is_col or right_is_col:
                        # Check for type mismatch that could lead to implicit conversion
                        if left_is_col and not isinstance(node.right, type(node.left)) or right_is_col and not isinstance(node.left, type(node.right)):
                            col_name = left_is_col and str(node.left.name) or str(node.right.name)
                            self.conversion_warnings.append(f"Implicit type conversion warning on indexed column '{col_name}'.")
                            self.match = True

                return self.match

            def visit_arithmetic_binary_expression(self, node, context):
                if catalog:
                    for expr in [node.left, node.right]:
                        if isinstance(expr, QualifiedNameReference) and catalog.is_column_indexed(str(expr.name)):
                            # If the other side is not the same type, it might suggest an implicit conversion
                            if not isinstance(node.left, type(expr)) or not isinstance(node.right, type(expr)):
                                col_name = str(expr.name)
                                self.conversion_warnings.append(f"Implicit type conversion warning on indexed column '{col_name}' due to arithmetic operation.")
                                self.match = True

                return self.match

        visitor = Visitor()
        visitor.process(root, catalog)

        if visitor.match:
            self._conversion_warnings_details = visitor.conversion_warnings
        return visitor.match

    def suggestion(self, root: Statement, catalog=None):
        if hasattr(self, '_conversion_warnings_details') and self._conversion_warnings_details:
            issue_list = "\n".join(self._conversion_warnings_details)
            detailed_suggestion = f"The following indexed columns may be involved in implicit type conversions due to comparison or arithmetic operations:\n{issue_list}\nReview these to ensure optimal index usage."
            return Result(self.rule_name, Level.WARN, detailed_suggestion, self.rule_description)
        else:
            return Result(self.rule_name, Level.OK, "No implicit type conversion warnings found for indexed columns.", self.rule_description)
