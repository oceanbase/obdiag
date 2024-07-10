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
@time: 2024/5/21
@file: index_column_fuzzy_search.py
@desc:
"""
from handler.analyzer.sql.rules.level import Level
from sqlgpt_parser.parser.tree.expression import QualifiedNameReference
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from handler.analyzer.sql.rules.abstract_rule import AbstractRule
from handler.analyzer.sql.rules.result import Result
from sqlgpt_parser.parser.tree.statement import Statement


class IndexColumnFuzzyMatchRule(AbstractRule):
    rule_name = "index_column_fuzzy_match_rule"
    rule_description = """
        Avoid using fuzzy or left fuzzy matches on indexed columns in query conditions 
        as it may lead to performance degradation.
        """

    def match(self, root: Statement, catalog=None) -> bool:
        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False
                self.fuzzy_matches_on_indexed_columns = []

            def visit_like_expression(self, node, context):
                # Assuming we have a mechanism to identify indexed columns, e.g., via `catalog`
                if catalog and isinstance(node.expression, QualifiedNameReference):
                    column_name = str(node.expression.name)
                    if catalog.is_column_indexed(column_name):
                        if node.pattern.startswith('%'):  # Left fuzzy match
                            self.fuzzy_matches_on_indexed_columns.append(column_name)
                            self.match = True
                        elif '%' in node.pattern and not node.pattern.endswith('%'):  # Fuzzy match but not left
                            self.fuzzy_matches_on_indexed_columns.append(column_name)
                            self.match = True
                return self.match

        visitor = Visitor()
        visitor.process(root, catalog)

        if visitor.match:
            self._fuzzy_matched_columns = visitor.fuzzy_matches_on_indexed_columns
        return visitor.match

    def suggestion(self, root: Statement, catalog=None):
        if hasattr(self, '_fuzzy_matched_columns') and self._fuzzy_matched_columns:
            column_list = ", ".join(self._fuzzy_matched_columns)
            detailed_suggestion = f"Avoid using fuzzy or left fuzzy matches on these indexed columns: {column_list}"
            return Result(self.rule_name, Level.WARN, detailed_suggestion, self.rule_description)
        else:
            return Result(self.rule_name, Level.OK, "No issues found with indexed column fuzzy matching.", self.rule_description)
