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
@time: 2024/5/20
@file: select_all.py
@desc:
"""
from handler.analyzer.sql.rules.level import Level
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from handler.analyzer.sql.rules.result import Result
from handler.analyzer.sql.rules.abstract_rule import AbstractRule
from sqlgpt_parser.parser.tree.literal import StringLiteral
from sqlgpt_parser.parser.tree.statement import Statement


class FullScanRule(AbstractRule):
    rule_name = "full_scan_rule"
    rule_description = """
    Online query full table scan is not recommended. 
    Exceptions are: 
    1. very small table
    2. very low frequency
    3. the table/result set returned is very small (within 100 records / 100 KB).
    """

    def match(self, root: Statement, catalog=None) -> bool:
        """
        match:
        select 1 from a
        select 1 from a where b != / <>
        select 1 from a where b not like
        select 1 from a where b not in
        select 1 from a where not exists
        select 1 from a where b like %a / %a%

        not match:
        select * from a left join b on (a.id = b.id) and a.c=1

        :param root:
        :param catalog:
        :return:
        """

        # Remove clauses such as exists / != / <> / not in / not like / like %a
        class Remove_Visitor(DefaultTraversalVisitor):
            def visit_comparison_expression(self, node, context):
                type = node.type
                if type in ('!=', '<>'):
                    node.left = None
                    node.right = None
                    node.type = None
                else:
                    self.process(node.left, context)
                    self.process(node.right, context)
                return None

            def visis_in_predicate(self, node, context):
                if node.is_not:
                    node.is_not = None
                    node.value = None
                    node.value_list = None
                return None

            def visit_like_predicate(self, node, context):
                process_flag = True

                pattern = node.pattern

                if isinstance(pattern, StringLiteral):
                    value = pattern.value
                    if value.startswith('%') or node.is_not:
                        process_flag = False
                        node.pattern = None
                        node.value = None
                        node.escape = None

                if process_flag:
                    self.process(node.value, context)
                    self.process(node.pattern, context)
                    if node.escape is not None:
                        self.process(node.escape, context)
                return None

        # Determine whether there is a expression that can extract query range, if there is, it is not a full table scan
        class Query_Range_Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = True

            def visit_comparison_expression(self, node, context):
                type = node.type
                if type and type in ('=', '>', '<', '>=', '<='):
                    self.match = False
                if node.left:
                    self.process(node.left, context)
                if node.right:
                    self.process(node.right, context)
                return None

            def visit_in_predicate(self, node, context):
                if node.is_not:
                    # Even though it's a NOT IN, it still suggests a non-full scan attempt
                    self.match = False
                else:
                    self.process(node.value, context)
                    self.process(node.value_list, context)

            def visit_like_predicate(self, node, context):
                if node.pattern and node.value:
                    pattern = node.pattern
                    if isinstance(pattern, StringLiteral):
                        value = pattern.value
                        if value.endswith('%'):
                            self.match = False

                if node.value:
                    self.process(node.value, context)
                if node.pattern:
                    self.process(node.pattern, context)
                if node.escape:
                    self.process(node.escape, context)
                return None

            # Add handling for NOT EXISTS
            def visit_exists_predicate(self, node, context):
                if node.is_not:
                    # NOT EXISTS can also imply a specific range consideration
                    self.match = False
                else:
                    self.process(node.subquery, context)

            def visit_between_predicate(self, node, context):
                if not node.is_not:
                    self.match = False

                self.process(node.value, context)
                self.process(node.min, context)
                self.process(node.max, context)

                return None

            def visit_not_expression(self, node, context):
                node.value = None
                self.match = True
                return None

        try:
            remove_visitor = Remove_Visitor()
            remove_visitor.process(root, None)

            query_range_visitor = Query_Range_Visitor()
            query_range_visitor.process(root, None)
            return query_range_visitor.match
        except Exception as e:
            pass

        return False

    def suggestion(self, root: Statement, catalog=None) -> Result:
        if self.match(root, catalog):
            suggestion_text = "Detected a potential full table scan which may impact performance. " "Consider adding indexes, refining WHERE clauses, or restructuring the query to utilize existing indexes."
            return Result(self.rule_name, Level.WARN, suggestion_text, self.rule_description)
        else:
            suggestion_text = "The query does not involve a full table scan. It appears to be well-optimized for the given conditions."
            return Result(self.rule_name, Level.OK, suggestion_text, self.rule_description)
