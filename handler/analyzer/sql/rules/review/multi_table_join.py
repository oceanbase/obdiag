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
@file: multi_table_join.py
@desc:
"""
from handler.analyzer.sql.rules.level import Level
from sqlgpt_parser.parser.tree.join_criteria import JoinOn, JoinUsing
from sqlgpt_parser.parser.tree.statement import Statement
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from handler.analyzer.sql.rules.abstract_rule import AbstractRule
from handler.analyzer.sql.rules.result import Result


class MultiTableJoinRule(AbstractRule):
    rule_name = "multi_table_join_rule"
    rule_description = """
        The number of association tables is not recommended to exceed 5
        """

    def match(self, root: Statement, catalog=None) -> bool:

        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False
                self.join_count = 0

            def visit_join(self, node, context):
                self.join_count = self.join_count + 1

                if self.join_count >= 5:
                    self.match = True
                else:
                    self.process(node.left, context)
                    self.process(node.right, context)

                    if isinstance(node.criteria, JoinOn):
                        self.process(node.criteria.expression, context)
                    elif isinstance(node.criteria, JoinUsing):
                        self.process(node.criteria.columns)

                return None

        visitor = Visitor()
        visitor.process(root, None)

        return visitor.match

    def get_join_count(self, root: Statement) -> int:
        """Helper method to count the number of JOIN operations in the statement."""

        class CountJoinVisitor(DefaultTraversalVisitor):
            def __init__(self):
                self.join_count = 0

            def visit_join(self, node, context):
                self.join_count += 1
                self.process(node.left, context)
                self.process(node.right, context)

                if isinstance(node.criteria, JoinOn):
                    self.process(node.criteria.expression, context)
                elif isinstance(node.criteria, JoinUsing):
                    self.process(node.criteria.columns)

        visitor = CountJoinVisitor()
        visitor.process(root, None)
        return visitor.join_count

    def suggestion(self, root: Statement, catalog=None) -> Result:
        join_count = self.get_join_count(root)
        if join_count > 5:
            # 如果关联表数量超过5，提供具体的改进建议
            suggestion_text = (
                f"The query involves {join_count} tables in JOIN operations, exceeding the recommended limit of 3.\n"
                "Consider the following optimizations:\n"
                "- Break the query into smaller, simpler queries and use application-side processing to combine results.\n"
                "- Review the schema design; denormalization or indexed views might reduce the need for complex joins.\n"
                "- Ensure all joined columns are properly indexed for involved tables.\n"
                "- If applicable, consider using materialized views or caching strategies for frequently accessed subsets of data."
            )
            return Result(self.rule_name, Level.WARN, suggestion_text)
        else:
            # 如果没有超过，说明查询在推荐范围内
            suggestion_text = "The number of joined tables is within the recommended limit. No further action needed."
            return Result(self.rule_name, Level.OK, suggestion_text)
