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
@file: update_delete_without_where_or_true_condition.py
@desc:
"""

from sqlgpt_parser.parser.tree.statement import Statement
from sqlgpt_parser.parser.tree.expression import ComparisonExpression
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from handler.analyzer.sql.rules.level import Level
from handler.analyzer.sql.rules.abstract_rule import AbstractRule
from handler.analyzer.sql.rules.result import Result


class UpdateDeleteWithoutWhereOrTrueConditionRule(AbstractRule):
    rule_name = "update_delete_without_where_or_true_condition_rule"
    rule_description = """
        UPDATE or DELETE statements should not be executed without a WHERE clause or with a always-true WHERE condition.
    """

    def match(self, root: Statement, catalog=None) -> bool:
        """
        :param root:
        :param catalog:
        :return:
        """

        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False
                self.visited_where = False

            def visit_update(self, node, context):
                self.check_for_where(node.where)

            def visit_delete(self, node, context):
                self.check_for_where(node.where)

            def check_for_where(self, where):
                if where is None:
                    # No WHERE clause found
                    self.match = True
                elif isinstance(where, ComparisonExpression) and where.left == where.right:
                    # WHERE clause exists but is always true
                    self.match = True
                else:
                    # Valid WHERE clause found
                    self.visited_where = True

        visitor = Visitor()
        visitor.process(root, None)

        # Only consider it a match if there was no valid WHERE clause encountered
        return visitor.match and not visitor.visited_where

    def suggestion(self, root: Statement, catalog=None) -> Result:
        if self.match(root, catalog):
            suggestion_text = (
                "Executing UPDATE or DELETE statements without a WHERE clause or with an always-true WHERE condition "
                "can be extremely dangerous, potentially affecting all rows in the table. Please ensure a proper and "
                "specific WHERE condition is used to limit the scope of the operation."
            )
            return Result(self.rule_name, Level.CRITICAL, suggestion_text, self.rule_description)
        else:
            return Result(self.rule_name, Level.OK, "UPDATE or DELETE operations include a WHERE clause with a specific condition, adhering to best practices.", self.rule_description)
