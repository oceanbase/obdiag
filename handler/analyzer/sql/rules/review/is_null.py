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
@file: is_null.py
@desc:
"""

from sqlgpt_parser.parser.tree.literal import NullLiteral
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from handler.analyzer.sql.rules.level import Level
from sqlgpt_parser.parser.tree.statement import Statement
from handler.analyzer.sql.rules.abstract_rule import AbstractRule
from handler.analyzer.sql.rules.result import Result


class IsNullRule(AbstractRule):
    rule_name = "is_null_rule"
    rule_description = """
        Use IS NULL to determine whether it is a NULL value
        A direct comparison of NULL to any value is NULL.
         1) The return result of NULL<>NULL is NULL, not false.
         2) The return result of NULL=NULL is NULL, not true.
         3) The return result of NULL<>1 is NULL, not true.
        """

    def match(self, root: Statement, catalog=None) -> bool:
        """
        NULL<>、<>NULL、=NULL、NULL=
        :param root:
        :param catalog:
        :return:
        """

        # NULL<>、<>NULL、=NULL、NULL=、!=NULL、 NULL!=
        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False

            def visit_comparison_expression(self, node, context):
                if isinstance(node.left, NullLiteral):
                    self.match = True
                if isinstance(node.right, NullLiteral):
                    self.match = True
                return None

        visitor = Visitor()
        visitor.process(root, None)

        return visitor.match

    def suggestion(self, root: Statement, catalog=None):
        if self.match(root, catalog):
            # 如果发现不正确的NULL比较，提供具体的修改建议
            suggestion_text = "Detected comparison with NULL using =, !=, or <>. " "Use 'IS NULL' or 'IS NOT NULL' for correct NULL checks."
            return Result(self.rule_name, Level.WARN, suggestion_text, self.rule_description)
        else:
            # 如果没有发现不正确比较，返回OK状态
            return Result(self.rule_name, Level.OK, "No improper NULL comparisons found.", self.rule_description)
