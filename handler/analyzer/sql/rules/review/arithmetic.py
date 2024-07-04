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
@file: arithmetic.py
@desc:
"""
from handler.analyzer.sql.rules.level import Level
from sqlgpt_parser.parser.tree.expression import QualifiedNameReference
from sqlgpt_parser.parser.tree.statement import Statement
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from handler.analyzer.sql.rules.abstract_rule import AbstractRule
from handler.analyzer.sql.rules.result import Result


class ArithmeticRule(AbstractRule):
    rule_name = "arithmetic_rule"
    rule_description = """
        Field operations are not recommended.
        Example: a + 1 > 2 => a > 2 - 1
        """

    def match(self, root: Statement, catalog=None) -> bool:
        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.match = False

            def visit_arithmetic_binary(self, node, context):
                if isinstance(node.left, QualifiedNameReference) or isinstance(node.right, QualifiedNameReference):
                    self.match = True

        visitor = Visitor()
        visitor.process(root, None)

        return visitor.match

    def suggestion(self, root: Statement, catalog=None):
        suggest_text = 'Consider simplifying your expressions by moving constants out of comparisons.'
        if not self.match(root, catalog):
            return Result(self.rule_name, Level.OK, "No improper field operations detected, query is optimized.", self.rule_description)
        else:
            return Result(self.rule_name, Level.NOTICE, suggest_text, self.rule_description)
