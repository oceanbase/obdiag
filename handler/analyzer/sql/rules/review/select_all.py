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
@file: full_scan.py
@desc:
"""
from handler.analyzer.sql.rules.level import Level
from sqlgpt_parser.parser.tree.statement import Statement
from sqlgpt_parser.parser.tree.expression import QualifiedNameReference
from sqlgpt_parser.parser.tree.qualified_name import QualifiedName
from sqlgpt_parser.parser.tree.select_item import SingleColumn
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from handler.analyzer.sql.rules.result import Result
from handler.analyzer.sql.rules.abstract_rule import AbstractRule


class SelectAllRule(AbstractRule):
    rule_name = "select_all_rule"
    rule_description = """
        select *
        """

    def match(self, root: Statement, catalog=None) -> bool:
        class Visitor(DefaultTraversalVisitor):
            def __init__(self):
                self.is_select_all = False

            def visit_select(self, node, context):
                for item in node.select_items:
                    if isinstance(item, SingleColumn) and isinstance(item.expression, QualifiedNameReference) and isinstance(item.expression.name, QualifiedName):
                        parts = item.expression.name.parts
                        for part in parts:
                            if part == '*':
                                self.is_select_all = True
                                break

        try:
            visitor = Visitor()
            visitor.process(root, None)
        except Exception as e:
            pass

        return visitor.is_select_all

    def suggestion(self, root: Statement, catalog=None) -> Result:
        if self.match(root, catalog):
            suggestion_text = "Using 'SELECT *' can lead to unnecessary data retrieval and potentially impact query performance. " "Consider specifying only the necessary columns explicitly to optimize your query."
            return Result(self.rule_name, Level.WARN, suggestion_text, self.rule_description)
        else:
            return Result(self.rule_name, Level.OK, "No 'SELECT *' usage detected, query is optimized for column selection.", self.rule_description)
