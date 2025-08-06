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
@time: 2024/5/20
@file: update_delete_multi_table.py
@desc:
"""

from sqlgpt_parser.parser.tree.relation import Join
from sqlgpt_parser.parser.tree.statement import Statement
from sqlgpt_parser.parser.tree.visitor import DefaultTraversalVisitor
from src.handler.analyzer.sql.rules.level import Level
from src.handler.analyzer.sql.rules.abstract_rule import AbstractRule
from src.handler.analyzer.sql.rules.result import Result


class UpdateDeleteMultiTableRule(AbstractRule):
    rule_name = "update_delete_multi_table_rule"
    rule_description = """
        UPDATE / DELETE does not recommend using multiple tables
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

            def visit_delete(self, node, context):
                table = node.table
                if table and isinstance(table[0], Join):
                    self.match = True

            def visit_update(self, node, context):
                table = node.table
                if table and isinstance(table[0], Join):
                    self.match = True

        try:
            visitor = Visitor()
            visitor.process(root, None)
        except Exception as e:
            pass

        return visitor.match

    def suggestion(self, root: Statement, catalog=None) -> Result:
        if self.match(root, catalog):
            suggestion_text = (
                "The use of multiple tables in UPDATE or DELETE operation is not recommended. " "Consider breaking down the operation into separate single-table statements or " "using transactions to manage the update/delete across multiple tables safely."
            )
            return Result(self.rule_name, Level.WARN, suggestion_text, self.rule_description)
        else:
            return Result(self.rule_name, Level.OK, "No multi-table UPDATE or DELETE operation detected, following best practices.", self.rule_description)
