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
@time: 2024/6/11
@file: rule_manager.py
@desc:
"""

from typing import Dict, Type, List
from sqlgpt_parser.parser.oceanbase_parser import parser
from handler.analyzer.sql.rules.abstract_rule import AbstractRule
from handler.analyzer.sql.rules.result import Result
from handler.analyzer.sql.rules.review.arithmetic import ArithmeticRule
from handler.analyzer.sql.rules.review.full_scan import FullScanRule
from handler.analyzer.sql.rules.review.is_null import IsNullRule
from handler.analyzer.sql.rules.review.large_in_clause import LargeInClauseAdjustedRule
from handler.analyzer.sql.rules.review.multi_table_join import MultiTableJoinRule
from handler.analyzer.sql.rules.review.select_all import SelectAllRule
from handler.analyzer.sql.rules.review.update_delete_multi_table import UpdateDeleteMultiTableRule
from handler.analyzer.sql.rules.review.update_delete_without_where_or_true_condition import UpdateDeleteWithoutWhereOrTrueConditionRule
from handler.analyzer.sql.rules.level import Level
from common.tool import SQLUtil


class RuleManager(object):
    def __init__(self):
        self._registered_rules: Dict[str, Type[AbstractRule]] = {}

    def register_rule(self, rule_class: Type[AbstractRule]):
        """
        注册一个新的规则类。
        :param rule_class: 规则类的类型。
        """
        self._registered_rules[rule_class.rule_name] = rule_class

    def analyze_sql_statement(self, sql, stdio, level_str='notice') -> List[Result]:
        """
        对SQL语句列表应用所有已注册的规则，并收集结果。
        :param sql_statements: SQL语句的列表。
        :return: 二维列表，每个内部列表包含对应SQL语句的所有规则检查结果。
        """
        try:
            sql = SQLUtil().remove_sql_text_affects_parser(sql)
            sql_statement = parser.parse(sql)
            stdio.verbose("sql [{0}]; sql_statement:[{1}]".format(sql, sql_statement))
        except Exception as e:
            stdio.verbose("parse sql Exception : {0}".format(e))
            return []
        level = Level.from_string(level_str)
        rule_results = []
        for rule_class in self._registered_rules.values():
            rule_instance = rule_class()
            result = rule_instance.match(sql_statement)
            suggestion = rule_instance.suggestion(sql_statement)
            if result:
                if suggestion.level >= level:
                    stdio.verbose("rule_name:{0}, suggestion_level:{1}, suggestion:{2}".format(suggestion.rule_name, suggestion.level, suggestion.suggestion))
                    rule_results.append(suggestion)
            else:
                if level <= Level.OK:
                    suggestion = Result(rule_class.rule_name, Level.OK, "No issues found with this rule.", rule_class.rule_description)
                    rule_results.append(suggestion)
        return rule_results


class SQLReviewRuleManager(object):
    def __init__(self):
        self.manager = RuleManager()
        self.manager.register_rule(SelectAllRule)
        self.manager.register_rule(ArithmeticRule)
        self.manager.register_rule(FullScanRule)
        self.manager.register_rule(IsNullRule)
        self.manager.register_rule(LargeInClauseAdjustedRule)
        self.manager.register_rule(MultiTableJoinRule)
        self.manager.register_rule(UpdateDeleteMultiTableRule)
        self.manager.register_rule(UpdateDeleteWithoutWhereOrTrueConditionRule)
