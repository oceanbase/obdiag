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
@time: 2024/5/22
@file: abstract_rule.py
@desc:
"""

from abc import ABCMeta, abstractmethod

from sqlgpt_parser.parser.tree.statement import Statement


class AbstractRule(metaclass=ABCMeta):
    def match(self, root: Statement, context=None) -> bool:
        return True

    @abstractmethod
    def suggestion(self, root: Statement, context=None):
        pass
