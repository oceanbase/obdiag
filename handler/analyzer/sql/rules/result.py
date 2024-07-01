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
@file: result.py
@desc:
"""
import json


class Result(object):
    def __init__(self, name, level, suggestion):
        self.class_name = name
        self.rule_name = name
        self.level = level
        self.suggestion = suggestion
        self.description = suggestion

    def __str__(self):
        return json.dumps({"class_name": self.rule_name, "rule_name": self.rule_name, "level": self.level.value, "suggestion": self.suggestion, "description": self.description}, indent=5)
