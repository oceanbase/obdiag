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
@file: level.py
@desc:
"""

from enum import Enum, unique


@unique
class Level(Enum):
    OK = (1, 'ok')
    NOTICE = (2, 'notice')
    WARN = (3, 'warn')
    CRITICAL = (4, 'critical')

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value[0] < other.value[0]
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value[0] <= other.value[0]
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value[0] > other.value[0]
        return NotImplemented

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value[0] >= other.value[0]
        return NotImplemented

    @classmethod
    def from_string(cls, s):
        for member in cls:
            if member.value[1] == s:
                return member
        raise ValueError(f"No such level: {s}")

    @property
    def string(self):
        return self.value[1]
