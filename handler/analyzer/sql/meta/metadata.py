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
@time: 2024/5/21
@file: metadata.py
@desc:
"""

from typing import List
from enum import Enum, unique


@unique
class IndexType(Enum):
    PRIMARY = '1.primary'
    UNIQUE = '2.unique'
    NORMAL = '3.normal'


class MetaData(object):
    def __init__(self, table_list: List, statistics_list: List):
        self.table_list = table_list
        self.statistics_list = statistics_list


class Table(object):
    def __init__(self, database_name, table_name, column_list: List, index_list: List, table_rows):
        self.table_name = table_name
        self.database_name = database_name
        self.column_list = column_list
        self.index_list = index_list
        self.table_rows = table_rows


class Column(object):
    def __init__(self, column_name, column_type, column_nullable):
        self.column_name = column_name
        self.column_type = column_type
        self.column_nullable = column_nullable


class Index(object):
    def __init__(
        self,
        index_name,
        column_list: List,
        index_type: IndexType,
        index_all_match=None,
        index_back=None,
        extract_range=None,
        has_interesting_order=None,
    ):
        self.index_name = index_name
        self.column_list = column_list
        self.column_count = len(column_list)
        self.index_type = index_type
        self.index_all_match = index_all_match
        self.index_back = index_back
        self.extract_range = extract_range
        self.has_interesting_order = has_interesting_order


class Selectivity(object):
    def __init__(self, column_name, min_value, max_value, ndv=None):
        self.column_name = column_name
        self.min_value = min_value
        self.max_value = max_value
        self.ndv = ndv


class Statistics(object):
    def __init__(self, database_name, table_name, selectivity_list: List):
        self.database_name = database_name
        self.table_name = table_name
        self.selectivity_list = selectivity_list
