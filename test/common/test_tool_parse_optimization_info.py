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
@time: 2025/3/25
@file: test_tool_parse_optimization_info.py
@desc: 
"""

import unittest
from src.common.tool import StringUtils
from unittest.mock import Mock


class TestToolParseOptimizationInfo(unittest.TestCase):
    def setUp(self):
        self.stdio = Mock()

    def test_stats_version_abnormal(self):
        text = """|                                                                                   Optimization Info:                                                                                   |
|                                                                         -------------------------------------                                                                          |
|                                                                                          game:                                                                                         |
|                                                                                         table_rows:9                                                                                   |
|                                                                                    physical_range_rows:9                                                                               |
|                                                                                     logical_range_rows:9                                                                               |
|                                                                                      index_back_rows:0                                                                                 |
|                                                                                        output_rows:9                                                                                   |
|                                                                                         table_dop:3                                                                                    |
|                                                                                    dop_method:Global DOP                                                                               |
|                                                                                  avaiable_index_name:[game]                                                                            |
|                                                                                stats version:1741874403197619                                                                          |
|                                                                                   dynamic sampling level:0                                                                             |
|                                                                           estimation method:[OPTIMIZER STATISTICS]                                                                     |
|                                                                                        Plan Type:                                                                                      |
|                                                                                         DISTRIBUTED                                                                                    |
|                                                                                          Note:                                                                                         |
|                                                                          Degree of Parallelism is 3 because of hint                                                                                           |
"""
        result = StringUtils.parse_optimization_info(text=text, stdio=self.stdio)
        self.assertEqual(result, 'In [Optimization Info], the [stats version] time for the game table is 2025-03-13 14:00:03, indicating that statistics are over 24 hours old. Please collect statistics.')

    def test_stats_info_abnormal(self):
        text = """| Optimization Info:                                                                                                                                   |
| -------------------------------------                                                                                                                |
|   chatDetail31:                                                                                                                                  |
|       table_rows:51951519                                                                                                                           |
|       physical_range_rows:903003                                                                                                                    |
|       logical_range_rows:903003                                                                                                                     |
|       index_back_rows:903003                                                                                                                        |
|       output_rows:894039                                                                                                                            |
|       table_dop:5                                                                                                                                   |
|       dop_method:Auto DOP                                                                                                                           |
|       avaiable_index_name:[idx_app_source_uid_create_time, idx_app_uid_source, idx_chat_id, idx_session_id, idx_uid_question_id, tblChatDetail31]   |
|       pruned_index_name:[idx_chat_id, idx_session_id, idx_uid_question_id]                                                                         |
|       stats info:[version=2025-03-18 16:40:17.716974, is_locked=0, is_expired=0]                                                                   |
|       dynamic sampling level:0                                                                                                                      |
|       estimation method:[OPTIMIZER STATISTICS, STORAGE]                                                                                            |
"""
        result = StringUtils.parse_optimization_info(text=text, stdio=self.stdio)
        self.assertEqual(result, 'In [Optimization Info], the [stats version] time for the chatDetail31 table is 2025-03-18 16:40:17, indicating that statistics are over 24 hours old. Please collect statistics.')

    def test_stats_version_unmatch(self):
        text = """|                                                                                   Optimization Info:                                                                                   |
|                                                                         -------------------------------------                                                                          |
|                                                                                          game:                                                                                         |
|                                                                                         table_rows:9                                                                                   |
|                                                                                    physical_range_rows:9                                                                               |
|                                                                                     logical_range_rows:9                                                                               |
|                                                                                      index_back_rows:0                                                                                 |
|                                                                                        output_rows:9                                                                                   |
|                                                                                         table_dop:3                                                                                    |
|                                                                                    dop_method:Global DOP                                                                               |
|                                                                                  avaiable_index_name:[game]                                                                            |
|                                                                                stats version_unmatch:1741874403197619                                                                          |
|                                                                                   dynamic sampling level:0                                                                             |
|                                                                           estimation method:[OPTIMIZER STATISTICS]                                                                     |
|                                                                                        Plan Type:                                                                                      |
|                                                                                         DISTRIBUTED                                                                                    |
|                                                                                          Note:                                                                                         |
|                                                                          Degree of Parallelism is 3 because of hint                                                                                           |
"""
        result = StringUtils.parse_optimization_info(text=text, stdio=self.stdio)
        self.assertEqual(result, '')

    def test_stats_info_unmatch(self):
        text = """| Optimization Info:                                                                                                                                   |
| -------------------------------------                                                                                                                |
|   chatDetail31:                                                                                                                                  |
|       table_rows:51951519                                                                                                                           |
|       physical_range_rows:903003                                                                                                                    |
|       logical_range_rows:903003                                                                                                                     |
|       index_back_rows:903003                                                                                                                        |
|       output_rows:894039                                                                                                                            |
|       table_dop:5                                                                                                                                   |
|       dop_method:Auto DOP                                                                                                                           |
|       avaiable_index_name:[idx_app_source_uid_create_time, idx_app_uid_source, idx_chat_id, idx_session_id, idx_uid_question_id, tblChatDetail31]   |
|       pruned_index_name:[idx_chat_id, idx_session_id, idx_uid_question_id]                                                                         |
|       stats info:[test_version=2025-03-18 16:40:17.716974, is_locked=0, is_expired=0]                                                                   |
|       dynamic sampling level:0                                                                                                                      |
|       estimation method:[OPTIMIZER STATISTICS, STORAGE]                                                                                            |
"""
        result = StringUtils.parse_optimization_info(text=text, stdio=self.stdio)
        self.assertEqual(result, '')


if __name__ == '__main__':
    unittest.main()
