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
@time: 2024/09/19
@file: all_sql.py
@desc:
"""
import re


class all_sql:
    def __init__(self):
        pass

    def redact(self, text):
        patterns = [
            (r'stmt:"(.*?[^\\])", stmt_len', 'stmt:"<SQL_QUERY_REDACTED>", stmt_len'),
            (r'ps_sql:"(.*?[^\\])", is_expired_evicted', 'ps_sql:"<SQL_QUERY_REDACTED>", is_expired_evicted'),
            (r'ps_sql:"(.*?[^\\])", ref_count:', 'ps_sql:"<SQL_QUERY_REDACTED>", ref_count:'),
            (r'origin_sql=(.*?[^\\]), ps_stmt_checksum', 'origin_sql=<SQL_QUERY_REDACTED>, ps_stmt_checksum'),
            (r'get_sql_stmt\(\)=(.*?[^\\]), route_sql_=', 'get_sql_stmt()=<SQL_QUERY_REDACTED>, route_sql_='),
            (r'multi_stmt_item={(.*?[^\\])\}', 'multi_stmt_item={<SQL_QUERY_REDACTED>}'),
        ]
        log_content = text
        # 遍历所有模式并进行替换
        for pattern, replacement in patterns:
            log_content = re.sub(pattern, replacement, text, flags=re.DOTALL)
        return log_content


all_sql = all_sql()
