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
@time: 2024/01/17
@file: sql_utils.py
@desc:
"""

import sqlparse

def extract_db_and_table(sql):
    parsed_sql = sqlparse.parse(sql)

    db_tables_list = []

    for statement in parsed_sql:
        tokens = list(statement.tokens)
        if statement.get_type() == 'SELECT':
            from_index = next((i for i, token in enumerate(tokens) if
                               token.ttype == sqlparse.tokens.Keyword and token.value.lower() == 'from'), -1)
            if from_index != -1:
                after_from_tokens = tokens[from_index + 1:]
                parse_db_table(after_from_tokens, db_tables_list)

        elif statement.get_type() == 'INSERT':
            into_index = next((i for i, token in enumerate(tokens) if
                               token.ttype == sqlparse.tokens.Keyword and token.value.lower() == 'into'), -1)
            if into_index != -1:
                after_into_tokens = tokens[into_index + 1:]
                parse_db_table(after_into_tokens, db_tables_list)

    return db_tables_list


def parse_db_table(tokens, db_tables_list):
    for token in tokens:
        if isinstance(token, sqlparse.sql.IdentifierList):
            for sub_token in token.tokens:
                parts = split_db_table(sub_token.value)
                if len(parts) > 1:
                    db_tables_list.append(parts)
        elif isinstance(token, sqlparse.sql.Identifier):
            parts = split_db_table(token.value)
            if len(parts) > 1:
                db_tables_list.append(parts)


def split_db_table(table_name):
    parts = table_name.replace('`', '').split('.')
    return ('unknown' if len(parts) == 1 else parts[0], parts[-1])