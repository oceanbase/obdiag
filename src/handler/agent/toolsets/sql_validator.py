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
@time: 2026/03/09
@file: sql_validator.py
@desc: SQL validation utilities for obdiag agent database tools
"""

import re
from typing import Tuple


def validate_sql(sql: str) -> Tuple[bool, str]:
    """
    Validate SQL query for safety — only allow read-only operations.

    Returns:
        Tuple of (is_valid, error_message)
    """
    if not sql or not sql.strip():
        return False, "Error: SQL query cannot be empty"

    sql_normalized = sql.strip()

    # Detect multiple statements (outside of quotes)
    semicolon_count = 0
    in_single_quote = False
    in_double_quote = False
    in_backtick = False

    for char in sql_normalized:
        if char == "'" and not in_double_quote and not in_backtick:
            in_single_quote = not in_single_quote
        elif char == '"' and not in_single_quote and not in_backtick:
            in_double_quote = not in_double_quote
        elif char == '`' and not in_single_quote and not in_double_quote:
            in_backtick = not in_backtick
        elif char == ';' and not in_single_quote and not in_double_quote and not in_backtick:
            semicolon_count += 1

    if semicolon_count > 1:
        return False, "Error: Only one SQL statement is allowed per query. Multiple statements detected."

    sql_for_validation = sql_normalized.rstrip(';').strip()
    sql_upper = sql_for_validation.upper().strip()

    allowed_keywords = ['SELECT', 'SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN', 'WITH']

    starts_with_keyword = None
    for keyword in allowed_keywords:
        if sql_upper.startswith(keyword):
            starts_with_keyword = keyword
            break

    if starts_with_keyword is None:
        return False, (
            f"Error: Only read-only SQL statements are allowed "
            f"(SELECT, SHOW, DESCRIBE, DESC, EXPLAIN, WITH). "
            f"Your query starts with: {sql_for_validation[:50]}"
        )

    if starts_with_keyword == 'WITH':
        if 'SELECT' not in sql_upper:
            return False, "Error: WITH statements must be followed by a SELECT statement"
        if sql_upper.find('SELECT', sql_upper.find('WITH')) == -1:
            return False, "Error: WITH statements must contain a SELECT statement"

    forbidden_keywords_pattern = re.compile(
        r'\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|REPLACE|MERGE|GRANT|REVOKE|COMMIT|ROLLBACK|LOCK|UNLOCK)\b',
        re.IGNORECASE,
    )

    if forbidden_keywords_pattern.search(sql_for_validation):
        return False, (
            "Error: Dangerous SQL operations are not allowed "
            "(INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, etc.). "
            "Only read-only queries are permitted."
        )

    if 'UNION' in sql_upper:
        union_parts = re.split(r'\bUNION\s+(?:ALL\s+)?', sql_upper, flags=re.IGNORECASE)
        for part in union_parts[1:]:
            part_stripped = part.strip()
            if not any(part_stripped.startswith(kw) for kw in allowed_keywords):
                return False, "Error: UNION queries must only contain SELECT statements"

    return True, ""
