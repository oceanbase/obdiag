#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.

"""Tests for sql_syntax_handler normalization."""

import pytest

from src.handler.tools.sql_syntax_handler import normalize_sql_for_syntax_check


@pytest.mark.parametrize(
    "raw,expected_sql",
    [
        ("SELECT 1", "SELECT 1"),
        ("  SELECT 1  ", "SELECT 1"),
        ("SELECT 1;", "SELECT 1"),
        ("SELECT 1;;", "SELECT 1"),
    ],
)
def test_normalize_accepts_single_statement(raw, expected_sql):
    sql, err = normalize_sql_for_syntax_check(raw)
    assert err is None
    assert sql == expected_sql


@pytest.mark.parametrize(
    "raw",
    [
        "SELECT 1; SELECT 2",
        "SELECT 1;\nSELECT 2",
        "",
        "   ",
    ],
)
def test_normalize_rejects_invalid(raw):
    sql, err = normalize_sql_for_syntax_check(raw)
    assert err is not None
    assert sql is None


def test_normalize_rejects_null_byte():
    sql, err = normalize_sql_for_syntax_check("SELECT 1\x00")
    assert err is not None
    assert sql is None
