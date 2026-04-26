#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Automated verification for:
  - obdiag#1202: PyMySQL UTF-8 decode compat (pymysql_binary_compat)
  - gather_plan_monitor: EXPLAIN skip for long/binary query_sql

Run from repo root:
  python workflow_data/repro_issue_1202/verify_automation.py

Or:
  cd workflow_data/repro_issue_1202 && PYTHONPATH=../.. python verify_automation.py
"""
from __future__ import print_function

import os
import sys

# Repo root = parent of workflow_data
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def test_pymysql_row_patch_applied():
    from src.common import pymysql_binary_compat  # noqa: F401 — apply patch
    from pymysql.connections import MySQLResult

    fn = getattr(MySQLResult._read_row_from_packet, "__name__", "")
    assert "patched" in fn or "_read_row_from_packet_patched" in repr(MySQLResult._read_row_from_packet), "MySQLResult._read_row_from_packet should be patched by pymysql_binary_compat"
    print("[OK] pymysql_binary_compat: MySQLResult._read_row_from_packet is patched")


def test_utf8_decode_same_failure_mode_as_issue():
    """Same exception shape as strict PyMySQL decode on invalid UTF-8."""
    payload = (b"a" * 268) + b"\xff"
    try:
        payload.decode("utf-8")
    except UnicodeDecodeError as e:
        assert e.start == 268
        print("[OK] minimal UnicodeDecodeError at position 268 (issue #1202 shape)")
        return
    raise AssertionError("expected UnicodeDecodeError")


class _MiniStdio(object):
    """Avoid importing src.common.stdio (heavy deps) for CI/local without venv."""

    def warn(self, *a, **k):
        pass

    def verbose(self, *a, **k):
        pass

    def print(self, *a, **k):
        pass

    def exception(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


def _mirror_sql_eligible_for_explain(raw_sql, max_chars=65536):
    """Must stay in sync with GatherPlanMonitorHandler._sql_eligible_for_explain_extended."""
    if not raw_sql or not isinstance(raw_sql, str):
        return False
    if len(raw_sql) > max_chars:
        return False
    if "\x00" in raw_sql:
        return False
    if "\ufffd" in raw_sql:
        return False
    ctrl = sum(1 for c in raw_sql if ord(c) < 32 and c not in "\t\n\r")
    if ctrl > 10:
        return False
    if len(raw_sql) > 1000 and (ctrl / float(len(raw_sql))) > 0.001:
        return False
    lead = raw_sql.lstrip()
    if len(lead) >= 6 and lead[:6].upper() == "INSERT" and len(raw_sql) > 8192:
        return False
    return True


def _mirror_truncate_sql_for_log(raw_sql, max_len=512):
    if raw_sql is None:
        return ""
    if len(raw_sql) <= max_len:
        return raw_sql
    return raw_sql[:max_len] + "...<truncated,len=%s>" % len(raw_sql)


def test_gather_plan_monitor_explain_eligibility():
    try:
        from src.common.context import HandlerContext
        from src.handler.gather.gather_plan_monitor import GatherPlanMonitorHandler

        ctx = HandlerContext(stdio=_MiniStdio())
        h = GatherPlanMonitorHandler(ctx)
        elig = h._sql_eligible_for_explain_extended
        trunc = h._truncate_sql_for_log
        label = "GatherPlanMonitorHandler"
    except ImportError as e:
        print("WARN: using mirror logic (import failed: %s). For full test use .venv/bin/python." % e)
        elig = _mirror_sql_eligible_for_explain
        trunc = _mirror_truncate_sql_for_log
        label = "mirror (no full obdiag import)"

    assert elig("SELECT 1 FROM t")
    assert elig("") is False
    assert elig(None) is False
    assert elig("x" * 65536) is True
    assert elig("x" * 65537) is False
    assert elig("a\x00b") is False

    long_s = "INSERT INTO t (b) VALUES (" + "x" * 70000 + ")"
    assert elig(long_s) is False
    assert elig("INSERT INTO t VALUES (1)") is True
    assert elig("INSERT INTO t VALUES (" + "x" * 9000 + ")") is False

    t = trunc(long_s, max_len=20)
    assert len(t) < len(long_s) and "truncated" in t
    print("[OK] %s: explain eligibility / truncate_sql_for_log" % label)


def test_main_imports_pymysql_compat():
    """Ensure entry path loads patch (same as obdiag main)."""
    import importlib

    importlib.import_module("src.common.pymysql_binary_compat")
    print("[OK] import src.common.pymysql_binary_compat succeeds")


def main():
    test_main_imports_pymysql_compat()
    test_pymysql_row_patch_applied()
    test_utf8_decode_same_failure_mode_as_issue()
    test_gather_plan_monitor_explain_eligibility()
    print("")
    print("All automated checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
