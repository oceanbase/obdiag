#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.

"""
Unit tests for plugins/rca/freeze_dump.py — SQL builders, JSON save, execute flow (mocked DB).
"""

import json
import os
import tempfile
import unittest
from decimal import Decimal
from unittest.mock import MagicMock

from plugins.rca.freeze_dump import FreezeDumpScene, _FreezeDumpJsonEncoder
from src.handler.rca.rca_exception import RCAInitException


class _FakeCursor(object):
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


def _make_context(store_dir, ob_connector, input_parameters=None, gather_log=None):
    ctx = MagicMock()
    ctx.stdio = MagicMock()
    ctx.options = MagicMock()
    ctx.cluster_config = {"servers": []}

    def get_variable(key, default=None):
        m = {
            "store_dir": store_dir,
            "observer_nodes": [],
            "obproxy_nodes": None,
            "oms_nodes": None,
            "report": None,
            "obproxy_version": "",
            "observer_version": "4.4.1.0",
            "ob_connector": ob_connector,
            "ob_cluster": None,
            "input_parameters": input_parameters or {},
            "gather_log": gather_log,
        }
        return m.get(key, default)

    ctx.get_variable = MagicMock(side_effect=get_variable)
    return ctx


class TestFreezeDumpJsonEncoder(unittest.TestCase):
    def test_decimal_and_datetime(self):
        from datetime import datetime as dt

        payload = [{"a": Decimal("10.5"), "b": Decimal("3"), "c": dt(2024, 1, 2, 3, 4, 5)}]
        s = json.dumps(payload, cls=_FreezeDumpJsonEncoder)
        out = json.loads(s)
        self.assertEqual(out[0]["a"], 10.5)
        self.assertEqual(out[0]["b"], 3)
        self.assertIn("2024", out[0]["c"])


class TestFreezeDumpSqlBuilders(unittest.TestCase):
    def setUp(self):
        self.scene = FreezeDumpScene()

    def test_cdb_freeze_no_tenant(self):
        sql = self.scene._sql_cdb_freeze_info()
        self.assertIn("CDB_OB_FREEZE_INFO", sql)
        self.assertIn("READ_CONSISTENCY(WEAK)", sql)
        self.assertNotIn("WHERE TENANT_ID", sql)

    def test_cdb_freeze_with_tenant(self):
        self.scene.filter_tenant_id = 1002
        sql = self.scene._sql_cdb_freeze_info()
        self.assertIn("WHERE TENANT_ID = 1002", sql)

    def test_minor_freeze_with_tenant(self):
        self.scene.filter_tenant_id = 1002
        sql = self.scene._sql_minor_freeze_info()
        self.assertIn("__all_virtual_minor_freeze_info", sql)
        self.assertIn("AND tenant_id = 1002", sql)
        self.assertIn("WAIT_READY_FOR_FLUSH", sql)

    def test_compaction_suggestions_with_tenant(self):
        self.scene.filter_tenant_id = 1001
        sql = self.scene._sql_compaction_suggestions()
        self.assertIn("GV$OB_COMPACTION_SUGGESTIONS", sql)
        self.assertIn("WHERE TENANT_ID = 1001", sql)

    def test_compaction_suggestions_no_tenant(self):
        self.scene.filter_tenant_id = None
        sql = self.scene._sql_compaction_suggestions()
        self.assertIn("GV$OB_COMPACTION_SUGGESTIONS", sql)
        self.assertIn("SUGGESTION", sql)
        self.assertNotIn("WHERE TENANT_ID", sql)

    def test_memstore_sql_where(self):
        self.scene.filter_tenant_id = 1
        sql = self.scene._sql_tenant_memstore_info()
        self.assertIn("WHERE tenant_id = 1", sql)


class TestFreezeDumpRowHelpers(unittest.TestCase):
    def test_row_get_case_insensitive(self):
        self.assertEqual(FreezeDumpScene._row_get({"TENANT_ID": 5}, "tenant_id"), 5)
        self.assertIsNone(FreezeDumpScene._row_get(None, "x"))


class TestFreezeDumpExecuteMocked(unittest.TestCase):
    def test_execute_writes_six_core_json_files_and_handles_decimal(self):
        tmp = tempfile.mkdtemp()
        try:
            rows_seq = [
                [{"TENANT_ID": 1}],  # freeze
                [],  # minor
                [],  # diagnose
                [],  # suggestions
                [],  # progress
                [
                    {
                        "tenant_id": 1,
                        "svr_ip": "127.0.0.1",
                        "svr_port": 2882,
                        "memstore_used_gb": Decimal("1.25"),
                        "memstore_limit_gb": Decimal("10.0"),
                        "memstore_usage_percent": Decimal("12.5"),
                        "freeze_trigger_gb": Decimal("2.0"),
                    }
                ],
            ]
            idx = [0]

            def execute_sql(sql):
                i = idx[0]
                idx[0] += 1
                self.assertLess(i, len(rows_seq), "unexpected extra SQL call")
                return _FakeCursor(rows_seq[i])

            conn = MagicMock()
            conn.execute_sql_return_cursor_dictionary = MagicMock(side_effect=execute_sql)

            scene = FreezeDumpScene()
            scene.init(_make_context(tmp, conn))
            scene.execute()

            self.assertEqual(idx[0], 6, "should run exactly 6 main queries when no drill")

            expected_files = [
                "rca_freeze_dump_freeze_info.json",
                "rca_freeze_dump_minor_freeze_info.json",
                "rca_freeze_dump_compaction_diagnose.json",
                "rca_freeze_dump_compaction_suggestions.json",
                "rca_freeze_dump_compaction_progress.json",
                "rca_freeze_dump_tenant_memstore_info.json",
            ]
            for name in expected_files:
                path = os.path.join(tmp, name)
                self.assertTrue(os.path.isfile(path), "missing {0}".format(name))
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                self.assertIsInstance(data, list)

            mem_path = os.path.join(tmp, "rca_freeze_dump_tenant_memstore_info.json")
            with open(mem_path, "r", encoding="utf-8") as f:
                mem = json.load(f)
            self.assertEqual(mem[0]["memstore_used_gb"], 1.25)
            self.assertEqual(mem[0]["memstore_usage_percent"], 12.5)
        finally:
            for fn in os.listdir(tmp):
                os.remove(os.path.join(tmp, fn))
            os.rmdir(tmp)

    def test_execute_drill_on_finish_dedupes_server_tenant(self):
        tmp = tempfile.mkdtemp()
        try:
            minor_row = {
                "state": "FINISH",
                "svr_ip": "10.0.0.1",
                "svr_port": 2882,
                "tenant_id": 1001,
                "tablet_id": 111,
            }
            rows_seq = [
                [],
                [minor_row, dict(minor_row, tablet_id=222)],
                [],
                [],
                [],
                [],
                [],
                [],
            ]
            idx = [0]

            def execute_sql(sql):
                i = idx[0]
                idx[0] += 1
                return _FakeCursor(rows_seq[i] if i < len(rows_seq) else [])

            conn = MagicMock()
            conn.execute_sql_return_cursor_dictionary = MagicMock(side_effect=execute_sql)

            scene = FreezeDumpScene()
            scene.init(_make_context(tmp, conn))
            scene.execute()

            self.assertEqual(
                idx[0],
                8,
                "6 main + 1 memstore drill + 1 dag (same svr/tenant deduped)",
            )
            self.assertTrue(os.path.isfile(os.path.join(tmp, "rca_freeze_dump_memstore_drill_10.0.0.1_2882_1001.json")))
            self.assertTrue(os.path.isfile(os.path.join(tmp, "rca_freeze_dump_dag_scheduler_10.0.0.1_2882_1001.json")))
        finally:
            for fn in os.listdir(tmp):
                os.remove(os.path.join(tmp, fn))
            os.rmdir(tmp)

    def test_init_rejects_old_observer(self):
        tmp = tempfile.mkdtemp()
        try:

            def get_variable(key, default=None):
                m = {
                    "store_dir": tmp,
                    "observer_nodes": [],
                    "obproxy_nodes": None,
                    "oms_nodes": None,
                    "report": None,
                    "obproxy_version": "",
                    "observer_version": "3.1.0.0",
                    "ob_connector": MagicMock(),
                    "ob_cluster": None,
                    "input_parameters": {},
                    "gather_log": None,
                }
                return m.get(key, default)

            ctx = MagicMock()
            ctx.stdio = MagicMock()
            ctx.options = MagicMock()
            ctx.cluster_config = {"servers": []}
            ctx.get_variable = MagicMock(side_effect=get_variable)

            scene = FreezeDumpScene()
            with self.assertRaises(RCAInitException):
                scene.init(ctx)
        finally:
            os.rmdir(tmp)

    def test_maybe_gather_logs_invokes_gather(self):
        tmp = tempfile.mkdtemp()
        try:
            gl = MagicMock()
            gl.execute = MagicMock(return_value=["/a/log1"])
            rows_seq = [[], [], [], [], [], []]
            idx = [0]

            def execute_sql(sql):
                i = idx[0]
                idx[0] += 1
                return _FakeCursor(rows_seq[i])

            conn = MagicMock()
            conn.execute_sql_return_cursor_dictionary = MagicMock(side_effect=execute_sql)

            scene = FreezeDumpScene()
            scene.init(_make_context(tmp, conn, input_parameters={"collect_logs": "true"}, gather_log=gl))
            scene.execute()

            gl.init_parameters.assert_called()
            gl.set_parameters.assert_any_call("scope", "observer")
            self.assertEqual(gl.grep.call_count, len(scene.LOG_GREP_KEYWORDS))
            gl.execute.assert_called_once()
        finally:
            for fn in os.listdir(tmp):
                os.remove(os.path.join(tmp, fn))
            os.rmdir(tmp)


if __name__ == "__main__":
    unittest.main()
