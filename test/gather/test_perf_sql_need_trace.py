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
@time: 2026/6/8
@file: test_perf_sql_need_trace.py
@desc: Unit tests for observer.perf_sql need_trace support.
"""

import os
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from plugins.gather.tasks.observer.perf_sql import PerfSQL
from src.common.result_type import ObdiagResult


class TestPerfSQLNeedTrace(unittest.TestCase):
    def setUp(self):
        self.context = MagicMock()
        self.context.stdio = MagicMock()
        self.context.options = SimpleNamespace(original=True)
        self.context.cluster_config = {"servers": []}
        self.context.obproxy_config = {"servers": []}
        self.context.get_variable.return_value = {}
        self.scene = PerfSQL()
        self.scene.init(self.context, "observer.perf_sql", "/tmp/perf_sql_report", env={})
        self.scene.trace_id = "YB420BA2D99B-0005EBBFC45D5A00-0-0"

    def test_need_trace_enabled_accepts_true_strings(self):
        for value in (True, "True", "true", "1", "yes", "on"):
            self.scene.env = {"need_trace": value}
            self.assertTrue(self.scene._need_trace_enabled())

        for value in (False, "False", "0", "no", "", None):
            self.scene.env = {"need_trace": value}
            self.assertFalse(self.scene._need_trace_enabled())

    def test_gather_flt_trace_uses_trace_id_and_restores_options(self):
        original_options = self.context.options
        handler = MagicMock()
        handler.handle.return_value = ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": "/tmp/flt"})
        captured_options = []

        def make_handler(context, gather_pack_dir=None):
            captured_options.append(context.options)
            return handler

        with patch("plugins.gather.tasks.observer.perf_sql.AnalyzeFltTraceHandler", side_effect=make_handler) as mock_handler:
            result = self.scene._PerfSQL__gather_flt_trace()

        self.assertEqual(result.code, ObdiagResult.SUCCESS_CODE)
        self.assertIs(self.context.options, original_options)
        mock_handler.assert_called_once()
        options = captured_options[0]
        self.assertEqual(options.flt_trace_id, self.scene.trace_id)
        self.assertEqual(options.store_dir, os.path.join(self.scene.report_path, "flt_trace"))

    def test_execute_runs_flt_trace_when_need_trace_true(self):
        self.scene.env = {"need_trace": "True"}
        self.context.get_variable.side_effect = lambda key, default=None: "sql" if key == "gather_skip_type" else self.scene.env

        with (
            patch.object(self.scene, "_PerfSQL__parse_env", return_value=True),
            patch.object(self.scene, "_PerfSQL__gather_log") as gather_log,
            patch.object(self.scene, "_PerfSQL__gather_obproxy_log") as gather_obproxy_log,
            patch.object(self.scene, "_PerfSQL__gather_sql_info") as gather_sql_info,
            patch.object(self.scene, "_PerfSQL__gather_dbms_xplan_opt_trace") as gather_xplan,
            patch.object(self.scene, "_PerfSQL__gather_flt_trace") as gather_flt_trace,
        ):
            self.scene.execute()

        gather_log.assert_called_once()
        gather_obproxy_log.assert_called_once()
        gather_sql_info.assert_not_called()
        gather_xplan.assert_not_called()
        gather_flt_trace.assert_called_once()


if __name__ == '__main__':
    unittest.main()
