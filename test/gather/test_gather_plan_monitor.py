#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

"""
@time: 2026/6/8
@file: test_gather_plan_monitor.py
@desc: Unit tests for gather_plan_monitor result paths.
"""

import os
import unittest
from unittest.mock import MagicMock

from src.handler.gather.gather_plan_monitor import GatherPlanMonitorHandler


class TestGatherPlanMonitorResultPaths(unittest.TestCase):
    def test_build_result_data_includes_report_and_resources_paths(self):
        context = MagicMock()
        context.stdio = MagicMock()
        context.get_variable.return_value = None
        handler = GatherPlanMonitorHandler(context)
        handler.report_file_path = "/tmp/pack/sql_plan_monitor_report.html"

        data = handler._build_result_data("/tmp/pack")

        self.assertEqual(data["store_dir"], "/tmp/pack")
        self.assertEqual(data["report_file"], "/tmp/pack/sql_plan_monitor_report.html")
        self.assertEqual(data["resources_dir"], os.path.join("/tmp/pack", "resources"))


if __name__ == '__main__':
    unittest.main()
