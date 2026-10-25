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
@file: test_major_hold_trace_logs.py
@desc: Unit tests for major_hold trace log collection.
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock

from plugins.rca.major_hold import MajorHoldScene
from src.handler.rca.rca_handler import RCA_ResultRecord


class TestMajorHoldTraceLogs(unittest.TestCase):
    def setUp(self):
        self.scene = MajorHoldScene()
        self.scene.local_path = tempfile.mkdtemp()
        self.scene.stdio = MagicMock()
        self.record = RCA_ResultRecord()

    def test_extract_trace_ids_from_diagnose_info(self):
        text = "error_no=-4016,error_trace=YB420A84FB23-00064F57EA191A97-0-0 trace_id=YAAA-BBB-0-0 Error trace: YCCC-DDD-0-0"
        self.assertEqual(
            self.scene._extract_trace_ids(text),
            ["YB420A84FB23-00064F57EA191A97-0-0", "YCCC-DDD-0-0", "YAAA-BBB-0-0"],
        )

    def test_collect_trace_logs_downloads_observer_and_rootservice_logs(self):
        ssh_client = MagicMock()
        ssh_client.get_name.return_value = "10.0.0.1:2882"
        ssh_client.exec_cmd.side_effect = ["FOUND", "", "FOUND", ""]
        self.scene.observer_nodes = [{"home_path": "/home/admin/oceanbase", "ssher": ssh_client}]

        self.scene._collect_trace_logs(["YB420A84FB23-00064F57EA191A97-0-0"], self.record)

        download_paths = [call.args[1] for call in ssh_client.download.call_args_list]
        self.assertEqual(len(download_paths), 2)
        self.assertTrue(any(path.endswith("_observer.log") for path in download_paths))
        self.assertTrue(any(path.endswith("_rootservice.log") for path in download_paths))
        self.assertTrue(all(os.path.dirname(path) == self.scene.local_path for path in download_paths))
        self.assertTrue(any("Downloaded trace_id log" in record for record in self.record.records))

    def test_collect_trace_logs_reports_when_not_found(self):
        ssh_client = MagicMock()
        ssh_client.get_name.return_value = "observer1"
        ssh_client.exec_cmd.side_effect = ["NOT_FOUND", "", "NOT_FOUND", ""]
        self.scene.observer_nodes = [{"home_path": "/home/admin/oceanbase", "ssher": ssh_client}]

        self.scene._collect_trace_logs(["YB420A84FB23-00064F57EA191A97-0-0"], self.record)

        ssh_client.download.assert_not_called()
        self.assertTrue(any("No observer/rootservice logs found for trace_id" in record for record in self.record.records))

    def test_collect_trace_logs_skips_duplicate_trace_ids(self):
        ssh_client = MagicMock()
        ssh_client.get_name.return_value = "observer1"
        ssh_client.exec_cmd.side_effect = ["NOT_FOUND", "", "NOT_FOUND", ""]
        self.scene.observer_nodes = [{"home_path": "/home/admin/oceanbase", "ssher": ssh_client}]

        self.scene._collect_trace_logs(["YAAA-BBB-0-0", "YAAA-BBB-0-0"], self.record)

        grep_commands = [call.args[0] for call in ssh_client.exec_cmd.call_args_list if "grep -F" in call.args[0]]
        self.assertEqual(len(grep_commands), 2)


if __name__ == '__main__':
    unittest.main()
