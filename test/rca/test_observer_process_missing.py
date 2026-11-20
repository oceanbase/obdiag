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
@time: 2026/06/08
@file: test_observer_process_missing.py
@desc: Unit tests for observer_process_missing RCA scene.
"""

import tempfile
import unittest
from unittest.mock import MagicMock, patch

from plugins.rca.observer_process_missing import ObserverProcessMissingScene
from src.handler.rca.rca_handler import RCA_ResultRecord


class TestObserverProcessMissingScene(unittest.TestCase):
    def setUp(self):
        self.scene = ObserverProcessMissingScene()
        self.scene.stdio = MagicMock()
        self.scene.store_dir = tempfile.mkdtemp()
        self.scene.record = RCA_ResultRecord()

    def _node(self, ssh_client):
        return {
            "ip": "10.0.0.1",
            "ssh_port": 22,
            "home_path": "/home/admin/oceanbase",
            "ssher": ssh_client,
        }

    def _ssh_client(self, content_checks):
        ssh_client = MagicMock()
        ssh_client.get_name.return_value = "10.0.0.1:22"
        responses = ["/tmp/obdiag_observer_process_missing.ABCDEF"]
        responses.extend(["" for _ in range(5)])
        responses.extend(content_checks)
        responses.append("")
        ssh_client.exec_cmd.side_effect = responses
        return ssh_client

    @patch("plugins.rca.observer_process_missing.get_observer_pid", return_value=[])
    def test_missing_process_collects_core_and_keyword_evidence(self, mock_get_pid):
        ssh_client = self._ssh_client(["EMPTY_FILE", "HAS_FILE", "HAS_FILE", "EMPTY_FILE", "HAS_FILE"])

        status = self.scene._inspect_node(self._node(ssh_client))

        self.assertEqual(status, "missing")
        mock_get_pid.assert_called_once()
        self.assertEqual(ssh_client.download.call_count, 3)
        self.assertTrue(any("observer process is missing" in record for record in self.scene.record.records))
        self.assertTrue(any("core file candidate list saved" in record for record in self.scene.record.records))
        self.assertTrue(any("found core file candidates" in self.scene.record.suggest for _ in [0]))
        self.assertTrue(any("rm -rf /tmp/obdiag_observer_process_missing.ABCDEF" in call.args[0] for call in ssh_client.exec_cmd.call_args_list))

    @patch("plugins.rca.observer_process_missing.get_observer_pid", return_value=["12345"])
    def test_running_process_records_process_start_time(self, mock_get_pid):
        ssh_client = MagicMock()
        ssh_client.get_name.return_value = "observer1"
        ssh_client.exec_cmd.side_effect = [
            "12345 Mon Jun  8 10:00:00 2026 01:02:03",
            "/tmp/obdiag_observer_process_missing.ABCDEF",
            "",
            "",
            "",
            "",
            "",
            "HAS_FILE",
            "EMPTY_FILE",
            "EMPTY_FILE",
            "HAS_FILE",
            "HAS_FILE",
            "",
        ]

        status = self.scene._inspect_node(self._node(ssh_client))

        self.assertEqual(status, "running")
        mock_get_pid.assert_called_once()
        self.assertTrue(any("process start time" in record for record in self.scene.record.records))
        self.assertEqual(ssh_client.download.call_count, 3)

    @patch("plugins.rca.observer_process_missing.get_observer_pid", return_value=[])
    def test_execute_filters_by_node_ip(self, mock_get_pid):
        ssh_client = self._ssh_client(["EMPTY_FILE", "EMPTY_FILE", "EMPTY_FILE", "EMPTY_FILE", "EMPTY_FILE"])
        self.scene.observer_nodes = [
            self._node(ssh_client),
            {"ip": "10.0.0.2", "home_path": "/home/admin/oceanbase", "ssher": MagicMock()},
        ]
        self.scene.node_ip = "10.0.0.1"

        self.scene.execute()

        mock_get_pid.assert_called_once()
        self.assertIn("observer process is missing on 1 node(s)", self.scene.record.suggest)

    def test_cleanup_remote_dir_ignores_unexpected_path(self):
        ssh_client = MagicMock()

        self.scene._cleanup_remote_dir(ssh_client, "/home/admin")

        ssh_client.exec_cmd.assert_not_called()

    def test_safe_name_removes_shell_sensitive_chars(self):
        self.assertEqual(self.scene._safe_name("10.0.0.1:22;rm -rf /"), "10.0.0.1_22_rm_-rf")


if __name__ == '__main__':
    unittest.main()
