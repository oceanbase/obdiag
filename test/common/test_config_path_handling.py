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

"""Regression tests for configured deployment path handling."""

import threading
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

from src.common.command import get_observer_pid, get_observer_version_by_ssh
from src.common.config import ConfigManager
from src.common.path_utils import quote_shell_path, validate_config_path
from src.handler.tools.config_check_handler import ConfigCheckHandler
from src.handler.tools.io_performance_handler import IoPerformanceHandler


def build_config_manager(config_data):
    manager = object.__new__(ConfigManager)
    manager.config_data = config_data
    return manager


class TestConfigPathValidation(unittest.TestCase):
    def test_accepts_supported_paths(self):
        paths = ["/home/admin/oceanbase", "/data/observer-4.3.5", "/数据/observer", "~/obproxy", "~"]
        for path in paths:
            with self.subTest(path=path):
                self.assertEqual(validate_config_path(path, "home_path"), path)

    def test_rejects_invalid_config_path(self):
        invalid_paths = ["", "relative/path", "/path with spaces", "/path;next", "/path$value", "/path\nnext", 42, None]
        for path in invalid_paths:
            with self.subTest(path=path):
                with self.assertRaisesRegex(ValueError, "Invalid configuration path for home_path"):
                    validate_config_path(path, "home_path")

    def test_preserves_supported_home_relative_path(self):
        self.assertEqual(quote_shell_path("~/observer/bin/observer"), '"$HOME"/observer/bin/observer')

    def test_uses_global_and_node_path_values(self):
        manager = build_config_manager(
            {
                "obcluster": {
                    "servers": {
                        "global": {"home_path": "/global/observer", "data_dir": "/global/data", "redo_dir": "/global/redo"},
                        "nodes": [{"ip": "127.0.0.1"}, {"ip": "192.0.2.1", "home_path": "/node/observer", "data_dir": "/node/data", "redo_dir": "/node/redo"}],
                    }
                },
                "obproxy": {"servers": {"global": {"home_path": "~/obproxy"}, "nodes": [{"ip": "127.0.0.1"}]}},
            }
        )

        observer_nodes = manager.get_ob_cluster_config["servers"]
        self.assertEqual(observer_nodes[0]["home_path"], "/global/observer")
        self.assertEqual(observer_nodes[0]["data_dir"], "/global/data")
        self.assertEqual(observer_nodes[0]["redo_dir"], "/global/redo")
        self.assertEqual(observer_nodes[1]["home_path"], "/node/observer")
        self.assertEqual(observer_nodes[1]["data_dir"], "/node/data")
        self.assertEqual(observer_nodes[1]["redo_dir"], "/node/redo")
        self.assertEqual(manager.get_obproxy_config["servers"][0]["home_path"], "~/obproxy")

    def test_applies_defaults_for_missing_paths(self):
        manager = build_config_manager({"obcluster": {"servers": {"nodes": [{"ip": "127.0.0.1"}]}}, "obproxy": {"servers": {"nodes": [{"ip": "127.0.0.1"}]}}})

        observer = manager.get_ob_cluster_config["servers"][0]
        self.assertEqual(observer["home_path"], "/root/observer")
        self.assertEqual(observer["data_dir"], "/root/observer/store")
        self.assertEqual(observer["redo_dir"], "/root/observer/store")
        self.assertEqual(manager.get_obproxy_config["servers"][0]["home_path"], "/root/obproxy")

    def test_rejects_invalid_observer_paths_before_downstream_use(self):
        for field_name in ("home_path", "data_dir", "redo_dir"):
            with self.subTest(field_name=field_name):
                manager = build_config_manager({"obcluster": {"servers": {"global": {field_name: "/invalid;path"}, "nodes": [{"ip": "127.0.0.1"}]}}})

                with self.assertRaisesRegex(ValueError, r"obcluster\.servers\.nodes\[0\]\." + field_name):
                    manager.get_ob_cluster_config

    def test_rejects_invalid_obproxy_path_before_downstream_use(self):
        manager = build_config_manager({"obproxy": {"servers": {"nodes": [{"ip": "127.0.0.1", "home_path": "/invalid;path"}]}}})

        with self.assertRaisesRegex(ValueError, r"obproxy\.servers\.nodes\[0\]\.home_path"):
            manager.get_obproxy_config

    @patch("src.handler.tools.config_check_handler.SshClient")
    def test_does_not_call_downstream_handler_for_invalid_path(self, ssh_client_class):
        manager = build_config_manager({"obcluster": {"servers": {"nodes": [{"ip": "127.0.0.1", "home_path": "relative/path"}]}}})

        with self.assertRaises(ValueError):
            nodes = manager.get_ob_cluster_config["servers"]
            ConfigCheckHandler(SimpleNamespace(stdio=Mock(), options=SimpleNamespace(), cluster_config={}, obproxy_config={}))._check_single_node(1, nodes[0], "observer", [], threading.Lock(), {})

        ssh_client_class.assert_not_called()


class TestConfigPathCommands(unittest.TestCase):
    def setUp(self):
        self.stdio = Mock()

    def test_passes_home_relative_path_to_version_handler(self):
        ssh_client = Mock()
        ssh_client.exec_cmd.return_value = "observer (OceanBase_CE 4.2.1.0)"

        self.assertEqual(get_observer_version_by_ssh(ssh_client, "~/observer", self.stdio), "4.2.1.0")
        ssh_client.exec_cmd.assert_called_once_with('"$HOME"/observer/bin/observer --version')

    def test_passes_home_relative_path_to_pid_handler(self):
        ssh_client = Mock()
        ssh_client.exec_cmd.side_effect = ["123", "123"]

        self.assertEqual(get_observer_pid(ssh_client, "~/observer", self.stdio), ["123"])
        self.assertEqual(ssh_client.exec_cmd.call_args_list[0], call('cat -- "$HOME"/observer/run/observer.pid'))

    @patch("src.handler.tools.config_check_handler.SshClient")
    def test_config_check_uses_validated_paths_for_local_and_remote_nodes(self, ssh_client_class):
        ssh_client = Mock()
        ssh_client.exec_cmd.side_effect = ["obdiag_test", "yes", "yes", "yes", "obdiag_test", "yes", "yes", "yes"]
        ssh_client_class.return_value = ssh_client
        context = SimpleNamespace(stdio=self.stdio, options=SimpleNamespace(), cluster_config={}, obproxy_config={})
        handler = ConfigCheckHandler(context)
        results = {"summary": {"success": 0, "failed": 0, "skipped": 0}}
        manager = build_config_manager(
            {
                "obcluster": {
                    "servers": {
                        "global": {"ssh_username": "user", "home_path": "~/observer", "data_dir": "~/data", "redo_dir": "~/redo"},
                        "nodes": [{"ip": "127.0.0.1", "ssh_type": "local"}, {"ip": "192.0.2.1", "ssh_type": "remote"}],
                    }
                }
            }
        )
        nodes = manager.get_ob_cluster_config["servers"]

        for index, node in enumerate(nodes, start=1):
            handler._check_single_node(index, node, "observer", [], threading.Lock(), results)

        expected_path_calls = [
            call('test -f "$HOME"/observer/bin/observer && echo \'yes\' || echo \'no\''),
            call('test -d "$HOME"/data/sstable && echo \'yes\' || echo \'no\''),
            call('test -d "$HOME"/redo/clog && echo \'yes\' || echo \'no\''),
        ]
        self.assertEqual(ssh_client.exec_cmd.call_args_list[1:4], expected_path_calls)
        self.assertEqual(ssh_client.exec_cmd.call_args_list[5:8], expected_path_calls)

    def test_io_performance_quotes_configured_and_discovered_paths(self):
        ssh_client = Mock()
        ssh_client.exec_cmd.side_effect = ["/data/store/clog", "/dev/sda"]
        handler = object.__new__(IoPerformanceHandler)
        handler.stdio = self.stdio
        node = {"ip": "127.0.0.1", "redo_dir": "~/store", "ssher": ssh_client}

        self.assertEqual(handler._get_disk_device(node, "clog"), "sda")
        self.assertEqual(ssh_client.exec_cmd.call_args_list[0], call('find "$HOME"/store -name "clog" -type d 2>/dev/null | head -1'))
        self.assertIn("df -P /data/store/clog", ssh_client.exec_cmd.call_args_list[1].args[0])


if __name__ == "__main__":
    unittest.main()
