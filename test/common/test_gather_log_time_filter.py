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

"""日志采集按时间区间筛选文件的回归测试（issue #1269）。"""

import datetime
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from src.common.diag_cmd import ObdiagGatherAllCommand, ObdiagGatherLogCommand, ObdiagGatherObproxyLogCommand
from src.common.ssh_client.local_client import LocalClient
from src.handler.gather.gather_log.obproxy import ObproxyGatherLogOnNode
from src.handler.gather.gather_log.observer import ObserverGatherLogOnNode

LOG_DIR = "/home/admin/log"


def build_handler(handler_class, from_time, to_time):
    context = SimpleNamespace(stdio=Mock())
    handler = handler_class(context, {"home_path": "/home/admin"}, {"from_time": from_time, "to_time": to_time, "scope": {}})
    handler.log_path = LOG_DIR
    handler.ssh_client = Mock()
    return handler


class TestObproxyLogTimeFilter(unittest.TestCase):
    # obproxy.log.<T> holds logs from the previous rotation up to T
    FILES = [
        "obproxy.log",
        "obproxy.log.20261111151129",  # ... ~ 15:11:29
        "obproxy.log.20261111152510",  # 15:11:29 ~ 15:25:10
        "obproxy.log.20261111153840",  # 15:25:10 ~ 15:38:40
        "obproxy_digest.log",
        "obproxy_digest.log.20261111152000",  # ... ~ 15:20:00
        "obproxy_digest.log.20261111160000",  # 15:20:00 ~ 16:00:00
    ]

    def select(self, from_time, to_time):
        handler = build_handler(ObproxyGatherLogOnNode, from_time, to_time)
        return handler._get_logfile_names_by_time("\n".join(self.FILES) + "\n")

    def test_parse_14_digit_timestamp(self):
        handler = build_handler(ObproxyGatherLogOnNode, "2026-11-11 15:20:00", "2026-11-11 15:30:00")
        self.assertEqual(handler._parse_timestamp_from_filename("obproxy.log.20261111212023"), datetime.datetime(2026, 11, 11, 21, 20, 23))
        self.assertIsNone(handler._parse_timestamp_from_filename("obproxy.log"))
        self.assertEqual(handler._get_log_type("obproxy_digest.log.20261111212023"), "obproxy_digest.log")

    def test_rotated_files_overlapping_range_are_selected(self):
        self.assertEqual(
            self.select("2026-11-11 15:20:00", "2026-11-11 15:30:00"),
            ["obproxy.log.20261111152510", "obproxy.log.20261111153840", "obproxy_digest.log.20261111152000", "obproxy_digest.log.20261111160000"],
        )

    def test_range_inside_one_rotated_file(self):
        self.assertEqual(self.select("2026-11-11 15:27:00", "2026-11-11 15:30:00"), ["obproxy.log.20261111153840", "obproxy_digest.log.20261111160000"])

    def test_current_file_is_selected_only_after_last_rotation(self):
        self.assertEqual(self.select("2026-11-11 15:50:00", "2026-11-11 16:10:00"), ["obproxy.log", "obproxy_digest.log", "obproxy_digest.log.20261111160000"])

    def test_unrecognized_file_is_skipped(self):
        handler = build_handler(ObproxyGatherLogOnNode, "2026-11-11 15:20:00", "2026-11-11 15:30:00")
        self.assertEqual(handler._get_logfile_names_by_time("obproxy.log\nobproxy.pid\n"), ["obproxy.log"])


class TestObserverLogTimeFilter(unittest.TestCase):
    ALERT_DIR = LOG_DIR + "/alert"
    LOGS = [
        ("observer.log", LOG_DIR),
        ("observer.log.20261110151129055", LOG_DIR),  # ... ~ 15:11:29
        ("observer.log.20261110152510123.zst", LOG_DIR),  # 15:11:29 ~ 15:25:10, compressed
        ("observer.log.20261110153840456", LOG_DIR),  # 15:25:10 ~ 15:38:40
        ("alert.log", ALERT_DIR),
        ("alert.log.20261110120000000", ALERT_DIR),  # ... ~ 12:00:00
    ]

    def select(self, from_time, to_time):
        handler = build_handler(ObserverGatherLogOnNode, from_time, to_time)
        return handler._filter_logs_by_time(self.LOGS)

    def test_rotated_files_overlapping_range_are_selected(self):
        self.assertEqual(
            self.select("2026-11-10 15:20:00", "2026-11-10 15:30:00"),
            [("observer.log.20261110152510123.zst", LOG_DIR), ("observer.log.20261110153840456", LOG_DIR), ("alert.log", self.ALERT_DIR)],
        )

    def test_range_inside_one_rotated_file(self):
        self.assertEqual(self.select("2026-11-10 15:27:00", "2026-11-10 15:30:00"), [("observer.log.20261110153840456", LOG_DIR), ("alert.log", self.ALERT_DIR)])

    def test_range_before_first_rotation(self):
        self.assertEqual(
            self.select("2026-11-10 11:00:00", "2026-11-10 11:30:00"),
            [("observer.log.20261110151129055", LOG_DIR), ("alert.log.20261110120000000", self.ALERT_DIR)],
        )


class TestLogTimeRangeHelp(unittest.TestCase):
    def test_help_explains_file_selection_and_unknown_start_fallback(self):
        for command_class in (ObdiagGatherAllCommand, ObdiagGatherLogCommand, ObdiagGatherObproxyLogCommand):
            with self.subTest(command=command_class.__name__):
                command = command_class()
                help_text = " ".join(command.parser.format_help().split())
                self.assertIn("whole overlapping files", help_text)
                self.assertIn("first timestamp when readable", help_text)
                self.assertIn("otherwise inferred from rotation history", help_text)
                options, _ = command.parser.parse_args(["--from", "2026-01-01 10:00:00", "--to", "2026-01-01 10:05:00"])
                self.assertEqual(getattr(options, "from"), "2026-01-01 10:00:00")
                self.assertEqual(options.to, "2026-01-01 10:05:00")


class TestCurrentLogStartTime(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix="obdiag-current-log-")
        self.addCleanup(directory.cleanup)
        self.log_dir = Path(directory.name)

    def build_current_handler(self, handler_class, content, from_time="2026-01-01 10:00:00", to_time="2026-01-01 10:05:00"):
        handler = build_handler(handler_class, from_time, to_time)
        handler.context.inner_config = {}
        handler.log_path = str(self.log_dir)
        handler.ssh_client = LocalClient(handler.context, {"ssh_type": "local"})
        name = handler.TARGET_NAME + ".log"
        (self.log_dir / name).write_text(content, encoding="utf-8")
        return handler, name

    def select(self, handler, names):
        if isinstance(handler, ObserverGatherLogOnNode):
            return [name for name, _ in handler._filter_logs_by_time([(name, str(self.log_dir)) for name in names])]
        return handler._get_logfile_names_by_time("\n".join(names))

    def test_current_file_starting_after_query_is_excluded_without_rotations(self):
        for handler_class in (ObserverGatherLogOnNode, ObproxyGatherLogOnNode):
            with self.subTest(handler=handler_class.__name__):
                handler, name = self.build_current_handler(handler_class, "[2026-01-01 12:00:00.000000] INFO New syslog file info\n")
                self.assertEqual(self.select(handler, [name]), [])

    def test_restart_gap_does_not_extend_current_file_into_query(self):
        for handler_class, suffix in ((ObserverGatherLogOnNode, "20260101090000000"), (ObproxyGatherLogOnNode, "20260101090000")):
            with self.subTest(handler=handler_class.__name__):
                handler, name = self.build_current_handler(handler_class, "[2026-01-01 12:00:00.000000] INFO restarted\n")
                self.assertEqual(self.select(handler, [name + "." + suffix, name]), [])

    def test_overlapping_current_file_is_kept_whole(self):
        content = "[2026-01-01 09:55:00.000000] INFO before range\n[2026-01-01 10:03:00.000000] INFO in range\n"
        handler, name = self.build_current_handler(ObserverGatherLogOnNode, content)
        self.assertEqual(self.select(handler, [name]), [name])
        output_dir = self.log_dir / "output"
        output_dir.mkdir()
        handler._log_path_mapping = {name: str(self.log_dir)}
        handler._grep_log_to_tmp([name], str(output_dir))
        self.assertEqual((output_dir / name).read_text(), content)

    def test_fractional_first_timestamp_is_compared_without_truncation(self):
        handler, name = self.build_current_handler(ObserverGatherLogOnNode, "[2026-01-01 10:05:00.000001] INFO after boundary\n")
        self.assertEqual(self.select(handler, [name]), [])

    def test_first_timestamp_at_query_end_is_kept(self):
        handler, name = self.build_current_handler(ObserverGatherLogOnNode, "[2026-01-01 10:05:00] INFO boundary\n")
        self.assertEqual(self.select(handler, [name]), [name])

    def test_unreadable_or_unrecognized_first_line_keeps_conservative_fallback(self):
        for content in ("", "unrecognized header\n", "[2026-13-01 12:00:00.000000] INFO invalid date\n"):
            with self.subTest(content=content):
                handler, name = self.build_current_handler(ObserverGatherLogOnNode, content)
                self.assertEqual(self.select(handler, [name]), [name])
        (self.log_dir / name).unlink()
        self.assertEqual(self.select(handler, [name]), [name])
        handler.ssh_client = Mock()
        handler.ssh_client.exec_cmd.side_effect = RuntimeError("read failed")
        self.assertEqual(self.select(handler, [name]), [name])

    def test_unrecognized_header_preserves_rotation_based_exclusion(self):
        handler, name = self.build_current_handler(ObserverGatherLogOnNode, "unrecognized header\n")
        handler.ssh_client = Mock(wraps=handler.ssh_client)
        self.assertEqual(self.select(handler, [name + ".20260101120000000", name]), [name + ".20260101120000000"])
        handler.ssh_client.exec_cmd.assert_not_called()

    def test_reading_first_line_quotes_shell_paths(self):
        self.log_dir = self.log_dir / "node's $(touch injected) logs"
        self.log_dir.mkdir()
        handler, name = self.build_current_handler(ObserverGatherLogOnNode, "[2026-01-01 12:00:00.000000] INFO newer\n")
        self.assertEqual(self.select(handler, [name]), [])

    def test_warning_file_start_is_checked(self):
        for handler_class in (ObserverGatherLogOnNode, ObproxyGatherLogOnNode):
            with self.subTest(handler=handler_class.__name__):
                handler, name = self.build_current_handler(handler_class, "[2026-01-01 12:00:00.000000] WARN newer\n")
                warning_name = name + ".wf"
                (self.log_dir / name).rename(self.log_dir / warning_name)
                self.assertEqual(self.select(handler, [warning_name]), [])

    def test_discovery_excludes_new_current_log_without_rotation_history(self):
        for handler_class in (ObserverGatherLogOnNode, ObproxyGatherLogOnNode):
            with self.subTest(handler=handler_class.__name__):
                handler, name = self.build_current_handler(handler_class, "[2026-01-01 12:00:00.000000] INFO newer\n")
                handler.scope = {handler.TARGET_NAME: handler.LOG_SCOPES[handler.TARGET_NAME]}
                self.assertEqual(handler._find_logs_name(), [])

    def test_recent_count_still_bypasses_time_filtering(self):
        for handler_class in (ObserverGatherLogOnNode, ObproxyGatherLogOnNode):
            with self.subTest(handler=handler_class.__name__):
                handler, name = self.build_current_handler(handler_class, "[2026-01-01 12:00:00.000000] INFO newer\n")
                handler.scope = {handler.TARGET_NAME: handler.LOG_SCOPES[handler.TARGET_NAME]}
                handler.recent_count = 1
                handler.ssh_client = Mock(wraps=handler.ssh_client)
                self.assertEqual(handler._find_logs_name(), [name])
                self.assertFalse(any(call.args[0].startswith("head ") for call in handler.ssh_client.exec_cmd.call_args_list))


if __name__ == "__main__":
    unittest.main()
