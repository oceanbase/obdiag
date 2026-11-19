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
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

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


if __name__ == "__main__":
    unittest.main()
