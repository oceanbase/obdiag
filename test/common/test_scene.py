#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
@time: 2024/8/6
@file: test_scene.py
@desc: 为scene模块中filter_by_version和get_version_by_type函数进行单元测试
"""
import unittest
from unittest.mock import MagicMock, patch
from common.scene import *


class TestFilterByVersion(unittest.TestCase):
    def setUp(self):
        self.stdio = MagicMock()
        StringUtils.compare_versions_greater = MagicMock()
        self.context = MagicMock()
        self.context.stdio = MagicMock()

    def test_no_version_in_cluster(self):
        scene = [{"version": "[1.0,2.0]"}]
        cluster = {}
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_empty_version_in_cluster(self):
        scene = [{"version": "[1.0,2.0]"}]
        cluster = {"version": ""}
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_version_not_string(self):
        scene = [{"version": 123}]
        cluster = {"version": "1.5"}
        with self.assertRaises(Exception):
            filter_by_version(scene, cluster, self.stdio)

    def test_version_match_min(self):
        scene = [{"version": "[1.0,2.0]"}]
        cluster = {"version": "1.0"}
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_version_match_max(self):
        scene = [{"version": "[1.0,2.0]"}]
        cluster = {"version": "2.0"}
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_version_in_range(self):
        scene = [{"version": "[1.0,2.0]"}]
        cluster = {"version": "1.5"}
        StringUtils.compare_versions_greater.side_effect = [True, True]
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_version_out_of_range(self):
        scene = [{"version": "[1.0,2.0]"}, {"version": "[2.0,3.0]"}]
        cluster = {"version": "2.5"}
        StringUtils.compare_versions_greater.side_effect = [False, True, True, True]
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 1)

    def test_no_version_in_steps(self):
        scene = [{}]
        cluster = {"version": "1.0"}
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_no_matching_version(self):
        scene = [{"version": "[1.0,2.0]"}, {"version": "[2.0,3.0]"}]
        cluster = {"version": "3.5"}
        StringUtils.compare_versions_greater.return_value = False
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, -1)

    def test_wildcard_min_version(self):
        scene = [{"version": "[*,2.0]"}]
        cluster = {"version": "1.0"}
        StringUtils.compare_versions_greater.side_effect = [True, True]
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_wildcard_max_version(self):
        scene = [{"version": "[1.0,*]"}]
        cluster = {"version": "3.0"}
        StringUtils.compare_versions_greater.side_effect = [True, True]
        result = filter_by_version(scene, cluster, self.stdio)
        self.assertEqual(result, 0)

    @patch('common.scene.get_observer_version')
    def test_get_observer_version(self, mock_get_observer_version):
        mock_get_observer_version.return_value = "1.0.0"
        result = get_version_by_type(self.context, "observer")
        self.assertEqual(result, "1.0.0")
        mock_get_observer_version.assert_called_once_with(self.context)

    @patch('common.scene.get_observer_version')
    def test_get_other_version(self, mock_get_observer_version):
        mock_get_observer_version.return_value = "2.0.0"
        result = get_version_by_type(self.context, "other")
        self.assertEqual(result, "2.0.0")
        mock_get_observer_version.assert_called_once_with(self.context)

    @patch('common.scene.get_observer_version')
    def test_get_observer_version_fail(self, mock_get_observer_version):
        mock_get_observer_version.side_effect = Exception("Observer error")
        with self.assertRaises(Exception) as context:
            get_version_by_type(self.context, "observer")
        self.assertIn("can't get observer version", str(context.exception))
        self.context.stdio.warn.assert_called_once()

    @patch('common.scene.get_obproxy_version')
    def test_get_obproxy_version(self, mock_get_obproxy_version):
        mock_get_obproxy_version.return_value = "3.0.0"
        result = get_version_by_type(self.context, "obproxy")
        self.assertEqual(result, "3.0.0")
        mock_get_obproxy_version.assert_called_once_with(self.context)

    def test_unsupported_type(self):
        with self.assertRaises(Exception) as context:
            get_version_by_type(self.context, "unsupported")
        self.assertIn("No support to get the version", str(context.exception))

    @patch('common.scene.get_observer_version')
    def test_general_exception_handling(self, mock_get_observer_version):
        mock_get_observer_version.side_effect = Exception("Unexpected error")
        with self.assertRaises(Exception) as context:
            get_version_by_type(self.context, "observer")
        self.assertIn("can't get observer version", str(context.exception))
        self.context.stdio.exception.assert_called_once()


if __name__ == '__main__':
    unittest.main()
