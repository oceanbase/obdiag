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
@time: 2024/01/16
@file: scene.py
@desc:
"""

import unittest
from unittest.mock import MagicMock
from common.scene import filter_by_version


class TestFilterByVersion(unittest.TestCase):
    def setUp(self):
        self.stdio = MagicMock()
        self.scene = [{"version": "[1.0.0,2.0.0)"}, {"version": "(1.0.0,2.0.0]"}]
        self.cluster = {"version": "1.5.0"}

    def test_filter_by_version_with_valid_version(self):
        # Test case where cluster version is within the range specified in the scene
        result = filter_by_version(self.scene, self.cluster, self.stdio)
        self.assertEqual(result, 1)

    def test_filter_by_version_with_invalid_version(self):
        # Test case where cluster version is outside the range specified in the scene
        self.cluster["version"] = "0.5.0"
        result = filter_by_version(self.scene, self.cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_filter_by_version_with_wildcard_min_version(self):
        # Test case where min version is wildcard (*) and cluster version is valid
        self.scene[0]["version"] = "[*,2.0.0)"
        result = filter_by_version(self.scene, self.cluster, self.stdio)
        self.assertEqual(result, 1)

    def test_filter_by_version_with_wildcard_max_version(self):
        # Test case where max version is wildcard (*) and cluster version is valid
        self.scene[1]["version"] = "(1.0.0,*]"
        result = filter_by_version(self.scene, self.cluster, self.stdio)
        self.assertEqual(result, 2)

    def test_filter_by_version_with_non_string_version(self):
        # Test case where version is not a string
        self.scene[0]["version"] = str(1.0)
        with self.assertRaises(Exception) as context:
            filter_by_version(self.scene, self.cluster, self.stdio)
        self.assertTrue(
            "filter_by_version steps_version Exception" in str(context.exception)
        )

    def test_filter_by_version_no_version_in_cluster(self):
        # Test case where version is not specified in the cluster
        del self.cluster["version"]
        result = filter_by_version(self.scene, self.cluster, self.stdio)
        self.assertEqual(result, 0)

    def test_filter_by_version_no_version_in_steps(self):
        # Test case where no version is specified in any steps
        self.scene = [{"some_key": "some_value"}]
        result = filter_by_version(self.scene, self.cluster, self.stdio)
        self.assertEqual(result, -1)


if __name__ == "__main__":
    unittest.main()
