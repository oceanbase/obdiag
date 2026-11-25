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
@file: test_major_freeze_not_allow.py
@desc:
"""

import unittest
from unittest.mock import MagicMock

from plugins.rca.major_freeze_not_allow import MajorFreezeNotAllowScene
from src.handler.rca.rca_handler import RCA_ResultRecord


class TestMajorFreezeNotAllowScene(unittest.TestCase):
    def setUp(self):
        self.scene = MajorFreezeNotAllowScene()
        self.scene.tenant_name = "standby_tenant"
        self.scene.record = RCA_ResultRecord()
        self.scene.ob_connector = MagicMock()

    def _set_query_rows(self, rows):
        cursor = MagicMock()
        cursor.fetchall.return_value = rows
        self.scene.ob_connector.execute_sql_return_cursor_dictionary.return_value = cursor

    def test_error_pattern_matches_4217(self):
        self.assertTrue(self.scene.is_major_freeze_not_allow_error("ERROR 4217 (HY000): Major freeze not allowed now"))
        self.assertTrue(self.scene.is_major_freeze_not_allow_error("ret=-4217 OB_MAJOR_FREEZE_NOT_ALLOW"))
        self.assertFalse(self.scene.is_major_freeze_not_allow_error("ERROR 4002: lock wait timeout"))

    def test_standby_tenant_role_adds_expected_suggestion(self):
        self._set_query_rows([{"tenant_name": "standby_tenant", "tenant_role": "STANDBY"}])
        self.scene._check_tenant_role()
        self.assertIn("standby tenant", self.scene.record.suggest)
        self.assertIn("not allowed", self.scene.record.suggest)

    def test_primary_tenant_role_does_not_blame_standby(self):
        self._set_query_rows([{"TENANT_NAME": "primary_tenant", "TENANT_ROLE": "PRIMARY"}])
        self.scene.tenant_name = "primary_tenant"
        self.scene._check_tenant_role()
        self.assertIn("not STANDBY", self.scene.record.suggest)

    def test_missing_tenant_adds_clear_suggestion(self):
        self._set_query_rows([])
        self.scene._check_tenant_role()
        self.assertIn("tenant_name is correct", self.scene.record.suggest)

    def test_tenant_name_is_escaped_in_sql(self):
        self._set_query_rows([{"tenant_name": "tenant'1", "tenant_role": "STANDBY"}])
        self.scene.tenant_name = "tenant'1"
        self.scene._check_tenant_role()
        sql = self.scene.ob_connector.execute_sql_return_cursor_dictionary.call_args[0][0]
        self.assertIn("tenant''1", sql)


if __name__ == '__main__':
    unittest.main()
