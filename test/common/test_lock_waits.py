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

"""Regression tests for OceanBase row lock waits."""

from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from plugins.check.tasks.observer.cluster.tenant_locks import TenantLocks
from plugins.rca.lock_conflict import LockConflictScene


def query_lock_waits(sql):
    if '"TR"' not in sql:
        return Mock(**{"fetchall.return_value": []})
    if "count(tenant_id)" in sql:
        return Mock(**{"fetchall.return_value": [{"tenant_id": 1004, "locks_count": 5001}]})
    return Mock(**{"fetchall.return_value": [{"TYPE": "TR", "ID1": "101", "TRANS_ID": "102"}]})


class TestLockWaits(unittest.TestCase):
    def setUp(self):
        self.scene = LockConflictScene()
        self.scene.observer_version = "4.2.5.7"
        self.scene.stdio = Mock()
        self.scene.input_parameters = {}
        self.scene.ob_connector = Mock()
        self.scene.Result = SimpleNamespace(records=[])

    def test_rca_reports_row_lock_wait_sessions(self):
        def execute_query(sql):
            if "GV$OB_LOCKS" in sql:
                return query_lock_waits(sql)
            if 'TX_ID="101"' in sql:
                return Mock(**{"fetchall.return_value": [{"SESSION_ID": 201}]})
            if 'TX_ID="102"' in sql:
                return Mock(**{"fetchall.return_value": [{"SESSION_ID": 202}]})
            self.assertIn("enable_sql_audit", sql)
            return Mock(**{"fetchone.return_value": {"value": "false"}})

        self.scene.ob_connector.execute_sql_return_cursor_dictionary.side_effect = execute_query

        self.scene.execute()

        record = self.scene.Result.records[0]
        self.assertIn("Holding lock session_id: 201", record.records)
        self.assertIn("Waiting lock session_id: 202", record.records)

    def test_rca_before_4_2_uses_legacy_lock_waits(self):
        self.scene.observer_version = "4.1.0.0"
        query = self.scene.ob_connector.execute_sql_return_cursor_dictionary
        query.return_value.fetchall.return_value = [{"block_session_id": 201}]

        self.scene.execute()

        self.assertIn("__all_virtual_lock_wait_stat", query.call_args.args[0])
        self.assertIn("Block session_id: 201", self.scene.Result.records[0].records)

    def test_tenant_check_counts_row_lock_waits(self):
        task = TenantLocks()
        task.observer_version = "4.2.5.7"
        task.stdio = Mock()
        task.report = Mock()
        task.ob_connector = Mock()
        task.ob_connector.execute_sql_return_cursor_dictionary.side_effect = query_lock_waits

        task.execute()

        task.report.add_critical.assert_called_once_with("tenant: 1004. The number of lock waits for the tenant is 5001")
