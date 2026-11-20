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
@file: test_gather_plan_monitor_ps_sql.py
@desc: Unit tests for prepared-statement SQL handling in plan monitor.
"""

import unittest
from unittest.mock import MagicMock

from src.handler.gather.gather_plan_monitor import GatherPlanMonitorHandler


class TestGatherPlanMonitorPreparedStatementSQL(unittest.TestCase):
    def setUp(self):
        context = MagicMock()
        context.stdio = MagicMock()
        context.get_variable.return_value = None
        self.handler = GatherPlanMonitorHandler(context)

    def test_unfilled_ps_placeholder_is_not_eligible_for_explain(self):
        sql = "SELECT * FROM users WHERE id = ?"
        self.assertFalse(self.handler._sql_eligible_for_explain_extended(sql))

    def test_question_mark_literal_is_eligible_for_explain(self):
        sql = "SELECT * FROM users WHERE remark = '?'"
        self.assertTrue(self.handler._sql_eligible_for_explain_extended(sql))


if __name__ == '__main__':
    unittest.main()
