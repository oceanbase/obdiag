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
@time: 2024/12/29
@file: sysbench_test_sql_net_thread_count.py
@desc: Check cluster info about sql_net_thread_count for sysbench
"""

from src.handler.check.check_task import TaskBase


class SysbenchTestSqlNetThreadCountTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check for inconsistent sql_net_thread_count in same zone
            sql = """SELECT GROUP_CONCAT(DISTINCT ZONE) as zones
                     FROM oceanbase.GV$OB_PARAMETERS t1
                     WHERE Name = 'sql_net_thread_count'
                     AND Value != 0
                     AND EXISTS (SELECT 1
                                 FROM oceanbase.GV$OB_PARAMETERS t2
                                 WHERE t2.zone = t1.zone
                                 AND t2.Value != t1.Value)"""

            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if result and result[0].get('zones'):
                self.report.add_critical("There is an observer whose sql_net_thread_count is not consistent in zone: {0}".format(result[0].get('zones')))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_test_sql_net_thread_count", "info": "Check cluster info about sql_net_thread_count for sysbench."}


sysbench_test_sql_net_thread_count = SysbenchTestSqlNetThreadCountTask()
