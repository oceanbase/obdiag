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
@file: sysbench_test_cluster_parameters.py
@desc: Check cluster parameters for sysbench
"""

from src.handler.check.check_task import TaskBase


class SysbenchTestClusterParametersTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check enable_record_trace_log
            sql = 'select count(0) as cnt from oceanbase.GV$OB_PARAMETERS where Name="enable_record_trace_log" and VALUE<>"False"'
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if result and result[0].get('cnt', 0) > 0:
                self.report.add_critical("cluster's enable_record_trace_log is true, need to change False")

            # Check enable_perf_event
            sql = 'select count(0) as cnt from oceanbase.GV$OB_PARAMETERS where Name="enable_perf_event" and VALUE<>"False"'
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if result and result[0].get('cnt', 0) > 0:
                self.report.add_critical("cluster's enable_perf_event is true, need to change False")

            # Check enable_sql_audit
            sql = 'select count(0) as cnt from oceanbase.GV$OB_PARAMETERS where Name="enable_sql_audit" and VALUE<>"False"'
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if result and result[0].get('cnt', 0) > 0:
                self.report.add_critical("cluster's enable_sql_audit is true, need to change False")

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_test_cluster_parameters", "info": "Check cluster parameters for sysbench."}


sysbench_test_cluster_parameters = SysbenchTestClusterParametersTask()
