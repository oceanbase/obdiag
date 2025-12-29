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
@file: sysbench_test_cpu_quota_concurrency.py
@desc: Check cluster info about cpu_quota_concurrency for sysbench
"""

from src.handler.check.check_task import TaskBase


class SysbenchTestCpuQuotaConcurrencyTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check cpu_quota_concurrency
            sql = """SELECT GROUP_CONCAT(DISTINCT tenant_id) as tenant_ids
                     FROM oceanbase.__all_virtual_tenant_parameter_info
                     WHERE name = 'cpu_quota_concurrency'
                     AND value NOT IN (2, 4) and tenant_id>1000"""

            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if result and result[0].get('tenant_ids'):
                self.report.add_critical("cpu_quota_concurrency should equal to 2 or 4. the tenant_id is {0}".format(result[0].get('tenant_ids')))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_test_cpu_quota_concurrency", "info": "Check cluster info about cpu_quota_concurrency for sysbench."}


sysbench_test_cpu_quota_concurrency = SysbenchTestCpuQuotaConcurrencyTask()
