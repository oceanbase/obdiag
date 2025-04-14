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
@time: 2025/04/8
@file: cpu_quota_concurrency.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class CpuQuotaConcurrency(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            cpu_quota_concurrency_data = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.GV$OB_PARAMETERS where Name=\"cpu_quota_concurrency\";").fetchall()

            for cpu_quota_concurrency_one in cpu_quota_concurrency_data:
                cpu_quota_concurrency_value = cpu_quota_concurrency_one.get("VALUE")
                tenant_id = cpu_quota_concurrency_one.get("TENANT_ID")
                svr_ip = cpu_quota_concurrency_one.get("SVR_IP")
                if cpu_quota_concurrency_value is None:
                    return self.report.add_fail("get cpu_quota_concurrency value error")
                cpu_quota_concurrency_value = int(cpu_quota_concurrency_value)
                if cpu_quota_concurrency_value > 4 or cpu_quota_concurrency_value < 2:
                    self.report.add_warning("tenant_id: {1}, svr_ip: {2}. cpu_quota_concurrency is {0}, recommended value is 2-4.".format(cpu_quota_concurrency_value, tenant_id, svr_ip))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "cpu_quota_concurrency", "info": "Check the maximum concurrency allowed for each CPU quota of the tenant, with a recommended value of 2-4. issue#738"}


cpu_quota_concurrency = CpuQuotaConcurrency()
