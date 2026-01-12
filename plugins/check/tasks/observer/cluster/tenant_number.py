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
@file: tenant_number.py
@desc: Check the number of tenant
"""

from src.handler.check.check_task import TaskBase


class TenantNumberTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = 'select count(0)/2 as tenant_count from oceanbase.__all_tenant where tenant_id>1000'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    tenant_count = float(result[0].get('tenant_count', 0))
                    if tenant_count > 100:
                        self.report.add_critical("The number of tenants: {0}. recommended: tenant_count < 50".format(int(tenant_count)))
                    elif tenant_count > 50:
                        self.report.add_warning("The number of tenants: {0}. recommended: tenant_count < 50".format(int(tenant_count)))
                    else:
                        self.stdio.verbose("Tenant count ({0}) is within acceptable range".format(int(tenant_count)))
            except Exception as e:
                self.report.add_fail("Failed to check tenant number: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "tenant_number", "info": "Check the number of tenant"}


tenant_number = TenantNumberTask()
