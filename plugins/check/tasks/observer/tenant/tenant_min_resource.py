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
@file: tenant_min_resource.py
@desc: Check tenant resource pool configuration, if the cpu or memory is less than 2C4G
"""

from src.handler.check.check_task import TaskBase


class TenantMinResourceTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = """
                SELECT GROUP_CONCAT(DISTINCT T4.TENANT_ID) as tenant_ids
                FROM oceanbase.DBA_OB_RESOURCE_POOLS T1 
                JOIN oceanbase.DBA_OB_UNIT_CONFIGS T2 ON T1.UNIT_CONFIG_ID = T2.UNIT_CONFIG_ID 
                JOIN oceanbase.DBA_OB_UNITS T3 ON T1.RESOURCE_POOL_ID = T3.RESOURCE_POOL_ID 
                JOIN oceanbase.DBA_OB_TENANTS T4 ON T1.TENANT_ID = T4.TENANT_ID 
                WHERE T4.TENANT_ID > 1 AND (T2.MAX_CPU < 2 OR ROUND(T2.MEMORY_SIZE/1024/1024/1024,2) < 4)
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    tenant_ids = result[0].get('tenant_ids')
                    if tenant_ids:
                        self.report.add_critical("There tenant resource pool configuration is less than 2C4G, please check it. tenant_id: {0}".format(tenant_ids))
            except Exception as e:
                self.report.add_fail("Failed to check tenant min resource: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "tenant_min_resource", "info": "Check tenant resource pool configuration, if the cpu or memory is less than 2C4G."}


tenant_min_resource = TenantMinResourceTask()
