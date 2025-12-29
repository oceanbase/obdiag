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
@file: task_opt_stat.py
@desc: Check task opt stat gather history
"""

from src.handler.check.check_task import TaskBase


class TaskOptStatTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.2.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = """
                SELECT GROUP_CONCAT(DISTINCT TENANT_ID) as failed_tenant_ids
                FROM oceanbase.__all_tenant t
                WHERE NOT EXISTS(
                    SELECT 1
                    FROM oceanbase.__all_virtual_task_opt_stat_gather_history h
                    WHERE TYPE = 1
                    AND start_time > date_sub(now(), interval 1 day)
                    AND h.tenant_id = t.tenant_id
                )
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    tenant_ids = result[0].get('failed_tenant_ids')
                    if tenant_ids:
                        self.report.add_critical("The collection of statistical information related to tenants has issues. Please check the tenant_ids: {0}".format(tenant_ids))
            except Exception as e:
                self.report.add_fail("Failed to check task_opt_stat: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "task_opt_stat", "info": "Check task opt stat gather history."}


task_opt_stat = TaskOptStatTask()
