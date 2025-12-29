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
@file: clog_disk_full.py
@desc: Check if there is a problem with clog disk full
"""

from src.handler.check.check_task import TaskBase


class ClogDiskFullTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: >= 4.0.0.0
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = '''
                SELECT GROUP_CONCAT(DISTINCT u.tenant_id) as tenant_ids 
                FROM oceanbase.gv$ob_units u 
                JOIN (
                    SELECT SVR_IP, SVR_PORT, TENANT_ID, value/100 AS value 
                    FROM oceanbase.GV$OB_PARAMETERS 
                    WHERE name = "log_disk_utilization_threshold"
                ) as c ON u.SVR_IP = c.SVR_IP AND u.SVR_PORT = c.SVR_PORT AND u.TENANT_ID = c.TENANT_ID 
                WHERE u.LOG_DISK_IN_USE > u.LOG_DISK_SIZE * c.value
            '''

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    tenant_ids = result[0].get('tenant_ids')
                    if tenant_ids:
                        self.report.add_critical("The following tenants have experienced clog disk full: {0}. Please check by obdiag rca --scene=clog_disk_full".format(tenant_ids))
                        self.stdio.warn("Found tenants with clog disk full: {0}".format(tenant_ids))
                    else:
                        self.stdio.verbose("No clog disk full issues found")
            except Exception as e:
                self.report.add_fail("Failed to check clog disk full: {0}".format(e))
                self.stdio.error("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "clog_disk_full",
            "info": "Check if there is a problem with clog disk full.",
        }


clog_disk_full = ClogDiskFullTask()
