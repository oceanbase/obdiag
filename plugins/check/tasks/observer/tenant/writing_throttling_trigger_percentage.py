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
@file: writing_throttling_trigger_percentage.py
@desc: Check writing_throttling_trigger_percentage configuration. Issue #758
"""

from src.handler.check.check_task import TaskBase


class WritingThrottlingTriggerPercentageTask(TaskBase):
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
                SELECT GROUP_CONCAT(DISTINCT TENANT_ID) as tenant_ids
                FROM oceanbase.GV$OB_PARAMETERS 
                WHERE name='writing_throttling_trigger_percentage' and VALUE=100
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    tenant_ids = result[0].get('tenant_ids')
                    if tenant_ids:
                        self.report.add_critical("There tenant's writing_throttling_trigger_percentage equal 100. It will causing memstore to full. tenant_id: {0}".format(tenant_ids))
            except Exception as e:
                self.report.add_fail("Failed to check writing_throttling_trigger_percentage: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "writing_throttling_trigger_percentage",
            "info": "Check writing_throttling_trigger_percentage configuration",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/758",
        }


writing_throttling_trigger_percentage = WritingThrottlingTriggerPercentageTask()
