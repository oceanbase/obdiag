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
@file: resource_limit_max_session_num.py
@desc: Check if the _resource_limit_max_session_num parameter has been changed
"""

from src.handler.check.check_task import TaskBase


class ResourceLimitMaxSessionNumTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = "SELECT GROUP_CONCAT(DISTINCT TENANT_ID) as tenant_ids FROM oceanbase.GV$OB_PARAMETERS WHERE NAME LIKE '_resource_limit_max_session_num' and VALUE<>0"

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    tenant_ids = result[0].get('tenant_ids')
                    if tenant_ids:
                        self.report.add_critical("_resource_limit_max_session_num is not recommended. Set it within the range of 0. It may result in Too many connections reporting errors. tenant_id: {0}".format(tenant_ids))
            except Exception as e:
                self.report.add_fail("Failed to check resource_limit_max_session_num: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "resource_limit_max_session_num", "info": "Check if the _resource_limit_max_session_num parameter has been changed."}


resource_limit_max_session_num = ResourceLimitMaxSessionNumTask()
