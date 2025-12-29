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
@file: ob_enable_plan_cache_bad_version.py
@desc: Check ob_enable_plan_cache on version [4.1.0.0,4.1.0.1]
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class ObEnablePlanCacheBadVersionTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: [4.1.0.0, 4.1.0.1]
            if not super().check_ob_version_min("4.1.0.0"):
                self.stdio.verbose("Version < 4.1.0.0, skip check")
                return

            if self.observer_version and StringUtils.compare_versions_greater(self.observer_version, "4.1.0.1"):
                self.stdio.verbose("Version > 4.1.0.1, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = 'select name from oceanbase.__all_virtual_tenant_parameter_stat where name like "%ob_enable_plan_cache%" and value like "%true%"'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    self.report.add_critical("On this version, ob_enable_plan_cache suggestion to close")
            except Exception as e:
                self.report.add_fail("Failed to check ob_enable_plan_cache: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "ob_enable_plan_cache_bad_version", "info": "Check ob_enable_plan_cache on version [4.1.0.0,4.1.0.1]."}


ob_enable_plan_cache_bad_version = ObEnablePlanCacheBadVersionTask()
