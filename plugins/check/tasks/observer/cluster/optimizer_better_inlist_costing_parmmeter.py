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
@file: optimizer_better_inlist_costing_parmmeter.py
@desc: Check if the _optimizer_better_inlist_costing parameter is enabled on bad versions
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class OptimizerBetterInlistCostingParmmeterTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check version [4.1.0.0,4.1.0.2] or [4.2.0.0,4.2.0.0]
            version_match = False
            if super().check_ob_version_min("4.1.0.0") and not StringUtils.compare_versions_greater(self.observer_version, "4.1.0.2"):
                version_match = True
            elif self.observer_version == "4.2.0.0":
                version_match = True

            if not version_match:
                self.stdio.verbose("Version not in affected range, skip check")
                return

            sql = 'select name from oceanbase.__all_virtual_tenant_parameter_stat where name like "%_optimizer_better_inlist_costing%" and value like "%true%"'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    self.report.add_critical("_optimizer_better_inlist_costing need close. Triggering this issue can lead to correctness issues, causing random errors or core issues.")
            except Exception as e:
                self.report.add_fail("Failed to check optimizer_better_inlist_costing: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "optimizer_better_inlist_costing_parmmeter", "info": "Check if the _optimizer_better_inlist_costing parameter is enabled on bad versions."}


optimizer_better_inlist_costing_parmmeter = OptimizerBetterInlistCostingParmmeterTask()
