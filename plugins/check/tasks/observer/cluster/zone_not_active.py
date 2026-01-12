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
@file: zone_not_active.py
@desc: Check whether there is any zone not in the ACTIVE state
"""

from src.handler.check.check_task import TaskBase


class ZoneNotActiveTask(TaskBase):
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

            sql = 'select GROUP_CONCAT(DISTINCT ZONE) as not_active_zones from oceanbase.dba_ob_zones where STATUS<>"ACTIVE"'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    not_active = result[0].get('not_active_zones')
                    if not_active:
                        self.report.add_critical("There is {0} not_ACTIVE zone, please check as soon as possible.".format(not_active))
                        self.stdio.warn("Found not active zones: {0}".format(not_active))
                    else:
                        self.stdio.verbose("All zones are in ACTIVE state")
            except Exception as e:
                self.report.add_fail("Failed to check zone status: {0}".format(e))
                self.stdio.error("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "zone_not_active",
            "info": "Check whether there is any zone not in the ACTIVE state.",
        }


zone_not_active = ZoneNotActiveTask()
