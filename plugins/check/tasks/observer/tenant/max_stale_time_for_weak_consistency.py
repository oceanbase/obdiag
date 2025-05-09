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
@file: max_stale_time_for_weak_consistency.py
@desc:
"""
from src.common.command import get_observer_version
from src.handler.checker.check_task import TaskBase


class MaxStaleTimeForWeakConsistency(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            obversion = get_observer_version(self.context)
            if super().check_ob_version_min("4.0.0.0"):
                pass
            else:
                return self.report.add_warning("the task is not supported in this version: {0}".format(obversion))
            # get all max_stale_time_for_weak_consistency
            sql = """
select * from oceanbase.GV$OB_PARAMETERS  where name="max_stale_time_for_weak_consistency";
            """
            max_stale_time_for_weak_consistency_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if len(max_stale_time_for_weak_consistency_data) == 0:
                return
            for max_stale_time_for_weak_consistency_one in max_stale_time_for_weak_consistency_data:
                tenant_id = max_stale_time_for_weak_consistency_one.get("TENANT_ID")
                default = "5s"
                value = max_stale_time_for_weak_consistency_one.get("VALUE")
                if value.lower() != default:
                    self.report.warning("tenant_id: {0} max_stale_time_for_weak_consistency: {1} is not default value: {2}".format(tenant_id, value, default))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "max_stale_time_for_weak_consistency", "info": "Confirm if the configuration item is the default value. issue#850"}


max_stale_time_for_weak_consistency = MaxStaleTimeForWeakConsistency()
