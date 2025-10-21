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
@time: 2025/04/23
@file: enable_lock_priority.py
@desc:
"""
from src.common.command import get_observer_version
from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class EnableLockPriority(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            if not super().check_ob_version_min("4.2.5.0"):
                return self.report.add_warning("this version:{} is not support this task".format(self.observer_version))
            observer_version = get_observer_version(self.context)
            if not (observer_version == "4.3.0.0" or StringUtils.compare_versions_greater("4.3.0.0", observer_version)):
                return

            enable_lock_priority_data = self.ob_connector.execute_sql_return_cursor_dictionary("SHOW PARAMETERS LIKE 'enable_lock_priority';").fetchall()
            if len(enable_lock_priority_data) < 1:
                return self.report.add_warning("not get enable_lock_priority data. maybe the version not support")
            for enable_lock_priority_one in enable_lock_priority_data:
                # check VALUE is exist
                enable_lock_priority_value = enable_lock_priority_one.get("value")
                if enable_lock_priority_value is None:
                    return self.report.add_fail("get enable_lock_priority value error")
                enable_lock_priority_value = enable_lock_priority_value
                tenant_id = enable_lock_priority_one.get("TENANT_ID")

                # check DEFAULT_VALUE is exist
                default_value = enable_lock_priority_data[0].get("default_value") or enable_lock_priority_data[0].get("DEFAULT_VALUE")
                if default_value is None:
                    default_value = "FALSE"
                if enable_lock_priority_value.lower() != default_value.lower():
                    self.report.add_warning("tenant_id: {2} enable_lock_priority is {0}, default value is {1}".format(enable_lock_priority_value, default_value, tenant_id))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "enable_lock_priority", "info": "After enabling, the activation of enable_lock_priority can also affect the performance of ddl/dml in daily use. Do not open it unless there is a need for lock free structural changes. issue #890"}


enable_lock_priority = EnableLockPriority()
