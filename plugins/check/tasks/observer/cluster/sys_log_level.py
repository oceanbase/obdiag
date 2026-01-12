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
@file: sys_log_level.py
@desc: Check sys_log_level parameter
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class SysLogLevelTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = 'SELECT value FROM oceanbase.__all_virtual_sys_parameter_stat where name like "%syslog_level%"'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    sys_log_level = result[0].get('value', '').strip()

                    # Check based on version
                    if super().check_ob_version_min("4.0.0.0"):
                        # 4.x recommended: WDIAG
                        if sys_log_level != "WDIAG":
                            self.report.add_warning("sys_log_level: {0}. on 4.x, the recommended value for sys_log_level is WDIAG".format(sys_log_level))
                    else:
                        # 3.x recommended: INFO
                        if sys_log_level != "INFO":
                            self.report.add_warning("sys_log_level: {0}. on 3.x, the recommended value for sys_log_level is INFO".format(sys_log_level))
            except Exception as e:
                self.report.add_fail("Failed to check sys_log_level: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sys_log_level", "info": "Check sys_log_level parameter."}


sys_log_level = SysLogLevelTask()
