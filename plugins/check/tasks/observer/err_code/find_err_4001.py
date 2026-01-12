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
@file: find_err_4001.py
@desc: Check whether Error 4001 is reported when enable_sql_audit is set to True
"""

from src.handler.check.check_task import TaskBase


class FindErr4001Task(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check if sql_audit is enabled
            sql_audit_check = 'select count(0) as cnt from oceanbase.GV$OB_PARAMETERS where NAME="enable_sql_audit" and VALUE<>"True"'
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_audit_check).fetchall()
            if result and result[0].get('cnt', 0) > 0:
                self.report.add_critical("Unable to proceed because enable_sql_audit is set to False")
                return

            # Check for error 4001
            sql = 'select count(0) as err_count from oceanbase.GV$OB_SQL_AUDIT where RET_CODE="-4001"'
            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    err_count = result[0].get('err_count', 0)
                    if err_count > 0:
                        self.report.add_critical("number of sql_error_4001 is {0}".format(err_count))
            except Exception as e:
                self.report.add_fail("Failed to check error 4001: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "find_err_4001", "info": "Check whether Error 4001 is reported when enable_sql_audit is set to True."}


find_err_4001 = FindErr4001Task()
