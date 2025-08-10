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
@time: 2025/05/6
@file: ob_enable_prepared_statement.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class OBEnablePreparedStatement(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            ob_enable_prepared_statement_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * FROM oceanbase.__all_virtual_sys_parameter_stat WHERE name='_ob_enable_prepared_statement';").fetchall()

            for ob_enable_prepared_statement_one in ob_enable_prepared_statement_data:
                ob_enable_prepared_statement_value = ob_enable_prepared_statement_one.get("value")
                svr_ip = ob_enable_prepared_statement_one.get("svr_ip")
                if ob_enable_prepared_statement_value is None:
                    return self.report.add_fail("get ob_enable_prepared_statement value error")
                if ob_enable_prepared_statement_value.lower() != "true":
                    self.report.add_warning("svr_ip: {1}. ob_enable_prepared_statement is {0}, recommended value is True.".format(ob_enable_prepared_statement_value, svr_ip))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "ob_enable_prepared_statement", "info": "Check whether prepared statement is enabled. It is recommended to enable it, especially the front end is JAVA application. issue#844"}


ob_enable_prepared_statement = OBEnablePreparedStatement()
