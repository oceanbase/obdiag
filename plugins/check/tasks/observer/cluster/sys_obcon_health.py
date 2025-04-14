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
@file: sys_obcon_health.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class SysObconHealth(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            pass_tag = True
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            build_version_data = self.ob_connector.execute_sql_return_cursor_dictionary("select version() ").fetchall()
            if len(build_version_data) != 1:
                return self.report.add_critical("can not build sys")
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "sys_obcon_health", "info": "Check if the cluster is connected by connecting to the sys tenant. issue#872"}


sys_obcon_health = SysObconHealth()
