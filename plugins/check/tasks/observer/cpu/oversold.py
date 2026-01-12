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
@file: oversold.py
@desc: Check whether there is any observer have CPU oversold
"""

from src.handler.check.check_task import TaskBase


class OversoldTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = "SELECT GROUP_CONCAT(CONCAT(SVR_IP, ':', SVR_PORT) SEPARATOR ', ') AS IP_PORT_COMBINATIONS FROM oceanbase.GV$OB_SERVERS WHERE CPU_ASSIGNED > CPU_CAPACITY"

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    oversold = result[0].get('IP_PORT_COMBINATIONS')
                    if oversold:
                        self.report.add_warning("Some observers have CPU oversold. There are {0}".format(oversold))
            except Exception as e:
                self.report.add_fail("Failed to check CPU oversold: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "oversold", "info": "Check whether there is any observer have CPU oversold."}


oversold = OversoldTask()
