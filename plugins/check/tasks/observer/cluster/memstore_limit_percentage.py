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
@time: 2025/04/8
@file: memstore_limit_percentage.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class MemstoreLimitPercentage(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            memstore_limit_percentage_data = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.GV$OB_PARAMETERS where Name=\"memstore_limit_percentage\";").fetchall()

            for memstore_limit_percentage_one in memstore_limit_percentage_data:
                memstore_limit_percentage_value = memstore_limit_percentage_one.get("VALUE")
                svr_ip = memstore_limit_percentage_one.get("SVR_IP")
                if memstore_limit_percentage_value is None:
                    return self.report.add_fail("get memstore_limit_percentage value error")
                memstore_limit_percentage_value = int(memstore_limit_percentage_value)
                if memstore_limit_percentage_value != 0 and memstore_limit_percentage_value != 50:
                    self.report.add_warning("svr_ip: {1}. memstore_limit_percentage is {0}, recommended value is 0 or 50.".format(memstore_limit_percentage_value, svr_ip))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "memstore_limit_percentage", "info": "Check the percentage of memory used by tenants using memstore to their total available memory. Suggest keeping the default value of 50. issue#871"}


memstore_limit_percentage = MemstoreLimitPercentage()
