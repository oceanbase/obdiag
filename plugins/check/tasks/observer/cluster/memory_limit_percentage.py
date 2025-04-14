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
@file: memory_limit_percentage.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class MemoryLimitPercentage(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            memory_limit_percentage_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * from oceanbase.GV$OB_PARAMETERS where name= \"memory_limit_percentage\";").fetchall()
            if len(memory_limit_percentage_data) < 1:
                return self.report.add_fail("get memory_limit_percentage data error")
            for memory_limit_percentage_one in memory_limit_percentage_data:
                # check VALUE is exist
                memory_limit_percentage_value = memory_limit_percentage_one.get("VALUE")
                if memory_limit_percentage_value is None:
                    return self.report.add_fail("get memory_limit_percentage value error")
                memory_limit_percentage_value = int(memory_limit_percentage_value)
                svr_ip = memory_limit_percentage_one.get("SVR_IP")

                # check DEFAULT_VALUE is exist
                default_value = memory_limit_percentage_data[0].get("default_value")
                if default_value is None:
                    default_value = 80
                if memory_limit_percentage_value != default_value:
                    self.report.add_warning("svr_ip: {2} memory_limit_percentage is {0}, default value is {1}".format(memory_limit_percentage_value, default_value, svr_ip))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "memory_limit_percentage", "info": "Check the percentage of total available memory size to total memory size in the system. Suggest keeping the default value of 80"}


memory_limit_percentage = MemoryLimitPercentage()
