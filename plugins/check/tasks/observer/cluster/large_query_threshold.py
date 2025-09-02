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
@file: large_query_threshold.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class LargeQueryThreshold(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("this version:{} is not support this task".format(self.observer_version))
            large_query_threshold_data = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.GV$OB_PARAMETERS where Name=\"large_query_threshold\";").fetchall()
            for large_query_threshold_one in large_query_threshold_data:
                large_query_threshold_value = large_query_threshold_one.get("VALUE")
                svr_ip = large_query_threshold_one.get("SVR_IP")
                if large_query_threshold_value is None:
                    return self.report.add_fail("get large_query_threshold value error")
                if large_query_threshold_value != "5s":
                    self.report.add_warning("svr_ip: {1}. large_query_threshold is {0}, recommended value is 5s.".format(large_query_threshold_value, svr_ip))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "large_query_threshold",
            "info": "Check the threshold for query execution time. Requests that exceed the time limit may be paused and automatically judged as large queries after the pause, and the large query scheduling strategy will be executed. Recommended setting is 5s. issue#859",
        }


large_query_threshold = LargeQueryThreshold()
