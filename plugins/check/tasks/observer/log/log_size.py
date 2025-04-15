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
@file: log_size.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class LogSize(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            log_size_data = self.ob_connector.execute_sql_return_cursor_dictionary("select * FROM oceanbase.GV$OB_PARAMETERS where name=\"max_syslog_file_count\";").fetchall()
            if len(log_size_data) < 1:
                return self.report.add_fail("get log_size data error")
            for log_size_one in log_size_data:
                # check VALUE is exist
                log_size_value = log_size_one.get("VALUE")
                if log_size_value is None:
                    return self.report.add_critical("get log_size value error")
                log_size_value = int(log_size_value)
                if log_size_value == 0 or log_size_value > 100:
                    pass
                else:
                    self.report.add_warning("log_size is {0}, please check. the recommended is 0 or over 100".format(log_size_value))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "log_size", "info": "check obcluster max_syslog_file_count is 0 or over 100"}


log_size = LogSize()
