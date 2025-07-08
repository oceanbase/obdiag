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
@time: 2025/07/08
@file: clog_hang.py
@desc:
"""
from decimal import Decimal

from src.handler.checker.check_task import TaskBase


class ClogHang(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            sql = '''
                select /*+ MONITOR_AGENT READ_CONSISTENCY(WEAK) */ svr_ip,is_disk_valid from oceanbase.__all_virtual_disk_stat where is_disk_valid = 0             
            '''
            clog_hang_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            print(clog_hang_data)
            if clog_hang_data is None:
                return self.report.add_fail("get clog hang value error")
            for clog_hang_one in clog_hang_data:
                self.report.add_critical("Disk failure detected on {0} ".format(clog_hang_one.get("svr_ip")))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "clog_hang", "info": "Disk failure"}


clog_hang = ClogHang()