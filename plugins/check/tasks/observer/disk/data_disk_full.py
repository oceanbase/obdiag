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
@time: 2025/06/03
@file: data_disk_full.py
@desc:
"""
from decimal import Decimal

from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class DataDiskFull(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return None
            if StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0"):
                pass
            else:
                return None

            sql = '''
                select /*+ READ_CONSISTENCY(WEAK)*/ svr_ip,ROUND(total_size/1024/1024/1024, 2) as total_size, ROUND(free_size/1024/1024/1024, 2) as free_size ,ROUND(allocated_size/1024/1024/1024, 2) as allocated_size from oceanbase.__all_virtual_disk_stat
            '''
            datadisk_full_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if datadisk_full_data is None:
                return self.report.add_fail("get data disk value error")
            for datadisk_full_one in datadisk_full_data:
                svr_ip = datadisk_full_one.get("svr_ip")
                total_size = Decimal(datadisk_full_one.get("total_size"))
                free_size = Decimal(datadisk_full_one.get("free_size"))
                used_size = total_size - free_size
                used_per = used_size / total_size
                if used_per > 0.85:
                    self.report.add_warning("svr_ip: {1}. data disk used is {0}".format(str(used_per), svr_ip))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "data_disk_full", "info": "Check data disk usage and alert when usage exceeds 85% threshold. issue #963"}


data_disk_full = DataDiskFull()
