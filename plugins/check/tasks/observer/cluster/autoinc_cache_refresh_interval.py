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
@file: autoinc_cache_refresh_interval.py
@desc:
"""
import re
from src.handler.checker.check_task import TaskBase


class AutoincCacheRefreshInterval(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        self.expected_value = "3600s"  # expected_value
        self.param_name = "autoinc_cache_refresh_interval"  # param_name

    def execute(self):
        try:
            # check autoinc_cache_refresh_interval
            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")
            sql = "select * from oceanbase.GV$OB_PARAMETERS where name='{0}';".format(self.param_name)
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if not result:
                return self.report.add_warning("can't find this  param_name")
            # gather svr_ip
            non_compliant_nodes = []

            for data_item in result:
                svr_ip = data_item['SVR_IP']
                value = data_item['VALUE']
                num_value = int(re.search(r'\d+', value).group())
                expected_num = int(re.search(r'\d+', self.expected_value).group())
                if num_value < expected_num:
                    non_compliant_nodes.append(svr_ip)
            if non_compliant_nodes:
                nodes_str = ", ".join(non_compliant_nodes)
                return self.report.add_warning(f"this server's autoinc_cache_refresh_interval<1h, please check: {nodes_str}")
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "autoinc_cache_refresh_interval", "info": "Check the refresh interval of the auto-increment column cache. Frequent refreshes will affect the system performance. It is recommended to set it to more than 1 hour. . issue #817"}

    def get_scene_info(self):
        pass


autoinc_cache_refresh_interval = AutoincCacheRefreshInterval()
