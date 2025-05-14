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
@file: server_permanent_offline_time.py
@desc:
"""
from src.handler.checker.check_task import TaskBase


class ServerPermanentOfflineTime(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        self.expected_value = "3600s"  # expected_value
        self.param_name = "server_permanent_offline_time"  # param_name

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")

            sql = "select * from oceanbase.GV$OB_PARAMETERS where name='{0}';".format(self.param_name)
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not result:
                return self.report.add_warning("can't find this param_name")

            non_compliant_nodes = []

            for data_item in result:
                svr_ip = data_item['SVR_IP']
                value = data_item['VALUE']
                if value != self.expected_value:
                    non_compliant_nodes.append(svr_ip)

            if non_compliant_nodes:
                nodes_str = ", ".join(non_compliant_nodes)
                return self.report.add_warning(f"this server's server_permanent_offline_time!=3600s, please check: {nodes_str}")

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "server_permanent_offline_time", "info": "check the cluster parameters of server_permanent_offline_time . issue #816"}


server_permanent_offline_time = ServerPermanentOfflineTime()
