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
@file: freeze_trigger_percentage.py
@desc:
"""
from src.handler.checker.check_task import TaskBase


class FreezeTriggerPercentage(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        self.expected_value = 20  # expected_value
        self.param_name = "freeze_trigger_percentage"  # param_name

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")
            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("this version:{} is not support this task".format(self.observer_version))
            sql = "select * from oceanbase.GV$OB_PARAMETERS where name='{0}';".format(self.param_name)
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not result:
                return self.report.add_warning("can't find this param_name")

            tenant_ip_map = {}

            for data_item in result:
                svr_ip = str(data_item['SVR_IP'])
                value = data_item['VALUE']
                tenant_id = str(data_item['TENANT_ID'])

                try:
                    if int(value) != self.expected_value:
                        if tenant_id not in tenant_ip_map:
                            tenant_ip_map[tenant_id] = []
                        tenant_ip_map[tenant_id].append(svr_ip)
                except (ValueError, TypeError):
                    continue

            if tenant_ip_map:
                # 构建每行一个租户的报告信息
                warning_msgs = []
                for tenant_id, ips in tenant_ip_map.items():
                    unique_ips = sorted(list(set(ips)))  # 去重并排序
                    warning_msgs.append(f"Tenant {tenant_id} has incorrect freeze_trigger_percentage on nodes: {', '.join(unique_ips)}")

                return self.report.add_warning("Found incorrect freeze_trigger_percentage(!=20):\n" + "\n".join(warning_msgs))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "freeze_trigger_percentage", "info": "It is recommended that the server keep the default configuration of 20. issue #795"}


freeze_trigger_percentage = FreezeTriggerPercentage()
