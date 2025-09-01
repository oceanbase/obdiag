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
@file: memstore_usage.py
@desc:
"""
from decimal import Decimal

from src.handler.checker.check_task import TaskBase


class MemstoreUsage(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            sql = '''
                SELECT
                  m.tenant_id,
                  m.svr_ip,
                  m.svr_port,
                  round(m.active_span / 1024 / 1024 / 1024, 5) active_gb,
                  round(m.freeze_trigger / 1024 / 1024 / 1024, 5) trigger_gb,
                  round(m.memstore_used / 1024 / 1024 / 1024, 5) used_gb,
                  round(m.memstore_limit / 1024 / 1024 / 1024, 5) limit_gb,
                  m.freeze_cnt freeze_count
                FROM
                  oceanbase.__all_virtual_tenant_memstore_info m
                  INNER JOIN oceanbase.__all_tenant t ON t.tenant_id = m.tenant_id;
            '''
            memstore_usage_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if memstore_usage_data is None:
                return self.report.add_fail("get memstore usage  value error")
            for memstore_usage_one in memstore_usage_data:
                memstore_used_gb = Decimal(memstore_usage_one.get("used_gb"))
                memstore_limit_gb = Decimal(memstore_usage_one.get("limit_gb"))
                svr_ip = memstore_usage_one.get("svr_ip")
                tenant_id = memstore_usage_one.get("tenant_id")
                memstore_use = memstore_used_gb / memstore_limit_gb
                if memstore_use > 0.5:
                    self.report.add_warning("tenant_id: {2}, svr_ip: {1}. the utilization rate of memstore is {0}".format(memstore_use, svr_ip, tenant_id))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "memstore_useage", "info": "check memstore usage"}


memstore_usage = MemstoreUsage()
