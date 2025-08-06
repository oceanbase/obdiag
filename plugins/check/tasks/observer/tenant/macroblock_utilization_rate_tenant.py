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
@time: 2025/04/30
@file: macroblock_utilization_rate_tenant.py
@desc:
"""
from src.handler.checker.check_task import TaskBase


class MacroblockUtilizationRateTenant(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    # [0,0.5]
    def execute(self):
        try:

            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")
            if super().check_ob_version_min("4.0.0.0") is False:
                return
            sql = '''
select /*+READ_CONSISTENCY(WEAK)*/ b.tenant_id, d.tenant_name, sum(c.occupy_size) / 1024 / 1024 / 1024 as data_size_gb, count(distinct(macro_block_idx)) * 2 / 1024 as required_size_gb from oceanbase.__all_virtual_table b inner join (select svr_ip, svr_port, tenant_id, row_count, tablet_id, occupy_size, macro_block_idx from oceanbase.__all_virtual_tablet_sstable_macro_info group by svr_ip, svr_port, tenant_id, tablet_id, macro_block_idx) c on b.tenant_id = c.tenant_id and b.tablet_id = c.tablet_id left join oceanbase.dba_ob_tenants d on d.tenant_id = b.tenant_id where b.tenant_id <> 1 group by tenant_id;
       '''
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            for row in result:
                tenant_name = row.get("tenant_name") or row.get("TENANT_NAME")
                data_size_gb = row.get("data_size_gb") or row.get("DATA_SIZE_GB")
                required_size_gb = row.get("required_size_gb") or row.get("REQUIRED_SIZE_GB")

                if required_size_gb > 1:
                    ratio = round(data_size_gb / required_size_gb, 2)
                    if ratio > 0.5:
                        self.report.add_warning("tenant: {0} ratio: {1}, dataSize: {2}G, requiredSize: {3}G. need major".format(tenant_name, ratio, round(data_size_gb, 2), round(required_size_gb, 2)))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "macroblock_utilization_rate_tenant",
            "info": "Check if the ratio of actual data volume to actual disk usage is within a certain range for all tenants in the OceanBase cluster. OceanBase stores data in macroblocks. Each macroblock may not be fully utilized for efficiency. If the ratio of actual data volume to actual disk usage is too low, full consolidation should be performed to improve disk utilization. issue #847",
        }


macroblock_utilization_rate_tenant = MacroblockUtilizationRateTenant()
