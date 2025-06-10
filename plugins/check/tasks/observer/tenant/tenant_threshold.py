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
@time: 2025/06/03
@file: data_disk_full.py
@desc:
"""

from collections import defaultdict
from src.common.tool import StringUtils
from src.handler.checker.check_exception import CheckException
from src.handler.checker.check_task import TaskBase


class TenantThreshold(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            sql = '''
                select /*+read_consistency(weak)*/ tenant_name, tenant_id, stat_id, value,svr_ip from oceanbase.gv$sysstat, oceanbase.__all_tenant where stat_id IN (140006, 140005) and (con_id > 1000 or con_id = 1) and __all_tenant.tenant_id = gv$sysstat.con_id;
            '''
            tenant_threshold_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if tenant_threshold_data is None:
                return self.report.add_fail("get data disk value error")
            # 分组
            grouped = defaultdict(list)
            for item in tenant_threshold_data:
                key = (item['tenant_id'], item['svr_ip'])
                grouped[key].append(item)

            for (tenant, svr), items in grouped.items():
                # 提取stat_id对应的value
                stat_140005 = None
                stat_140006 = None
                for item in items:
                    if item['stat_id'] == 140005:
                        stat_140005 = item['value']
                    elif item['stat_id'] == 140006:
                        stat_140006 = item['value']

                        # 检查是否都存在
                if stat_140005 is not None and stat_140006 is not None:
                    try:
                        result = stat_140006 / stat_140005
                        if result > 0.95:
                            self.report.add_warning("tenant_id:{0},svr_ip:{1},The tenant's thread utilization rate is {2}, exceeding the threshold of 0.95.".format(tenant, svr, result))
                    except ZeroDivisionError:
                        # 处理除零错误，比如记录错误或跳过
                        pass
                else:
                    # 处理缺少stat_id的情况
                    pass
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "tenant threshold", "info": ""}


tenant_threshold = TenantThreshold()
