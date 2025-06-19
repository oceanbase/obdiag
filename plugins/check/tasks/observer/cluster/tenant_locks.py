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
@file: tenant_locks.py
@desc:
"""

from src.handler.checker.check_task import TaskBase
from src.common.tool import StringUtils
from src.handler.checker.check_exception import CheckException


class TenantLocks(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise CheckException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            self.stdio.error("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
            raise CheckException("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            sql = '''
                select tenant_id,count(tenant_id) as locks_count from oceanbase.GV$OB_LOCKS where BLOCK=1 and TYPE="TX" group by tenant_id;
            '''
            tenant_locks_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if tenant_locks_data is None:
                return self.report.add_fail("get tenant locks value error")
            for tenant_locks_one in tenant_locks_data:
                tenant = tenant_locks_one.get("tenant_id")
                locks_count = tenant_locks_one.get("locks_count")
                if tenant is None:
                    return
                if locks_count > 5000:
                    self.report.add_critical("tenant: {1}. The number of lock waits for the tenant is {0}".format(locks_count, tenant))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "tennat_locks", "info": "retrieve locks information for the tenant. issue #963"}


tenant_locks = TenantLocks()
