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
@file: memstore_usage.py
@desc:
"""
from decimal import Decimal

from src.handler.checker.check_task import TaskBase
from src.common.tool import StringUtils
from src.handler.checker.check_exception import CheckException


class SessionLimit(TaskBase):

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
                SELECT tenant,count(tenant) as session_count FROM  oceanbase.GV$OB_PROCESSLIST group by tenant;
            '''
            session_limit_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if session_limit_data is None:
                return self.report.add_fail("get session limit value error")
            for session_limit_one in session_limit_data:
                tenant = session_limit_one.get("tenant")
                session_count = int(session_limit_one.get("session_count"))
                if tenant is None:
                    return
                if session_count > 5000:
                    self.report.add_critical("tenant: {1}. The number of connections for the tenant is {0}".format(session_count, tenant))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "session_count", "info": "retrieve connection information for the tenant"}


session_limit = SessionLimit()
