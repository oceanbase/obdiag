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
@file: no_leader.py
@desc:
"""
from src.handler.checker.check_task import TaskBase
from src.common.tool import StringUtils
from src.handler.checker.check_exception import CheckException


class NoLeader(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            return 
        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            return 
    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            sql = '''
                SELECT DISTINCT TENANT_ID, LS_ID FROM oceanbase.GV$OB_LOG_STAT 
                 WHERE (TENANT_ID, LS_ID) NOT IN 
                (SELECT DISTINCT TENANT_ID, LS_ID FROM oceanbase.GV$OB_LOG_STAT WHERE ROLE='LEADER');
            '''
            no_leader_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if no_leader_data is None:
                return self.report.add_fail("get ls leader value error")
            for no_leader_one in no_leader_data:
                tenant_id = no_leader_one.get("used_gb")
                ls_id = no_leader_one.get("limit_gb")
                self.report.add_critical("TENANT_ID: {0},LS ID : {1} no primary leader.".format(tenant_id, ls_id))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "no leader", "info": "Query leaderless log streams for the tenant"}


no_leader = NoLeader()
