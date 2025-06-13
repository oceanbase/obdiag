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
@time: 2025/06/13
@file: paxo_members.py
@desc: Paxos members status check task for OceanBase
"""

from src.handler.checker.check_task import TaskBase


class PaxosMembersTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)
        self.stdio.verbose("PaxosMembersTask initialized")

    def execute(self):
        # 检查版本兼容性
        if not super().check_ob_version_min("4.0.0.0"):
            return self.report.add_warning("Paxos members check requires OceanBase 4.0.0.0+")

        # 执行SQL查询
        sql = """
        SELECT * FROM oceanbase.DBA_OB_LS_LOCATIONS ;
        """

        try:
            self.stdio.verbose("Executing Paxos members SQL query")
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            # 处理查询结果
            if not result:
                self.report.add_warning("No Paxos member information found")
                return

            self.stdio.verbose(f"Paxos members query returned {len(result)} rows")
            ls_data = []

            # 检查每个LS的Paxos成员状态
            for row in result:
                ls_id = row.get('LS_ID')
                SVR_IP = row.get('SVR_IP')
                SVR_PORT = row.get('SVR_PORT')
                ls_data.append("{0}:{1}:{2}".format(SVR_IP, SVR_PORT, ls_id))
            self.stdio.verbose(f"ls_data: {ls_data}")
            # select all tenant members_list from PAXOS_MEMBER_LIST
            sql = """
            SELECT * FROM oceanbase.GV$OB_LOG_STAT;
            """
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            for row in result:
                tenant_id = row.get('TENANT_ID')
                PAXOS_MEMBER_LIST = row.get('PAXOS_MEMBER_LIST')
                for ls in ls_data:
                    if ls not in PAXOS_MEMBER_LIST:
                        self.report.add_fail(f"tenant_id:{tenant_id} ls:{ls} is not in paxos_member_list")

        except Exception as e:
            self.report.add_fail(f"Paxos members query failed: {str(e)}")
            self.stdio.warn(f"SQL execution error: {str(e)}")

    def get_task_info(self):
        return {"name": "paxo_members", "info": "Inspecion checks if ls consistents with paxo-members，else delete server can't success. #"}


# 注册任务实例（注意：实例名与文件名保持一致）
paxo_members = PaxosMembersTask()
