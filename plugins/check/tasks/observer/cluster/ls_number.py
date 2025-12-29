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
@time: 2024/12/29
@file: ls_number.py
@desc: Check ls id is not_enough_replica
"""

from src.handler.check.check_task import TaskBase


class LsNumberTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = """
                WITH LeaderInfo AS (
                    SELECT 
                        tenant_id, 
                        ls_id, 
                        paxos_replica_num
                    FROM 
                        oceanbase.__all_virtual_log_stat
                    WHERE 
                        role = 'LEADER'
                ),
                RowCounts AS (
                    SELECT 
                        tenant_id, 
                        ls_id, 
                        COUNT(*) as row_count
                    FROM 
                        oceanbase.__all_virtual_log_stat
                    GROUP BY 
                        tenant_id, 
                        ls_id
                )
                SELECT 
                    GROUP_CONCAT(DISTINCT L.tenant_id) as not_enough_replica
                FROM 
                    LeaderInfo L
                JOIN 
                    RowCounts R
                ON 
                    L.tenant_id = R.tenant_id AND L.ls_id = R.ls_id
                WHERE 
                    R.row_count < L.paxos_replica_num
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    not_enough = result[0].get('not_enough_replica')
                    if not_enough:
                        self.report.add_critical("There is not_enough_replica tenant_id: {0}, please check as soon as possible.".format(not_enough))
            except Exception as e:
                self.report.add_fail("Failed to check ls number: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "ls_number", "info": "Check ls id is not_enough_replica"}


ls_number = LsNumberTask()
