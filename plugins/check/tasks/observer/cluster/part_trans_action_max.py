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
@file: part_trans_action_max.py
@desc: Check whether there are more than 200 transaction participants
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class PartTransActionMaxTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version: [3.0.0.0, 4.0.0.0)
            if not super().check_ob_version_min("3.0.0.0"):
                self.stdio.verbose("Version < 3.0.0.0, skip check")
                return

            if super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version >= 4.0.0.0, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # First check if there are any transactions with part_trans_action > 2
            sql1 = 'select count(0) as cnt from oceanbase.__all_virtual_trans_stat where part_trans_action > 2'

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql1).fetchall()
                if not result or result[0].get('cnt', 0) == 0:
                    self.stdio.verbose("No transactions with part_trans_action > 2")
                    return

                # Check max transaction participants
                sql2 = 'select count(0) as max_cnt from oceanbase.__all_virtual_trans_stat where part_trans_action > 2 group by trans_id order by max_cnt desc limit 1'
                result2 = self.ob_connector.execute_sql_return_cursor_dictionary(sql2).fetchall()
                if result2 and len(result2) > 0:
                    max_participants = result2[0].get('max_cnt', 0)
                    if max_participants > 200:
                        self.report.add_critical("part_trans_action_max is {0}, over 200".format(max_participants))
            except Exception as e:
                self.report.add_fail("Failed to check part_trans_action_max: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "part_trans_action_max", "info": "Check whether there are more than 200 transaction participants."}


part_trans_action_max = PartTransActionMaxTask()
