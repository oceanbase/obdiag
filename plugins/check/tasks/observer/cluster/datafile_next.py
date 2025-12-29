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
@file: datafile_next.py
@desc: Check node's parameter 'datafile_maxsize' and 'datafile_next'. Issue #573
"""

from src.handler.check.check_task import TaskBase


class DatafileNextTask(TaskBase):
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
                SELECT GROUP_CONCAT(DISTINCT p1.SVR_IP) as ip_list 
                FROM oceanbase.GV$OB_PARAMETERS p1 
                JOIN oceanbase.GV$OB_PARAMETERS p2 ON p1.SVR_IP = p2.SVR_IP 
                JOIN oceanbase.GV$OB_PARAMETERS p3 ON p1.SVR_IP = p3.SVR_IP 
                WHERE p1.NAME = 'datafile_maxsize' AND p1.VALUE <> '0'
                    AND p2.NAME = 'datafile_size' AND CAST(p1.VALUE AS DECIMAL) > CAST(p2.VALUE AS DECIMAL)
                    AND p3.NAME = 'datafile_next' AND p3.VALUE = '0'
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    ip_list = result[0].get('ip_list')
                    if ip_list:
                        self.report.add_critical("node: {0} datafile_next is 0, the data file will not grow. More info: https://github.com/oceanbase/obdiag/issues/573".format(ip_list))
            except Exception as e:
                self.report.add_fail("Failed to check datafile_next: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "datafile_next",
            "info": "Check node's parameter 'datafile_maxsize' and 'datafile_next'",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/573",
        }


datafile_next = DatafileNextTask()
