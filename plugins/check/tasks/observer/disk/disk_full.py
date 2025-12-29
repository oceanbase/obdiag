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
@file: disk_full.py
@desc: Check whether the disk usage has reached the threshold
"""

from src.handler.check.check_task import TaskBase


class DiskFullTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: >= 4.0.0.0
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check data disk usage > 80%
            sql_data_80 = 'SELECT count(0) as cnt FROM oceanbase.GV$OB_SERVERS where DATA_DISK_IN_USE*100/DATA_DISK_CAPACITY >80'
            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_data_80).fetchall()
                if result and len(result) > 0 and result[0].get('cnt', 0) > 0:
                    self.report.add_warning("data disk usage exceeds 80% of capacity")
                    self.stdio.warn("Found servers with data disk usage > 80%")
            except Exception as e:
                self.stdio.error("Failed to check data disk 80%: {0}".format(e))

            # Check data disk usage > 90%
            sql_data_90 = 'SELECT count(0) as cnt FROM oceanbase.GV$OB_SERVERS where DATA_DISK_IN_USE*100/DATA_DISK_CAPACITY >90'
            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_data_90).fetchall()
                if result and len(result) > 0 and result[0].get('cnt', 0) > 0:
                    self.report.add_critical("data disk usage exceeds 90% of capacity")
                    self.stdio.warn("Found servers with data disk usage > 90%")
            except Exception as e:
                self.stdio.error("Failed to check data disk 90%: {0}".format(e))

            # Check log disk usage > 80%
            sql_log_80 = 'SELECT count(0) as cnt FROM oceanbase.GV$OB_SERVERS where LOG_DISK_IN_USE*100/LOG_DISK_CAPACITY >80'
            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_log_80).fetchall()
                if result and len(result) > 0 and result[0].get('cnt', 0) > 0:
                    self.report.add_warning("log disk usage exceeds 80% of capacity")
                    self.stdio.verbose("Found servers with log disk usage > 80%")
            except Exception as e:
                self.stdio.error("Failed to check log disk 80%: {0}".format(e))

            # Check log disk usage > 90%
            sql_log_90 = 'SELECT count(0) as cnt FROM oceanbase.GV$OB_SERVERS where LOG_DISK_IN_USE*100/LOG_DISK_CAPACITY >90'
            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_log_90).fetchall()
                if result and len(result) > 0 and result[0].get('cnt', 0) > 0:
                    self.report.add_critical("log disk usage exceeds 90% of capacity")
                    self.stdio.verbose("Found servers with log disk usage > 90%")
            except Exception as e:
                self.stdio.error("Failed to check log disk 90%: {0}".format(e))

            self.stdio.verbose("Disk usage check completed")

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "disk_full",
            "info": "Check whether the disk usage has reached the threshold.",
        }


disk_full = DiskFullTask()
