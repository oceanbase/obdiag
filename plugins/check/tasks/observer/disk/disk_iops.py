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
@file: disk_iops.py
@desc: Check whether the disk iops is sufficient
"""

from src.handler.check.check_task import TaskBase


class DiskIopsTask(TaskBase):
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
                SELECT GROUP_CONCAT(DISTINCT CONCAT(SVR_IP, ':', SVR_PORT) SEPARATOR ', ') AS unique_server_endpoints
                FROM oceanbase.GV$OB_IO_BENCHMARK
                WHERE size=16384 AND IOPS<1024
            """

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    servers = result[0].get('unique_server_endpoints')
                    if servers:
                        self.report.add_critical("These observer 16K IOPS are below 1024, please migrate as soon as possible. {0}".format(servers))
            except Exception as e:
                self.report.add_fail("Failed to check disk iops: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "disk_iops", "info": "Check whether the disk iops is sufficient."}


disk_iops = DiskIopsTask()
