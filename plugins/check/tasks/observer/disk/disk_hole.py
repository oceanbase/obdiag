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
@file: disk_hole.py
@desc: Check whether there is a disk hole issue
"""

from src.handler.check.check_task import TaskBase


class DiskHoleTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            if super().check_ob_version_min("4.0.0.0"):
                # For version >= 4.0.0.0
                sql_data = "select sum(DATA_SIZE) as da from oceanbase.CDB_OB_TABLET_REPLICAS group by tenant_id ORDER BY da DESC limit 1"
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_data).fetchall()

                if result and result[0].get('da') and result[0].get('da') > 0:
                    sql_ratio = "select sum(REQUIRED_SIZE)/sum(DATA_SIZE) as da from oceanbase.CDB_OB_TABLET_REPLICAS group by tenant_id ORDER BY da DESC limit 1"
                    ratio_result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_ratio).fetchall()

                    if ratio_result and ratio_result[0].get('da'):
                        ratio = float(ratio_result[0].get('da'))
                        if ratio > 1000:
                            self.report.add_warning("There may be a problem of disk holes. Please check oceanbase.CDB_OB_TABLET_REPLICAS group by tenant_id ORDER BY da DESC")
            else:
                # For version < 4.0.0.0
                sql_ratio = "select sum(REQUIRED_SIZE)/sum(DATA_SIZE) as da from oceanbase.__all_virtual_meta_table group by tenant_id ORDER BY da DESC limit 1"
                try:
                    ratio_result = self.ob_connector.execute_sql_return_cursor_dictionary(sql_ratio).fetchall()
                    if ratio_result and ratio_result[0].get('da'):
                        ratio = float(ratio_result[0].get('da'))
                        if ratio > 1000:
                            self.report.add_warning("There may be an issue with empty disks")
                except Exception as e:
                    self.stdio.error("Failed to check disk hole: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "disk_hole", "info": "Check whether there is a disk hole issue."}


disk_hole = DiskHoleTask()
