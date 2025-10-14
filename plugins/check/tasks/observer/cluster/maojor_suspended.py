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
@time: 2025/07/16
@file: maojor_suspended.py
@desc: Check for suspended major compaction in OceanBase cluster
"""
from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class MajorSuspendedTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                self.report.add_fail("Database connection is not available")
                return
            if StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0"):
                pass
            else:
                return None
            # Query for suspended major compaction
            sql = "SELECT TENANT_ID, IS_SUSPENDED FROM oceanbase.CDB_OB_MAJOR_COMPACTION WHERE IS_SUSPENDED = 'YES'"

            try:
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
                results = cursor.fetchall()

                self.stdio.verbose("Found {0} suspended major compaction records".format(len(results)))

                if results:
                    for row in results:
                        tenant_id = row.get('TENANT_ID', 'Unknown')
                        is_suspended = row.get('IS_SUSPENDED', 'Unknown')

                        self.report.add_warning("Tenant ID {0} has major compaction manually suspended (IS_SUSPENDED = {1}). " "This may impact storage efficiency and performance.".format(tenant_id, is_suspended))
                        self.stdio.warn("Major compaction suspended for tenant {0}".format(tenant_id))
                else:
                    self.stdio.verbose("No suspended major compaction found")

            except Exception as e:
                self.report.add_fail("Failed to query major compaction status: {0}".format(e))
                self.stdio.warn("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "maojor_suspended",
            "info": "Check for manually suspended major compaction in OceanBase cluster. issue #1015",
        }


maojor_suspended = MajorSuspendedTask()
