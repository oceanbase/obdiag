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
@file: bug_385.py
@desc: Check for multiple root users issue in OB version [4.2.1.0,4.2.1.3]
       GitHub issue: https://github.com/oceanbase/obdiag/issues/385
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class Bug385Task(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: [4.2.1.0, 4.2.1.3]
            if not super().check_ob_version_min("4.2.1.0"):
                self.stdio.verbose("Version < 4.2.1.0, skip check")
                return

            if self.observer_version and StringUtils.compare_versions_greater(self.observer_version, "4.2.1.3"):
                self.stdio.verbose("Version > 4.2.1.3, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            sql = '''
                SELECT GROUP_CONCAT(TENANT_ID) AS tenant_ids
                FROM oceanbase.CDB_OB_USERS
                WHERE USER_NAME = 'root'
                GROUP BY TENANT_ID
                HAVING COUNT(*) > 1
            '''

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    for row in result:
                        tenant_ids = row.get('tenant_ids')
                        if tenant_ids:
                            self.report.add_critical(
                                "tenant: {0}. These tenants have multiple root users. Please consider upgrading the OceanBase version or removing the redundant users. Please get bug's info on https://github.com/oceanbase/obdiag/issues/385".format(
                                    tenant_ids
                                )
                            )
                            self.stdio.warn("Found tenants with multiple root users: {0}".format(tenant_ids))
                else:
                    self.stdio.verbose("No tenants with multiple root users found")
            except Exception as e:
                self.report.add_fail("Failed to check multiple root users: {0}".format(e))
                self.stdio.error("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "bug_385",
            "info": "OB version [4.2.1.0,4.2.1.3] If tenants have multiple root users",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/385",
        }


bug_385 = Bug385Task()
