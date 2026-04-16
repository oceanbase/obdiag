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
@time: 2026/04/16
@file: ob_vector_memory_limit_percentage.py
@desc: Validate ob_vector_memory_limit_percentage (available from OceanBase 4.3.3).
"""

from src.handler.check.check_task import TaskBase


class ObVectorMemoryLimitPercentage(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            if not super().check_ob_version_min("4.3.3.0"):
                return self.report.add_warning("this version:{} is not support this task".format(self.observer_version))
            rows = self.ob_connector.execute_sql_return_cursor_dictionary(
                'SELECT * FROM oceanbase.GV$OB_PARAMETERS WHERE name = "ob_vector_memory_limit_percentage";'
            ).fetchall()
            if len(rows) < 1:
                return self.report.add_fail("get ob_vector_memory_limit_percentage data error")
            for row in rows:
                value_raw = row.get("VALUE")
                svr_ip = row.get("SVR_IP")
                tenant_id = row.get("TENANT_ID")
                if value_raw is None:
                    return self.report.add_fail("get ob_vector_memory_limit_percentage value error")
                try:
                    pct = int(value_raw)
                except (TypeError, ValueError):
                    return self.report.add_fail("get ob_vector_memory_limit_percentage value error: {0}".format(value_raw))
                if pct > 60:
                    tid = "tenant_id: {0}, ".format(tenant_id) if tenant_id is not None else ""
                    self.report.add_warning(
                        "svr_ip: {1} {2}ob_vector_memory_limit_percentage is {0}, greater than 60. "
                        "In production it is recommended to keep this at or below 60.".format(pct, svr_ip, tid)
                    )

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "ob_vector_memory_limit_percentage",
            "info": "Check ob_vector_memory_limit_percentage (since 4.3.3). Warn when value is greater than 60; production should keep it at or below 60.",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1232",
        }


ob_vector_memory_limit_percentage = ObVectorMemoryLimitPercentage()
