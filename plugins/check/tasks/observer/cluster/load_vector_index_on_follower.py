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
@file: load_vector_index_on_follower.py
@desc: Validate load_vector_index_on_follower (available from OceanBase 4.4.1).
"""

from src.handler.check.check_task import TaskBase


def _is_ob_bool_false(value_raw):
    """OceanBase bool parameters often appear as True/False strings in GV$OB_PARAMETERS."""
    if value_raw is None:
        return False
    s = str(value_raw).strip().lower()
    return s in ("false", "0", "no", "off")


class LoadVectorIndexOnFollower(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            if not super().check_ob_version_min("4.4.1.0"):
                return self.report.add_warning("this version:{} is not support this task".format(self.observer_version))
            rows = self.ob_connector.execute_sql_return_cursor_dictionary('SELECT * FROM oceanbase.GV$OB_PARAMETERS WHERE name = "load_vector_index_on_follower";').fetchall()
            if len(rows) < 1:
                return self.report.add_fail("get load_vector_index_on_follower data error")
            for row in rows:
                value_raw = row.get("VALUE")
                svr_ip = row.get("SVR_IP")
                tenant_id = row.get("TENANT_ID")
                if value_raw is None:
                    return self.report.add_fail("get load_vector_index_on_follower value error")
                if _is_ob_bool_false(value_raw):
                    tid = "tenant_id: {0}, ".format(tenant_id) if tenant_id is not None else ""
                    self.report.add_warning("svr_ip: {1} {2}load_vector_index_on_follower is {0}. For latency-sensitive workloads, " "keep the default true so followers load in-memory vector indexes.".format(value_raw, svr_ip, tid))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "load_vector_index_on_follower",
            "info": "Check load_vector_index_on_follower (since 4.4.1). Warn when set to false; RT-sensitive workloads should keep true (default).",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1231",
        }


load_vector_index_on_follower = LoadVectorIndexOnFollower()
