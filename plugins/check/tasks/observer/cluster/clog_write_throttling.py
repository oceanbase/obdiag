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
@file: clog_write_throttling.py
@desc: Detect clog write throttling via log_disk_throttling_percentage and observer.log (#1230).
"""

from src.handler.check.check_task import TaskBase

PARAM_NAME = "log_disk_throttling_percentage"
LOG_MARK = "[LOG DISK THROTTLING]"


class ClogWriteThrottlingTask(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("this version:{} is not support this task".format(self.observer_version))

            rows = self.ob_connector.execute_sql_return_cursor_dictionary(
                "SELECT SVR_IP, TENANT_ID, VALUE FROM oceanbase.GV$OB_PARAMETERS WHERE name = '{0}';".format(PARAM_NAME)
            ).fetchall()
            if len(rows) < 1:
                return self.report.add_warning("can't find {0} in GV$OB_PARAMETERS, skip clog write throttling check".format(PARAM_NAME))

            svr_need_log = set()
            for row in rows:
                svr_ip = row.get("SVR_IP")
                val_raw = row.get("VALUE")
                if svr_ip is None or val_raw is None:
                    continue
                try:
                    if int(val_raw) < 100:
                        svr_need_log.add(str(svr_ip).strip())
                except (TypeError, ValueError):
                    continue

            if not svr_need_log:
                self.stdio.verbose("all {0} >= 100, skip observer.log scan for clog throttling".format(PARAM_NAME))
                return

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_ip = str(node.get("ip", "")).strip()
                if node_ip not in svr_need_log:
                    self.stdio.verbose("node {0}: {1}>=100 for all tenants, skip log scan".format(ssh_client.get_name(), PARAM_NAME))
                    continue

                home_path = node.get("home_path")
                if not home_path:
                    self.report.add_fail("node: {0} home_path is not configured".format(ssh_client.get_name()))
                    continue

                log_file_path = "{0}/log/observer.log".format(home_path)
                # grep -c prints 0 with exit status 1 when there are zero matches; avoid "|| echo 0" (would duplicate 0).
                check_cmd = "grep -cF '{0}' {1} 2>/dev/null || true".format(LOG_MARK, log_file_path)
                result = ssh_client.exec_cmd(check_cmd).strip()
                lines = [ln for ln in result.replace("\r", "").split("\n") if ln.strip() != ""]
                try:
                    hit_count = int(lines[0].strip()) if lines else 0
                except ValueError:
                    self.report.add_fail("node {0}: failed to parse grep result: {1}".format(ssh_client.get_name(), result))
                    continue

                self.stdio.verbose(
                    "node {0}: {1} hits of {2} in observer.log".format(ssh_client.get_name(), hit_count, LOG_MARK)
                )
                if hit_count > 0:
                    self.report.add_warning(
                        "node {0}: {1} < 100 and observer.log contains {2} ({3} occurrence(s)). "
                        "Clog write throttling is in effect; tune log disk or throttling settings if unexpected.".format(
                            ssh_client.get_name(), PARAM_NAME, LOG_MARK, hit_count
                        )
                    )

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "clog_write_throttling",
            "info": "When log_disk_throttling_percentage < 100, scan {home_path}/log/observer.log for [LOG DISK THROTTLING] to detect clog write throttling.",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1230",
        }


clog_write_throttling = ClogWriteThrottlingTask()
