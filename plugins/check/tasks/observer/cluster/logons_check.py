#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
@time: 2025/06/14
@file: logons_check.py
@desc: Check if user logons cumulative value is approaching 2147483647 threshold
"""

from src.handler.checker.check_task import TaskBase
from src.common.tool import StringUtils


class LogonsCheckTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)
        if self.ob_connector is None:
            self.report.add_critical("Database connection required for logons check")

    def execute(self):
        # 版本限制检查（仅检查低于4.2.1.4版本）
        if super().check_ob_version_min("4.2.1.4"):
            return self.report.add_normal("[SKIP] This task only applies to versions before 4.2.1.4")

        # 定义阈值参数
        MAX_THRESHOLD = 2147483647
        WARNING_THRESHOLD = MAX_THRESHOLD * 0.8  # 80% 阈值
        CRITICAL_THRESHOLD = MAX_THRESHOLD * 0.95  # 95% 阈值（可选）

        # 执行SQL查询
        sql = "SELECT * FROM oceanbase.GV$SYSSTAT WHERE NAME = 'user logons cumulative'"

        try:
            self.stdio.verbose("Executing user logons cumulative query")
            results = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not results:
                return self.report.add_warning("No user logons cumulative data found")

            self.stdio.verbose(f"Found {len(results)} nodes with logons data")

            # 逐节点检查
            for row in results:
                svr_ip = row.get('SVR_IP')
                svr_port = row.get('SVR_PORT')
                value = int(row.get('VALUE', 0))

                self.stdio.verbose(f"Node {svr_ip}:{svr_port} - Value: {value} " f"(Threshold: {WARNING_THRESHOLD:.0f})")

                # 构造节点标识
                node_id = f"{svr_ip}:{svr_port}"

                # 阈值判断逻辑
                if value >= MAX_THRESHOLD:
                    self.report.add_critical(f"Node {node_id} has reached the maximum value {MAX_THRESHOLD}. " "Immediate action required: Plan version upgrade or emergency restart.")
                elif value >= WARNING_THRESHOLD:
                    self.report.add_critical(f"Node {node_id} is approaching threshold: {value}/{MAX_THRESHOLD} (~{value/MAX_THRESHOLD*100:.1f}%). " "Recommended: Upgrade to 4.2.1.4+ or restart observer process.")
                elif value >= CRITICAL_THRESHOLD:
                    self.report.add_critical(f"Node {node_id} is critically close to threshold: {value}/{MAX_THRESHOLD} (~{value/MAX_THRESHOLD*100:.1f}%). " "Urgent: Restart observer process immediately.")
                else:
                    self.stdio.verbose(f"Node {node_id} is within safe range: {value}/{MAX_THRESHOLD}")

        except Exception as e:
            self.report.add_fail(f"Logons check failed: {str(e)}")
            self.stdio.warn(f"SQL execution error: {str(e)}")

    def get_task_info(self):
        return {"name": "logons_check", "info": "Check if user logons cumulative value is approaching 2147483647 threshold (versions before 4.2.1.4). issue #972"}


logons_check = LogonsCheckTask()
