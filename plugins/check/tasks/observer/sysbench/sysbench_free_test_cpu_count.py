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
@file: sysbench_free_test_cpu_count.py
@desc: Check cluster info about cpu_count for sysbench
"""

from src.handler.check.check_task import TaskBase


class SysbenchFreeTestCpuCountTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Get cpu_count from cluster
            sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where Name='cpu_count'"
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not result:
                return

            cpu_count = int(result[0].get('VALUE', 0))
            if cpu_count == 0:
                return  # Auto mode, skip check

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    continue

                node_name = ssh_client.get_name()

                try:
                    # Get OS CPU count
                    os_cpu_count = ssh_client.exec_cmd("lscpu | grep '^CPU(s):' | awk '{print $2}'").strip()
                    os_cpu = int(os_cpu_count) if os_cpu_count.isdigit() else 0

                    if os_cpu > 0:
                        ratio = cpu_count * 100 // os_cpu
                        if ratio < 90 or ratio > 100:
                            self.report.add_critical("On {0}: cpu_count/os_cpu_count is {1}%, is not between 90 and 100".format(node_name, ratio))

                except Exception as e:
                    self.stdio.error("Failed to check cpu count on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_free_test_cpu_count", "info": "Check cluster info about cpu_count for sysbench."}


sysbench_free_test_cpu_count = SysbenchFreeTestCpuCountTask()
