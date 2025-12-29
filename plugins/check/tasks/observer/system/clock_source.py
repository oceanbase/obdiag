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
@file: clock_source.py
@desc: Check the type of clock_source is tsc
"""

from src.handler.check.check_task import TaskBase


class ClockSourceTask(TaskBase):
    VALID_CLOCK_SOURCES = ["tsc", "arch_sys_counter", "kvm-clock"]

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()

                try:
                    clock_source = ssh_client.exec_cmd("cat /sys/devices/system/clocksource/clocksource0/current_clocksource").strip()

                    if clock_source not in self.VALID_CLOCK_SOURCES:
                        self.report.add_critical("On {0}: clock_source: {1}. recommended: tsc. Uneven CPU utilization during pressure testing resulted in low TPS during pressure testing".format(node_name, clock_source))
                except Exception as e:
                    self.stdio.error("Failed to check clock_source on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "clock_source", "info": "Check the type of clock_source is tsc."}


clock_source = ClockSourceTask()
