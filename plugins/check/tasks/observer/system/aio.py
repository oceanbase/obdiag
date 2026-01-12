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
@file: aio.py
@desc: Check AIO settings
"""

from src.handler.check.check_task import TaskBase


class AioTask(TaskBase):
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
                    # Get observer count on this node
                    observer_count = ssh_client.exec_cmd("ps -ef | grep observer | grep -v grep | wc -l").strip()
                    observer_num = int(observer_count) if observer_count.isdigit() else 1

                    # Check fs.aio-max-nr
                    aio_max_nr = super().get_system_parameter(ssh_client, "fs.aio-max-nr")
                    if aio_max_nr:
                        try:
                            aio_max = int(aio_max_nr)
                            if aio_max < 1048576:
                                self.report.add_warning("On {0}: fs.aio-max-nr: {1}. recommended: >= 1048576".format(node_name, aio_max))

                            # Check fs.aio-nr
                            aio_nr = super().get_system_parameter(ssh_client, "fs.aio-nr")
                            if aio_nr:
                                aio_current = int(aio_nr)
                                required_aio = 20000 * observer_num
                                available_aio = aio_max - aio_current

                                if available_aio < required_aio:
                                    self.report.add_warning("On {0}: fs.aio-nr: {1}. recommended: aio-max-nr - aio-nr > 20000 * observer_num ({2})".format(node_name, aio_current, required_aio))
                        except ValueError:
                            self.stdio.error("Failed to parse aio values on {0}".format(node_name))

                except Exception as e:
                    self.stdio.error("Failed to check aio on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "aio",
            "info": "Check AIO settings (fs.aio-max-nr, fs.aio-nr).",
            "supported_os": ["linux"],  # AIO parameters are Linux-specific
        }


aio = AioTask()
