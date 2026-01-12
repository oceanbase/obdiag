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
@file: TCP_retransmission.py
@desc: Check TCP retransmission. From https://github.com/oceanbase/obdiag/issues/348
"""

from src.handler.check.check_task import TaskBase


class TcpRetransmissionTask(TaskBase):
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
                    # Check if tsar exists
                    result = ssh_client.exec_cmd('if command -v tsar &>/dev/null; then echo "exist"; fi').strip()
                    if result != "exist":
                        self.report.add_critical("On {0}: tsar is not installed. we can not check tcp retransmission.".format(node_name))
                        continue

                    # Check TCP retransmission
                    retran = ssh_client.exec_cmd("tsar --check --tcp -s retran | awk -F '=' '{print $2}'").strip()
                    try:
                        retran_value = float(retran)
                        if retran_value > 10:
                            self.report.add_critical("On {0}: tcp retransmission is too high ({1}%), over 10%.".format(node_name, retran_value))
                    except ValueError:
                        self.stdio.error("Cannot parse tcp retransmission value on {0}: {1}".format(node_name, retran))

                except Exception as e:
                    self.stdio.error("Failed to check TCP retransmission on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "TCP_retransmission", "info": "Check TCP retransmission. From https://github.com/oceanbase/obdiag/issues/348"}


TCP_retransmission = TcpRetransmissionTask()
