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
@file: network_speed.py
@desc: Check cluster info about network_speed
"""

from src.handler.check.check_task import TaskBase
import re


class NetworkSpeedTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()
                remote_ip = node.get("ip")

                try:
                    # Get network device name from OB parameters
                    if self.ob_connector is None:
                        continue

                    sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where NAME='devname' and SVR_IP='{0}'".format(remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

                    if not result:
                        self.stdio.verbose("Cannot get network name for {0}".format(node_name))
                        continue

                    network_name = result[0].get('VALUE', '').strip()
                    if network_name == "lo":
                        self.report.add_critical("On {0}: network_name is lo, can not get real speed".format(node_name))
                        continue

                    # Get network speed
                    speed_output = ssh_client.exec_cmd("ethtool {0} 2>/dev/null | grep Speed".format(network_name)).strip()
                    if not speed_output:
                        self.report.add_critical("On {0}: network_speed is null, can not get real speed".format(node_name))
                        continue

                    if "Unknown" in speed_output:
                        self.report.add_critical("On {0}: network_speed is {1}, can not get real speed".format(node_name, speed_output))
                        continue

                    # Extract numeric speed value
                    speed_match = re.search(r'(\d+)', speed_output)
                    if speed_match:
                        speed = int(speed_match.group(1))
                        if speed < 999:
                            self.report.add_critical("On {0}: network_speed is {1}Mb/s, less than 1000".format(node_name, speed))
                        elif speed < 9999:
                            self.report.add_warning("On {0}: network_speed is {1}Mb/s, less than 10000. Unpredictable anomalies are prone to occur in backup scenarios, it is recommended to upgrade to 10Gbps".format(node_name, speed))

                except Exception as e:
                    self.stdio.error("Failed to check network speed on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "network_speed", "info": "Check cluster info about network_speed."}


network_speed = NetworkSpeedTask()
