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
@time: 2025/06/10
@file: network_speed_diff.py
@desc: Check network card speed consistency across observers
"""

import re
from src.handler.checker.check_task import TaskBase


class NetworkSpeedDiff(TaskBase):
    def init(self, context, report):
        super().init(context, report)
        if self.ob_connector is None:
            self.report.add_critical("Database connection required for NIC name lookup")

    def execute(self):
        if self.observer_version:
            if super().check_ob_version_min("4.0.0.0"):
                pass
            else:
                return
        else:
            self.report.add_warning("Unable to determine observer version")
            return

        speeds = []
        for node in self.observer_nodes:
            ssh_client = node.get("ssher")
            node_ip = node.get("ip")

            if not ssh_client:
                self.report.add_fail("SSH client not available for {}".format(node.get("name", "unknown")))
                continue

            if not node_ip:
                self.report.add_fail("IP address not found for {}".format(ssh_client.get_name()))
                continue

            # Get NIC name from database
            nic_name = self._get_nic_name_from_db(node_ip)
            if not nic_name:
                continue  # Error already logged in _get_nic_name_from_db

            # Get NIC speed
            speed = self._get_nic_speed(ssh_client, nic_name)
            if speed:
                speeds.append((ssh_client.get_name(), speed))
                self.stdio.verbose("NIC speed on {} is {} Mb/s".format(ssh_client.get_name(), speed))

        # Check consistency
        if len(speeds) < len(self.observer_nodes):
            self.report.add_warning("Only {} out of {} observers reported valid NIC speeds".format(len(speeds), len(self.observer_nodes)))

        if len(set(speed for _, speed in speeds)) > 1:
            detail = ", ".join(["{}: {} Mb/s".format(node, speed) for node, speed in speeds])
            self.report.add_critical("Inconsistent NIC speeds detected: {}".format(detail))

    def _get_nic_name_from_db(self, node_ip):
        """Get NIC name from database for specified IP node"""
        sql = "SELECT VALUE FROM oceanbase.GV$OB_PARAMETERS WHERE NAME='devname' AND SVR_IP='{}'".format(node_ip)
        try:
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchone()
            if result and result.get("VALUE"):
                return result["VALUE"].strip()
            self.report.add_warning("No NIC name found for IP: {}".format(node_ip))
            return None
        except Exception as e:
            self.report.add_fail("Failed to query NIC name for {}: {}".format(node_ip, str(e)))
            return None

    def _get_nic_speed(self, ssh_client, nic_name):
        """Get speed of specified NIC on single node"""
        try:
            # 优先尝试ethtool
            if super().check_command_exist(ssh_client, "ethtool"):
                output = ssh_client.exec_cmd(f"ethtool {nic_name}")
                speed_match = re.search(r"Speed:\s*(\d+)Mb/s", output)
                if speed_match:
                    return speed_match.group(1)
                self.stdio.warn(f"ethtool output invalid for {nic_name}: {output}")

            # 回退到sys文件读取
            speed_file = f"/sys/class/net/{nic_name}/speed"
            speed = ssh_client.exec_cmd(f"cat {speed_file}").strip()
            return speed if speed.isdigit() else None

        except Exception as e:
            self.stdio.warn("Error getting NIC speed: {}".format(str(e)))
            return None

    def get_task_info(self):
        return {
            "name": "network_speed_diff",
            "info": "Check if all observers have consistent NIC speeds by dynamic NIC name lookup. issue #763",
        }


network_speed_diff = NetworkSpeedDiff()
