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
@file: network_drop.py
@desc: Check cluster info about network errors and drops
"""

from src.handler.check.check_task import TaskBase


class NetworkDropTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: >= 4.0.0.0
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                remote_ip = node.get("ip")
                node_name = ssh_client.get_name()

                # Get network device name from OB parameters
                try:
                    sql = 'select VALUE from oceanbase.GV$OB_PARAMETERS where NAME="devname" and SVR_IP="{0}"'.format(remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if not result or len(result) == 0:
                        self.stdio.verbose("No network device found for {0}".format(remote_ip))
                        continue
                    network_name = result[0].get('VALUE', '').strip()
                    if not network_name:
                        self.stdio.verbose("Empty network device name for {0}".format(remote_ip))
                        continue
                except Exception as e:
                    self.stdio.error("Failed to get network device name for {0}: {1}".format(remote_ip, e))
                    continue

                self.stdio.verbose("Checking network {0} on node {1}".format(network_name, node_name))

                # Check RX errors
                try:
                    cmd = "ip -s link show {0} | awk '/RX:/ {{getline; print $3}}'".format(network_name)
                    rx_error = ssh_client.exec_cmd(cmd).strip()
                    if rx_error and rx_error != "0":
                        self.report.add_critical("network: {0} RX error is not 0 on {1}, please check by ip -s link show {0}".format(network_name, node_name))
                except Exception as e:
                    self.stdio.error("Failed to check RX error on {0}: {1}".format(node_name, e))

                # Check TX errors
                try:
                    cmd = "ip -s link show {0} | awk '/TX:/ {{getline; print $3}}'".format(network_name)
                    tx_error = ssh_client.exec_cmd(cmd).strip()
                    if tx_error and tx_error != "0":
                        self.report.add_critical("network: {0} TX error is not 0 on {1}, please check by ip -s link show {0}".format(network_name, node_name))
                except Exception as e:
                    self.stdio.error("Failed to check TX error on {0}: {1}".format(node_name, e))

                # Check RX drops
                try:
                    cmd = "ip -s link show {0} | awk '/RX:/ {{getline; print $4}}'".format(network_name)
                    rx_drop = ssh_client.exec_cmd(cmd).strip()
                    if rx_drop and rx_drop != "0":
                        self.report.add_critical("network: {0} RX drop is not 0 on {1}, please check by ip -s link show {0}".format(network_name, node_name))
                except Exception as e:
                    self.stdio.error("Failed to check RX drop on {0}: {1}".format(node_name, e))

                # Check TX drops
                try:
                    cmd = "ip -s link show {0} | awk '/TX:/ {{getline; print $4}}'".format(network_name)
                    tx_drop = ssh_client.exec_cmd(cmd).strip()
                    if tx_drop and tx_drop != "0":
                        self.report.add_critical("network: {0} TX drop is not 0 on {1}, please check by ip -s link show {0}".format(network_name, node_name))
                except Exception as e:
                    self.stdio.error("Failed to check TX drop on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "network_drop",
            "info": "Check cluster info about network errors and drops.",
        }


network_drop = NetworkDropTask()
