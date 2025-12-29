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
@time: 2025/01/23
@file: memory_limit_vs_phy_mem.py
@desc: Check if memory_limit is larger than physical memory size. This will cause serious problems.
"""

from src.handler.check.check_task import TaskBase


class MemoryLimitVsPhyMemTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def _parse_memory_value_to_bytes(self, value_str):
        """
        Parse memory value string with unit to bytes.
        Supports formats like: '62G', '1024M', '1073741824', '0M', '0'
        Returns bytes as float, or None if parsing fails.
        """
        if value_str is None:
            return None

        value_str = str(value_str).strip().upper()

        if not value_str or value_str == '0' or value_str == '0M' or value_str == '0G' or value_str == '0K':
            return 0.0

        # Try to parse as pure number (bytes)
        try:
            return float(value_str)
        except ValueError:
            pass

        # Parse with unit suffix
        unit_multipliers = {
            'K': 1024,
            'KB': 1024,
            'M': 1024 * 1024,
            'MB': 1024 * 1024,
            'G': 1024 * 1024 * 1024,
            'GB': 1024 * 1024 * 1024,
            'T': 1024 * 1024 * 1024 * 1024,
            'TB': 1024 * 1024 * 1024 * 1024,
        }

        for unit, multiplier in unit_multipliers.items():
            if value_str.endswith(unit):
                try:
                    number_part = value_str[: -len(unit)].strip()
                    return float(number_part) * multiplier
                except ValueError:
                    pass

        return None

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")

            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("this version: {0} is not support this task".format(self.observer_version))

            # Get memory_limit and memory_limit_percentage for all nodes
            memory_limit_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT SVR_IP, VALUE as memory_limit_value FROM oceanbase.GV$OB_PARAMETERS WHERE name = 'memory_limit';").fetchall()

            memory_limit_percentage_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT SVR_IP, VALUE as memory_limit_percentage_value FROM oceanbase.GV$OB_PARAMETERS WHERE name = 'memory_limit_percentage';").fetchall()

            if len(memory_limit_data) < 1:
                return self.report.add_fail("get memory_limit data error")

            # Create dictionaries for quick lookup
            memory_limit_dict = {}
            for row in memory_limit_data:
                svr_ip = row.get("SVR_IP") or row.get("svr_ip")
                memory_limit_value = row.get("memory_limit_value") or row.get("VALUE")
                if svr_ip:
                    memory_limit_dict[svr_ip] = memory_limit_value

            memory_limit_percentage_dict = {}
            for row in memory_limit_percentage_data:
                svr_ip = row.get("SVR_IP") or row.get("svr_ip")
                memory_limit_percentage_value = row.get("memory_limit_percentage_value") or row.get("VALUE")
                if svr_ip:
                    memory_limit_percentage_dict[svr_ip] = memory_limit_percentage_value

            # Check each observer node
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    node_ip = node.get("ip", "unknown")
                    self.report.add_fail("node: {0} ssh client is None".format(node_ip))
                    continue

                node_ip = node.get("ip")
                if not node_ip:
                    self.stdio.warn("node: {0} ip is not configured".format(ssh_client.get_name()))
                    continue

                # Get physical memory size (in GB)
                phy_mem_gb = self._get_physical_memory_gb(ssh_client)
                if phy_mem_gb is None or phy_mem_gb <= 0:
                    self.stdio.warn("node: {0} failed to get physical memory size".format(ssh_client.get_name()))
                    continue

                # Get memory_limit for this node
                memory_limit_value = memory_limit_dict.get(node_ip)
                memory_limit_percentage_value = memory_limit_percentage_dict.get(node_ip, "80")

                # Calculate actual memory_limit
                # If memory_limit is 0, use memory_limit_percentage to calculate
                if memory_limit_value is None:
                    self.stdio.warn("node: {0} memory_limit not found in parameters".format(ssh_client.get_name()))
                    continue

                # Parse memory_limit value (supports formats like '62G', '1024M', '1073741824')
                memory_limit_bytes = self._parse_memory_value_to_bytes(memory_limit_value)
                if memory_limit_bytes is None:
                    self.stdio.warn("node: {0} failed to parse memory_limit value: {1}".format(ssh_client.get_name(), memory_limit_value))
                    continue

                try:
                    memory_limit_percentage_value = float(memory_limit_percentage_value) if memory_limit_percentage_value else 80
                except (ValueError, TypeError) as e:
                    self.stdio.warn("node: {0} failed to parse memory_limit_percentage value: {1}".format(ssh_client.get_name(), e))
                    memory_limit_percentage_value = 80

                # Calculate memory_limit in GB
                # memory_limit is in bytes, convert to GB
                if memory_limit_bytes == 0:
                    # Use percentage calculation
                    memory_limit_gb = phy_mem_gb * memory_limit_percentage_value / 100.0
                    self.stdio.verbose("node: {0} memory_limit is 0, using percentage calculation: {1}% of {2}GB = {3}GB".format(ssh_client.get_name(), memory_limit_percentage_value, phy_mem_gb, memory_limit_gb))
                else:
                    # memory_limit is in bytes, convert to GB
                    memory_limit_gb = memory_limit_bytes / (1024 * 1024 * 1024)
                    self.stdio.verbose("node: {0} memory_limit: {1} ({2} bytes) = {3}GB".format(ssh_client.get_name(), memory_limit_value, memory_limit_bytes, memory_limit_gb))

                self.stdio.verbose("node: {0} physical memory: {1}GB, memory_limit: {2}GB".format(ssh_client.get_name(), phy_mem_gb, memory_limit_gb))

                # Check if memory_limit > physical memory
                if memory_limit_gb > phy_mem_gb:
                    self.report.add_critical(
                        "node: {0} memory_limit ({1}GB) is larger than physical memory ({2}GB). "
                        "This will cause serious problems. Please adjust memory_limit to be less than or equal to physical memory size.".format(ssh_client.get_name(), round(memory_limit_gb, 2), round(phy_mem_gb, 2))
                    )

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _get_physical_memory_gb(self, ssh_client):
        """Get physical memory size in GB from the system"""
        try:
            # Method 1: Use free -m command (most reliable)
            cmd = "free -m | grep Mem | awk '{print int($2/1024)}'"
            result = ssh_client.exec_cmd(cmd).strip()
            self.stdio.verbose("get physical memory using free -m, result: {0}".format(result))

            if result and result.isdigit():
                return float(result)

            # Method 2: Use /proc/meminfo
            cmd2 = "grep MemTotal /proc/meminfo | awk '{print int($2/1024/1024)}'"
            result2 = ssh_client.exec_cmd(cmd2).strip()
            self.stdio.verbose("get physical memory using /proc/meminfo, result: {0}".format(result2))

            if result2 and result2.isdigit():
                return float(result2)

            # Method 3: Use dmidecode (if available)
            cmd3 = "dmidecode -t memory 2>/dev/null | grep 'Size:' | grep -v 'No Module' | awk '{sum+=$2} END {print int(sum/1024)}'"
            result3 = ssh_client.exec_cmd(cmd3).strip()
            self.stdio.verbose("get physical memory using dmidecode, result: {0}".format(result3))

            if result3 and result3.isdigit():
                return float(result3)

            return None

        except Exception as e:
            self.stdio.warn("Error getting physical memory: {0}".format(e))
            return None

    def get_task_info(self):
        return {
            "name": "memory_limit_vs_phy_mem",
            "info": "Check if memory_limit is larger than physical memory size. memory_limit larger than physical memory will cause serious problems",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1066",
        }


memory_limit_vs_phy_mem = MemoryLimitVsPhyMemTask()
