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
@time: 2025/06/12
@file: local_ip_check.py
@desc: Validate if local_ip in observer.config.bin matches the actual NIC IP on the configured network interface
"""

import re
from src.handler.checker.check_task import TaskBase


class LocalIPCheck(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        if self.observer_version:
            if super().check_ob_version_min("4.0.0.0"):
                pass
            else:
                self.report.add_warning("Unadapted by version. SKIP")
                return
        else:
            self.report.add_warning("Unable to determine observer version")
            return
        for node in self.observer_nodes:
            ssh_client = node.get("ssher")
            home_path = node.get("home_path")
            node_ip = node.get("ip")  # Get node IP address

            if not ssh_client:
                self.report.add_fail("SSH client not available for {}".format(node.get("name", "unknown")))
                continue

            if not home_path:
                self.report.add_fail("Home path not found for {}".format(ssh_client.get_name()))
                continue

            if not node_ip:
                self.report.add_fail("Node IP not found for {}".format(ssh_client.get_name()))
                continue

            # Get config file content
            config_path = f"{home_path}/etc/observer.config.bin"
            try:
                config_content = ssh_client.exec_cmd(f"cat {config_path}").strip()
                if not config_content:
                    self.report.add_warning(f"Empty config file: {config_path}")
                    continue
            except Exception as e:
                self.report.add_fail(f"Failed to read {config_path}: {str(e)}")
                continue

            # Parse local_ip from config
            config_ip = self._parse_config_value(config_content, "local_ip")
            if not config_ip:
                self.report.add_fail(f"Missing local_ip in {config_path}")
                continue

            self.stdio.verbose(f"Configured local_ip for {ssh_client.get_name()}: {config_ip}")

            # Validate IP match
            if config_ip != node_ip:
                self.report.add_critical("Config IP mismatch on {}: local_ip={} != NIC IP={}".format(ssh_client.get_name(), config_ip, node_ip))
                continue

            self.stdio.verbose(f"IP validation passed for {ssh_client.get_name()}")

    def _parse_config_value(self, content, key):
        """Parse specified key value from config file"""
        try:
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith('#') or '=' not in line:
                    continue
                k, v = line.split('=', 1)
                if k.strip() == key:
                    return v.strip()
            return None
        except Exception as e:
            self.stdio.warn(f"Config parsing error: {str(e)}")
            return None

    def get_task_info(self):
        return {
            "name": "local_ip_check",
            "info": "Validate if local_ip in observer.config.bin matches the actual NIC IP on the configured network interface. issue #878",
        }


# Register task instance
local_ip_check = LocalIPCheck()
