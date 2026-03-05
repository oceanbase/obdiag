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
@time: 2026/03/05
@file: docker0_interface_check.py
@desc: Check for docker0-like network interface during deployment environment validation.
       When deploying obproxy via OCP, if docker0 exists, the displayed IP might be
       docker0's address instead of the actual physical host address.
       Reference: https://github.com/oceanbase/obdiag/issues/1198
"""

import re
from src.handler.check.check_task import TaskBase


class Docker0InterfaceCheckTask(TaskBase):
    # Match docker0 or docker0-like interface names (e.g., docker0, docker0@if1)
    DOCKER0_PATTERN = re.compile(r"docker0", re.IGNORECASE)

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check both observer and obproxy nodes (build_before skips obproxy handler,
            # but observer nodes may be deployment targets for obproxy too)
            # Dedupe by (ip, ssh_port) to avoid duplicate warnings for same host
            seen = set()
            nodes_to_check = []
            for node in (self.observer_nodes or []) + (self.obproxy_nodes or []):
                key = (node.get("ip"), node.get("ssh_port", 22))
                if key not in seen:
                    seen.add(key)
                    nodes_to_check.append(node)

            for node in nodes_to_check:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()

                if not self.is_linux(ssh_client):
                    self.stdio.verbose("docker0 check skipped on {0}: not Linux".format(node_name))
                    continue

                try:
                    if self._has_docker0_interface(ssh_client):
                        self.report.add_warning(
                            "On {0}: docker0-like network interface detected. "
                            "When deploying obproxy via OCP, the displayed IP address might correspond to "
                            "the docker0 interface rather than the actual physical host address. "
                            "It is recommended to remove the docker0 interface if it is confirmed unused. "
                            "If obproxy has already been deployed, it is advised to remove the docker0 "
                            "interface and redeploy obproxy.".format(node_name)
                        )
                except Exception as e:
                    self.stdio.error("Failed to check docker0 interface on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def _has_docker0_interface(self, ssh_client) -> bool:
        """
        Check if docker0-like interface exists on the target host.

        Uses 'ip link show' to list interfaces. Supports both 'ip' and legacy 'ifconfig'.
        """
        # Try ip link show first (preferred on modern Linux)
        try:
            result = ssh_client.exec_cmd("ip link show 2>/dev/null")
            if result and self.DOCKER0_PATTERN.search(result):
                return True
        except Exception:
            pass

        # Fallback: check /sys/class/net/docker0
        try:
            result = ssh_client.exec_cmd("test -d /sys/class/net/docker0 && echo exists")
            if result and "exists" in result.strip():
                return True
        except Exception:
            pass

        return False

    def get_task_info(self):
        return {
            "name": "docker0_interface_check",
            "info": "Check for docker0-like network interface. When deploying obproxy via OCP, "
            "docker0 may cause the displayed IP to be wrong. Remove docker0 if unused.",
            "supported_os": ["linux"],
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1198",
        }


docker0_interface_check = Docker0InterfaceCheckTask()
