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
@file: old_version.py
@desc: Check obproxy version. Some versions of obproxy are not recommended
"""

import re

from src.handler.checker.check_task import TaskBase


class OldVersionTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check if obproxy nodes exist
            if not self.obproxy_nodes or len(self.obproxy_nodes) == 0:
                self.stdio.verbose("No obproxy nodes found, skipping version check")
                return

            # Deprecated version patterns
            deprecated_patterns = [
                "4.0",
                "4.1",
                "4.2",
                "4.3.0",
                "3",
                "4.3.1",
                "4.3.2",
            ]

            # Check each obproxy node
            for node in self.obproxy_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                # Get obproxy version
                obproxy_version = self._get_obproxy_version_from_node(node, ssh_client)
                if not obproxy_version:
                    self.report.add_fail("node: {0} failed to get obproxy version".format(ssh_client.get_name()))
                    continue

                self.stdio.verbose("obproxy version on {0}: {1}".format(ssh_client.get_name(), obproxy_version))

                # Check if version matches any deprecated pattern
                is_deprecated = False
                for pattern in deprecated_patterns:
                    if obproxy_version.startswith(pattern):
                        is_deprecated = True
                        break

                if is_deprecated:
                    self.report.add_warning(
                        "obproxy version {0} on {1} is not recommended, please upgrade to the obproxy".format(
                            obproxy_version, ssh_client.get_name()
                        )
                    )

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _get_obproxy_version_from_node(self, node, ssh_client):
        """Get obproxy version from a specific node"""
        try:
            home_path = node.get("home_path")
            if not home_path:
                self.stdio.warn("node: {0} home_path is not configured".format(ssh_client.get_name()))
                return None

            # Try to get version using the same method as get_obproxy_version
            cmd = "export LD_LIBRARY_PATH={0}/lib && {0}/bin/obproxy --version 2>&1 | grep \"obproxy (\" | awk '{{print $3}}'".format(
                home_path
            )
            result = ssh_client.exec_cmd(cmd).strip()
            self.stdio.verbose("get obproxy version, run cmd = [{0}], result = [{1}]".format(cmd, result))

            if result:
                # Extract version number (e.g., "4.3.2.0-42" -> "4.3.2.0-42")
                # The version might be in format like "4.3.2.0-42" or just "4.3.2.0"
                version = result.strip()
                # Remove any trailing whitespace or newlines
                version = version.split()[0] if version.split() else version
                return version

            # Fallback: try without grep/awk
            cmd = "export LD_LIBRARY_PATH={0}/lib && {0}/bin/obproxy --version".format(home_path)
            result = ssh_client.exec_cmd(cmd)
            self.stdio.verbose("get obproxy version (fallback), run cmd = [{0}], result = [{1}]".format(cmd, result))

            if result:
                # Try to extract version using regex
                pattern = r"(\d+\.\d+\.\d+\.\d+(?:-\d+)?)"
                match = re.search(pattern, result)
                if match:
                    return match.group(1)

                # Try simpler pattern
                pattern = r"(\d+\.\d+\.\d+\.\d+)"
                match = re.search(pattern, result)
                if match:
                    return match.group(1)

            return None

        except Exception as e:
            self.stdio.warn("Error getting obproxy version from node {0}: {1}".format(ssh_client.get_name(), e))
            return None

    def get_task_info(self):
        return {
            "name": "old_version",
            "info": "Check obproxy version. Some versions of obproxy are not recommended. issue #1103",
        }


old_version = OldVersionTask()

