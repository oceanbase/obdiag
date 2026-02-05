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
@time: 2025/07/18
@file: instruction_set_avx.py
@desc: Check if CPU supports AVX instruction set for OceanBase
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class InstructionSetAvxTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check if version requires AVX instruction set check

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get_name()))
                    continue

                # Check CPU architecture first
                arch_cmd = "uname -m"
                arch_result = ssh_client.exec_cmd(arch_cmd).strip()
                self.stdio.verbose("CPU architecture on {0}: {1}".format(ssh_client.get_name(), arch_result))

                if arch_result != "x86_64":
                    self.stdio.verbose("CPU architecture is not x86_64, skipping AVX check on {0}".format(ssh_client.get_name()))
                    continue

                # Check if CPU supports AVX instruction set using lscpu
                cpu_flags_check_cmd = "lscpu | tail -n 1"
                cpu_flags_result = ssh_client.exec_cmd(cpu_flags_check_cmd).strip()
                self.stdio.verbose("CPU instruction set info on {0}: {1}".format(ssh_client.get_name(), cpu_flags_result))
                cpu_flags = cpu_flags_result.split()
                # check if avx
                if "avx" not in cpu_flags:
                    if self.observer_version:
                        # for self.observer_version exist
                        # check if current version requires AVX instruction set
                        if self._should_check_avx():
                            self.report.add_critical(
                                "CPU on {0} does not support AVX instruction set. if you want to use observer, please upgrade the observer version to '4.2.5.6 or later' or '4.3.5.4 or later' or '4.4.1.0 or later'".format(ssh_client.get_name())
                            )
                    else:
                        # for self.observer_version not exist, print warning
                        self.report.add_warning(
                            "CPU on {0} does not support AVX instruction set. Observer versions before '4.2.5.6', '4.3.5.4', or '4.4.1.0' may have compatibility issues. Please upgrade to '4.2.5.6 or later', '4.3.5.4 or later', or '4.4.1.0 or later' if you encounter problems.".format(ssh_client.get_name())
                        )

                # check if avx2
                if "avx2" not in cpu_flags:
                    if self.observer_version:
                        # when avx2 not in cpu_flags, check if observer version need not 4.2.0.0
                        if self.observer_version == "4.2.0.0":
                            self.report.add_critical("CPU on {0} does not support AVX2 instruction set. observer (version 4.2.0.0) need it. ".format(ssh_client.get_name()))
                    else:
                        # for self.observer_version not exist, print warning
                        self.report.add_warning(
                            "CPU on {0} does not support AVX2 instruction set. Observer version 4.2.0.0 requires AVX2 support. Please upgrade to a newer version if you encounter problems.".format(ssh_client.get_name())
                        )

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def _should_check_avx(self):
        """Check if current version requires AVX instruction set check"""
        if not self.observer_version:
            self.stdio.warn("Observer version is not available")
            return True  # Default to check if version is unknown

        version = self.observer_version.strip()

        # Parse version components
        try:
            parts = version.split('.')
            if len(parts) >= 4:
                major = parts[0]
                minor = parts[1]
                patch = parts[2]
                build = parts[3]

                # 4.2.5.x versions: 4.2.5.6 and later don't require AVX
                if major == "4" and minor == "2" and patch == "5":
                    if StringUtils.compare_versions_greater(version, "4.2.5.5"):
                        return False

                # 4.3.5.x versions: 4.3.5.4 and later don't require AVX
                elif major == "4" and minor == "3" and patch == "5":
                    if StringUtils.compare_versions_greater(version, "4.3.5.3"):
                        return False

                # 4.4.1.0 and later don't require AVX
                elif StringUtils.compare_versions_greater(version, "4.4.0.0"):
                    return False

                return True
            else:
                self.stdio.warn("Invalid version format: {0}".format(version))
                return True
        except Exception as e:
            self.stdio.warn("Error parsing version {0}: {1}".format(version, e))
            return True

    def get_task_info(self):
        return {
            "name": "instruction_set_avx",
            "info": "Check if CPU supports AVX instruction set for OceanBase compatibility",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1024",
        }


instruction_set_avx = InstructionSetAvxTask()
