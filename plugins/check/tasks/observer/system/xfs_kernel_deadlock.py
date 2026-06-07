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
@time: 2026/06/08
@file: xfs_kernel_deadlock.py
@desc:
"""
import re

from src.handler.check.check_task import TaskBase


FIXED_KERNEL_RELEASE = (1062,)


def is_redhat_compatible_7(release_text):
    if not release_text:
        return False
    normalized = release_text.lower()
    if "release 7" not in normalized:
        return False
    return any(name in normalized for name in ("centos", "red hat", "rhel", "oracle linux", "alinux", "anolis"))


def parse_el7_310_kernel_release(kernel_version):
    if not kernel_version:
        return None
    match = re.search(r"^3\.10\.0-([0-9]+(?:\.[0-9]+)*)\.el7", kernel_version.strip())
    if not match:
        return None
    return tuple(int(part) for part in match.group(1).split("."))


def has_xfs_mount(mount_output):
    if not mount_output:
        return False
    for line in mount_output.splitlines():
        normalized = line.lower()
        if " xfs" in normalized or " type xfs " in normalized:
            return True
    return False


def is_affected_xfs_kernel(release_text, kernel_version, mount_output):
    kernel_release = parse_el7_310_kernel_release(kernel_version)
    return is_redhat_compatible_7(release_text) and kernel_release is not None and kernel_release < FIXED_KERNEL_RELEASE and has_xfs_mount(mount_output)


class XfsKernelDeadlock(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                self._execute_node(node)
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _execute_node(self, node):
        ssh_client = node.get("ssher")
        if ssh_client is None:
            self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
            return

        release_text = ssh_client.exec_cmd("cat /etc/redhat-release 2>/dev/null || true").strip()
        kernel_version = ssh_client.exec_cmd("uname -r").strip()
        mount_output = ssh_client.exec_cmd("findmnt -rn -t xfs -o TARGET,FSTYPE 2>/dev/null || mount | grep ' type xfs ' || true")

        self.stdio.verbose("node: {0} os release: {1}, kernel: {2}".format(ssh_client.get_name(), release_text, kernel_version))
        if is_affected_xfs_kernel(release_text, kernel_version, mount_output):
            self.report.add_critical(
                "node: {0} runs {1} with kernel {2} and XFS mounts. This kernel is affected by the XFS possible memory allocation deadlock risk. "
                "Please upgrade to kernel-3.10.0-1062.el7 or later.".format(ssh_client.get_name(), release_text, kernel_version)
            )

    def get_task_info(self):
        return {
            "name": "xfs_kernel_deadlock",
            "info": "Check Red Hat compatible 7.x systems for old 3.10 XFS kernels affected by possible memory allocation deadlock.",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/238",
            "supported_os": ["linux"],
        }


xfs_kernel_deadlock = XfsKernelDeadlock()
