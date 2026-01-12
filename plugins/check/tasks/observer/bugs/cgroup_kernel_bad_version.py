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
@time: 2025/04/30
@file: cgroup_kernel_bad_version.py
@desc:
"""
import re

from src.common.command import get_observer_version
from src.handler.check.check_task import TaskBase


class CgroupKernelBadVersion(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    # [0,0.5]
    def execute(self):
        try:

            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")
            # get kernel version by ssher
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                # check cgroup is existed
                home_path = node.get("home_path")
                home_dir_path = ssh_client.exec_cmd("ls {0}".format(home_path))
                if "cgroup" not in home_dir_path:
                    self.stdio.verbose("node: {0} cgroup path is not exist.".format(ssh_client.get_name()))
                    continue
                if self.check_ob_version_min("3.2.4"):
                    # check enable_cgroup
                    enable_cgroup_data = self.ob_connector.execute_sql_return_cursor_dictionary("SHOW PARAMETERS LIKE 'enable_cgroup';").fetchall()
                    enable_cgroup_tag = False
                    for row in enable_cgroup_data:
                        if row.get("name") == "enable_cgroup":
                            if row.get("value") == "true":
                                enable_cgroup_tag = True
                                self.stdio.verbose("node: {0} enable_cgroup is true.".format(ssh_client.get_name()))
                    if not enable_cgroup_tag:
                        continue

                kernel_version = ssh_client.exec_cmd("uname -r")
                if not kernel_version:
                    return self.report.add_fail("get kernel version error")
                # check kernel version
                self.stdio.verbose("node: {0} kernel version is {1}".format(ssh_client.get_name(), kernel_version))

                if re.match(r"3\.10\.\d+-\d+", kernel_version):
                    self.report.add_critical(
                        "node: {0} kernel version is {1}, There is a risk of system downtime when deploying OBServer using cgroup method on an operating system with kernel version 3.10 issue #910".format(ssh_client.get_name(), kernel_version)
                    )
                    continue
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "cgroup_kernel_bad_version",
            "info": "There is a risk of system downtime when deploying OBServer using cgroup method on an operating system with kernel version 3.10",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/910",
            "supported_os": ["linux"],
        }


cgroup_kernel_bad_version = CgroupKernelBadVersion()
