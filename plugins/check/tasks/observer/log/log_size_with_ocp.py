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
@file: log_size_with_ocp.py
@desc: Check log_dir free space is over the size of 100 file for OCP deployed nodes
"""

from src.handler.check.check_task import TaskBase


class LogSizeWithOcpTask(TaskBase):
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
                home_path = node.get("home_path", "")

                if not home_path:
                    self.stdio.verbose("No home_path configured for node {0}".format(node_name))
                    continue

                try:
                    # Check if OCP agent config exists
                    ocp_config = "/home/admin/ocp_agent/conf/config_properties/ob_logcleaner.yaml"
                    exists = ssh_client.exec_cmd('[ -e "{0}" ] && echo "yes" || echo "no"'.format(ocp_config)).strip()

                    if exists != "yes":
                        self.stdio.verbose("Node {0} is not deployed by OCP, skip".format(node_name))
                        continue

                    # Get threshold from OCP config
                    threshold_cmd = "grep 'ob.logcleaner.ob_log.disk.threshold' {0} -A1 | grep -oE '[0-9]+'".format(ocp_config)
                    threshold = ssh_client.exec_cmd(threshold_cmd).strip()

                    try:
                        threshold_val = int(threshold) if threshold else 80
                    except ValueError:
                        threshold_val = 80

                    if threshold_val < 80:
                        self.report.add_warning("On {0}: ocp ob.logcleaner.ob_log.disk.threshold is less than 80%".format(node_name))

                    # Check disk free space vs needed space
                    log_path = "{0}/log/".format(home_path)
                    free_space_cmd = "df {0} | awk 'NR==2{{print int($4*{1}/100)}}'".format(log_path, threshold_val)
                    disk_free_space = ssh_client.exec_cmd(free_space_cmd).strip()

                    log_count_cmd = "find {0} -type f -name '*.log*' | wc -l".format(log_path)
                    log_count = ssh_client.exec_cmd(log_count_cmd).strip()

                    try:
                        free_kb = int(disk_free_space) if disk_free_space.isdigit() else 0
                        count = int(log_count) if log_count.isdigit() else 0
                        need_space = (100 - count) * 256 * 1024  # KB

                        if need_space > 0 and free_kb < need_space:
                            self.report.add_critical("On {0}: disk_free_space_KB < log_dir_need_space. disk_free_space_KB:{1}KB, log_dir_need_space:{2}KB".format(node_name, free_kb, need_space))
                    except ValueError:
                        self.stdio.error("Failed to parse log space values on {0}".format(node_name))

                except Exception as e:
                    self.stdio.error("Failed to check log size on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "log_size_with_ocp", "info": "Check log_dir free space is over the size of 100 file for OCP deployed nodes."}


log_size_with_ocp = LogSizeWithOcpTask()
