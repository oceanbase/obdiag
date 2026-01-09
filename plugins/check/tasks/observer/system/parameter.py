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
@file: parameter.py
@desc: Check kernel parameters
       Reference: https://www.oceanbase.com/docs/enterprise-oceanbase-ocp-cn-1000000000125643
"""

from src.handler.check.check_task import TaskBase


class ParameterTask(TaskBase):
    # Parameter checks: (name, check_type, expected, error_message)
    PARAM_CHECKS = [
        ("net.core.somaxconn", "between", (2048, 16384), "net.core.somaxconn: {0}. recommended: 2048 ≤ value ≤ 16384."),
        ("net.core.netdev_max_backlog", "between", (500, 10000), "net.core.netdev_max_backlog: {0}. recommended: 500 ≤ value ≤ 10000."),
        ("net.core.rmem_default", "between", (65536, 16777216), "net.core.rmem_default: {0}. recommended: 65536 ≤ value ≤ 16777216."),
        ("net.core.wmem_default", "between", (65536, 16777216), "net.core.wmem_default: {0}. recommended: 65536 ≤ value ≤ 16777216."),
        ("net.core.rmem_max", "between", (8388608, 16777216), "net.core.rmem_max: {0}. recommended: 8388608 ≤ value ≤ 16777216."),
        ("net.core.wmem_max", "between", (8388608, 16777216), "net.core.wmem_max: {0}. recommended: 8388608 ≤ value ≤ 16777216."),
        ("net.ipv4.ip_forward", "equal", 0, "net.ipv4.ip_forward: {0}. recommended: 0."),
        ("net.ipv4.conf.default.rp_filter", "equal", 1, "net.ipv4.conf.default.rp_filter: {0}. recommended: 1."),
        ("net.ipv4.conf.default.accept_source_route", "equal", 0, "net.ipv4.conf.default.accept_source_route: {0}. recommended: 0."),
        ("net.ipv4.tcp_syncookies", "equal", 1, "net.ipv4.tcp_syncookies: {0}. recommended: 1."),
        ("net.ipv4.tcp_max_syn_backlog", "between", (1024, 16384), "net.ipv4.tcp_max_syn_backlog: {0}. recommended: 1024 ≤ value ≤ 16384."),
        ("net.ipv4.tcp_fin_timeout", "between", (15, 60), "net.ipv4.tcp_fin_timeout: {0}. recommended: 15 ≤ value ≤ 60."),
        ("net.ipv4.tcp_slow_start_after_idle", "equal", 0, "net.ipv4.tcp_slow_start_after_idle: {0}. recommended: 0."),
        ("vm.swappiness", "equal", 0, "vm.swappiness: {0}. recommended: 0."),
        ("vm.min_free_kbytes", "between", (32768, 2097152), "vm.min_free_kbytes: {0}. recommended: 32768 ≤ value ≤ 2097152."),
        ("vm.max_map_count", "between", (327680, 1000000), "vm.max_map_count: {0}. recommended: 327680 ≤ value ≤ 1000000."),
        ("vm.overcommit_memory", "equal", 0, "vm.overcommit_memory: {0}. recommended: 0."),
        ("vm.nr_hugepages", "equal", 0, "vm.nr_hugepages: {0}. recommended: 0."),
        ("fs.aio-max-nr", "min", 1048576, "fs.aio-max-nr: {0}. recommended: ≥ 1048576."),
        ("kernel.numa_balancing", "equal", 0, "kernel.numa_balancing: {0}. recommended: 0."),
        ("vm.zone_reclaim_mode", "equal", 0, "vm.zone_reclaim_mode: {0}. recommended: 0."),
        ("fs.file-max", "min", 6573688, "fs.file-max: {0}. recommended: ≥ 6573688."),
        ("fs.pipe-user-pages-soft", "equal", 0, "fs.pipe-user-pages-soft: {0}. recommended: 0."),
    ]

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    self.report.add_fail("node: {0} ssh client is None".format(node.get("ip")))
                    continue

                node_name = ssh_client.get_name()

                for param_name, check_type, expected, err_msg in self.PARAM_CHECKS:
                    try:
                        value = super().get_system_parameter(ssh_client, param_name)
                        if value is None:
                            continue

                        try:
                            val = int(value)
                        except ValueError:
                            self.stdio.verbose("Cannot parse {0} value: {1}".format(param_name, value))
                            continue

                        failed = False
                        if check_type == "equal":
                            failed = val != expected
                        elif check_type == "between":
                            failed = val < expected[0] or val > expected[1]
                        elif check_type == "min":
                            failed = val < expected

                        if failed:
                            self.report.add_warning("On {0}: {1}".format(node_name, err_msg.format(val)))
                    except Exception as e:
                        self.stdio.error("Failed to check {0} on {1}: {2}".format(param_name, node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "parameter",
            "info": "Check kernel parameters. Reference: https://www.oceanbase.com/docs/enterprise-oceanbase-ocp-cn-1000000000125643",
            "supported_os": ["linux"],  # Linux kernel parameters
        }


parameter = ParameterTask()
