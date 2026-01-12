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
@file: sysbench_free_test_memory_limit.py
@desc: Check cluster info about memory_limit for sysbench
"""

from src.handler.check.check_task import TaskBase
import re


class SysbenchFreeTestMemoryLimitTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    continue

                node_name = ssh_client.get_name()
                remote_ip = node.get("ip")

                try:
                    # Get memory_limit
                    sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where NAME='memory_limit' and SVR_IP='{0}'".format(remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    memory_limit = 0
                    if result:
                        val = result[0].get('VALUE', '0')
                        match = re.search(r'(\d+)', str(val))
                        if match:
                            memory_limit = int(match.group(1))

                    # Get memory_limit_percentage
                    sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where NAME='memory_limit_percentage' and SVR_IP='{0}'".format(remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    memory_limit_percentage = int(result[0].get('VALUE', 80)) if result else 80

                    # Get OS memory
                    os_memory = int(ssh_client.exec_cmd("free -m | grep Mem | awk '{print int($2/1024)}'").strip() or 0)
                    if os_memory == 0:
                        continue

                    # Calculate actual memory_limit
                    if memory_limit == 0:
                        actual_memory_limit = os_memory * memory_limit_percentage // 100
                    else:
                        actual_memory_limit = memory_limit

                    # Check ratio
                    if os_memory > 0:
                        ratio = actual_memory_limit * 100 // os_memory
                        if ratio < 80 or ratio > 100:
                            self.report.add_critical("On {0}: memory_limit: {1}G. os_memory: {2}G. memory_limit/os_memory is {3}%, is not between 80% and 100%".format(node_name, actual_memory_limit, os_memory, ratio))

                    # Check memory_size of tenants
                    sql = """SELECT GROUP_CONCAT(DISTINCT TENANT_NAME) as tenant_names
                             FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4
                             where t1.tenant_id = t4.tenant_id
                             AND t4.resource_pool_id=t2.resource_pool_id
                             AND t4.unit_config_id=t3.unit_config_id
                             and t2.svr_ip='{0}'
                             and t1.tenant_id>1000
                             and t3.MEMORY_SIZE/1024/1024/1024<({1}*0.8)
                             ORDER BY t1.tenant_name""".format(
                        remote_ip, actual_memory_limit
                    )
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result and result[0].get('tenant_names'):
                        self.report.add_warning("On {0}: memory_size should over memory_limit*80%. tenant: {1} need check".format(node_name, result[0].get('tenant_names')))

                except Exception as e:
                    self.stdio.error("Failed to check memory limit on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_free_test_memory_limit", "info": "Check cluster info about memory_limit for sysbench."}


sysbench_free_test_memory_limit = SysbenchFreeTestMemoryLimitTask()
