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
@file: sysbench_run_test_tenant_memory_used.py
@desc: Check cluster info about memory used and memory hold for sysbench
"""

from src.handler.check.check_task import TaskBase


class SysbenchRunTestTenantMemoryUsedTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Get first USER tenant
            sql = "SELECT TENANT_ID FROM oceanbase.DBA_OB_TENANTS WHERE TENANT_TYPE='USER' limit 1"
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if not result or not result[0].get('TENANT_ID'):
                self.report.add_critical("the tenant_id of TENANT_TYPE='USER' is null. Please check your TENANT.")
                return

            tenant_id = result[0].get('TENANT_ID')

            for node in self.observer_nodes:
                remote_ip = node.get("ip")
                node_name = node.get("ssher").get_name() if node.get("ssher") else remote_ip

                try:
                    # Get memory_size
                    sql = """SELECT ROUND(t3.MEMORY_SIZE/1024/1024/1024) as memory_size 
                             FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4
                             where t1.tenant_id = t4.tenant_id
                             and t1.tenant_id={0}
                             AND t4.resource_pool_id=t2.resource_pool_id
                             AND t4.unit_config_id=t3.unit_config_id
                             and t2.svr_ip='{1}'
                             ORDER BY t1.tenant_name limit 1""".format(
                        tenant_id, remote_ip
                    )
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if not result:
                        continue
                    memory_size = int(result[0].get('memory_size', 0))
                    if memory_size == 0:
                        continue

                    # Get memory_hold
                    sql = "select ROUND(SUM(hold/1024/1024/1024)) as memory_hold from oceanbase.__all_virtual_memory_info where tenant_id={0} and svr_ip='{1}'".format(tenant_id, remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result:
                        memory_hold = int(result[0].get('memory_hold', 0))
                        if memory_hold * 100 // memory_size >= 90:
                            self.report.add_warning("On {0}: tenant memory is not enough. memory_hold is {1}G. memory_size is {2}G".format(node_name, memory_hold, memory_size))

                    # Get memory_used
                    sql = "select ROUND(SUM(used/1024/1024/1024)) as memory_used from oceanbase.__all_virtual_memory_info where tenant_id={0} and svr_ip='{1}'".format(tenant_id, remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result:
                        memory_used = int(result[0].get('memory_used', 0))
                        if memory_used * 100 // memory_size >= 90:
                            self.report.add_warning("On {0}: tenant memory is not enough. memory_used is {1}G. memory_size is {2}G".format(node_name, memory_used, memory_size))

                except Exception as e:
                    self.stdio.error("Failed to check tenant memory on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_run_test_tenant_memory_used", "info": "Check cluster info about memory used and memory hold for sysbench."}


sysbench_run_test_tenant_memory_used = SysbenchRunTestTenantMemoryUsedTask()
