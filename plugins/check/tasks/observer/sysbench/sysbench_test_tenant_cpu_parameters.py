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
@file: sysbench_test_tenant_cpu_parameters.py
@desc: Check tenant cpu parameters for sysbench
"""

from src.handler.check.check_task import TaskBase


class SysbenchTestTenantCpuParametersTask(TaskBase):
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
                    # Get cpu_count
                    sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where Name='cpu_count'"
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    cluster_cpu = int(result[0].get('VALUE', 0)) if result else 0

                    # Get OS CPU count
                    os_cpu = int(ssh_client.exec_cmd("lscpu | grep '^CPU(s):' | awk '{print $2}'").strip() or 0)
                    cpu_count = os_cpu if cluster_cpu == 0 else cluster_cpu
                    cpu_min = cpu_count // 2

                    # Check min_cpu
                    sql = """SELECT GROUP_CONCAT(DISTINCT TENANT_NAME) as TENANT_NAME 
                             FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4
                             where t1.tenant_id = t4.tenant_id
                             AND t4.resource_pool_id=t2.resource_pool_id
                             AND t4.unit_config_id=t3.unit_config_id
                             and t2.svr_ip='{0}'
                             AND t3.min_cpu<={1}
                             ORDER BY t1.tenant_name""".format(
                        remote_ip, cpu_min
                    )
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result and result[0].get('TENANT_NAME'):
                        self.report.add_warning("On {0}: cpu_count is {1}. the min_cpu of tenant should cpu_count/2 ~ cpu_count. tenant: {2} need check".format(node_name, cpu_count, result[0].get('TENANT_NAME')))

                    # Check max_cpu
                    sql = """SELECT GROUP_CONCAT(DISTINCT TENANT_NAME) as TENANT_NAME 
                             FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4
                             where t1.tenant_id = t4.tenant_id
                             AND t4.resource_pool_id=t2.resource_pool_id
                             AND t4.unit_config_id=t3.unit_config_id
                             and t2.svr_ip='{0}'
                             AND t3.max_cpu<={1}
                             ORDER BY t1.tenant_name""".format(
                        remote_ip, cpu_min
                    )
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result and result[0].get('TENANT_NAME'):
                        self.report.add_warning("On {0}: cpu_count is {1}. the max_cpu of tenant should cpu_count/2 ~ cpu_count. tenant: {2} need check".format(node_name, cpu_count, result[0].get('TENANT_NAME')))

                    # Check MAX_IOPS
                    sql = """SELECT GROUP_CONCAT(DISTINCT TENANT_NAME) as TENANT_NAME 
                             FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4
                             where t1.tenant_id = t4.tenant_id
                             AND t4.resource_pool_id=t2.resource_pool_id
                             AND t4.unit_config_id=t3.unit_config_id
                             and t2.svr_ip='{0}'
                             and t1.tenant_id>1000
                             and (t3.MAX_IOPS<t3.max_cpu*1000 or t3.MAX_IOPS>t3.max_cpu*100000)
                             ORDER BY t1.tenant_name""".format(
                        remote_ip
                    )
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result and result[0].get('TENANT_NAME'):
                        self.report.add_warning("On {0}: the MAX_IOPS of tenant should max_cpu * 10000 ~ max_cpu * 1000000. tenant: {1} need check".format(node_name, result[0].get('TENANT_NAME')))

                    # Check MIN_IOPS
                    sql = """SELECT GROUP_CONCAT(DISTINCT TENANT_NAME) as TENANT_NAME 
                             FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4
                             where t1.tenant_id = t4.tenant_id
                             AND t4.resource_pool_id=t2.resource_pool_id
                             AND t4.unit_config_id=t3.unit_config_id
                             and t2.svr_ip='{0}'
                             and t1.tenant_id>1000
                             and (t3.MIN_IOPS<t3.min_cpu*1000 or t3.MIN_IOPS>t3.min_cpu*100000)
                             ORDER BY t1.tenant_name""".format(
                        remote_ip
                    )
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result and result[0].get('TENANT_NAME'):
                        self.report.add_warning("On {0}: the MIN_IOPS of tenant should min_cpu * 10000 ~ min_cpu * 1000000. tenant: {1} need check".format(node_name, result[0].get('TENANT_NAME')))

                except Exception as e:
                    self.stdio.error("Failed to check tenant cpu parameters on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_test_tenant_cpu_parameters", "info": "Check tenant cpu parameters for sysbench."}


sysbench_test_tenant_cpu_parameters = SysbenchTestTenantCpuParametersTask()
