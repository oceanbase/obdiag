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
@file: sysbench_run_test_tenant_cpu_used.py
@desc: Check cluster info about cpu for sysbench run
"""

from src.handler.check.check_task import TaskBase


class SysbenchRunTestTenantCpuUsedTask(TaskBase):
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
                self.report.add_critical("tenant_id is null. Please check your tenant without sys")
                return

            tenant_id = result[0].get('TENANT_ID')

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    continue

                node_name = ssh_client.get_name()
                remote_ip = node.get("ip")
                home_path = node.get("home_path", "")

                if not home_path:
                    continue

                try:
                    # Get cpu_quota_concurrency
                    sql = """select VALUE from oceanbase.GV$OB_PARAMETERS where Name='cpu_quota_concurrency'
                             and TENANT_ID={0} and SVR_IP='{1}' limit 1""".format(
                        tenant_id, remote_ip
                    )
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    cpu_quota_concurrency = int(result[0].get('VALUE', 2)) if result else 2

                    # Get max_cpu
                    sql = """SELECT t3.MAX_CPU as max_cpu FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4
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
                    max_cpu = int(float(result[0].get('max_cpu', 0)))
                    if max_cpu == 0:
                        continue

                    # Get observer pid
                    observer_pid = ssh_client.exec_cmd("cat {0}/run/observer.pid".format(home_path)).strip()
                    if not observer_pid.isdigit():
                        continue

                    # Get tenant cpu used
                    cpu_cmd = "top -d 2 -H -b -n1 -p {0} | grep {1} | awk '{{total+=$9}}END{{printf \"%.0f\",total}}'".format(observer_pid, tenant_id)
                    tenant_cpu_used = int(ssh_client.exec_cmd(cpu_cmd).strip() or 0)

                    # Calculate ratio
                    if cpu_quota_concurrency > 0 and max_cpu > 0:
                        result_ratio = tenant_cpu_used * 100 // (max_cpu * cpu_quota_concurrency)
                        if result_ratio < 90:
                            self.report.add_warning(
                                "On {0}: tenant_cpu_used/max_cpu*cpu_quota_concurrency <90%, it is {1}%. tenant_id: {2}, max_cpu: {3}, tenant_cpu_used: {4}, cpu_quota_concurrency: {5}".format(
                                    node_name, result_ratio, tenant_id, max_cpu, tenant_cpu_used, cpu_quota_concurrency
                                )
                            )

                    # Check threads over 0.9c
                    thread_cmd = "top -H -b -n1 -p {0} | grep {1} | awk -v threshold=0.9 -F' ' 'NR > 7 && $9 > threshold {{ if (length(names)>0) names=names\",\"; names=names$12 }} END{{print names}}'".format(observer_pid, tenant_id)
                    over_threads = ssh_client.exec_cmd(thread_cmd).strip()
                    if over_threads:
                        self.report.add_warning("On {0}: tenant_id: {1}. over 0.9c thread is {2}".format(node_name, tenant_id, over_threads))

                except Exception as e:
                    self.stdio.error("Failed to check tenant cpu on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_run_test_tenant_cpu_used", "info": "Check cluster info about cpu for sysbench run."}


sysbench_run_test_tenant_cpu_used = SysbenchRunTestTenantCpuUsedTask()
