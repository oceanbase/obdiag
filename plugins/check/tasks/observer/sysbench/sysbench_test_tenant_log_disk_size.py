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
@file: sysbench_test_tenant_log_disk_size.py
@desc: Check tenant log_disk_size parameters for sysbench
"""

from src.handler.check.check_task import TaskBase


class SysbenchTestTenantLogDiskSizeTask(TaskBase):
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
                remote_ip = node.get("ip")
                node_name = node.get("ssher").get_name() if node.get("ssher") else remote_ip

                sql = """SELECT GROUP_CONCAT(DISTINCT t1.tenant_name) as tenant_names 
                         FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4
                         where t1.tenant_id = t4.tenant_id
                         AND t4.resource_pool_id=t2.resource_pool_id
                         AND t4.unit_config_id=t3.unit_config_id
                         and t2.svr_ip='{0}'
                         and t3.LOG_DISK_SIZE/1024/1024/1024<20
                         and t1.tenant_id>1000
                         ORDER BY t1.tenant_name""".format(
                    remote_ip
                )

                try:
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result and result[0].get('tenant_names'):
                        self.report.add_warning("On {0}: log_disk_size <20G tenant: {1}. log_disk_size need >20G".format(node_name, result[0].get('tenant_names')))
                except Exception as e:
                    self.stdio.error("Failed to check tenant log disk size on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_test_tenant_log_disk_size", "info": "Check tenant log_disk_size parameters for sysbench."}


sysbench_test_tenant_log_disk_size = SysbenchTestTenantLogDiskSizeTask()
