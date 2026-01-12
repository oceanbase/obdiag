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
@file: tenant_parameters.py
@desc: Check for column storage poc on tenant parameters
"""

from src.handler.check.check_task import TaskBase


class TenantParametersTask(TaskBase):
    PARAM_CHECKS = [
        ("collation_connection", "46", "collation_connection is recommended utf8mb4_bin. There are some tenant need change: {0}"),
        ("collation_server", "46", "collation_server is recommended utf8mb4_bin. There are some tenant need change: {0}"),
    ]

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.3.1.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check collation_connection (value 46 = utf8mb4_bin)
            self._check_sys_variable("collation_connection", "46", "collation_connection is recommended utf8mb4_bin. There are some tenant need change: {0}")

            # Check collation_server
            self._check_sys_variable("collation_server", "46", "collation_server is recommended utf8mb4_bin. There are some tenant need change: {0}")

            # Check ob_query_timeout (>= 10000000000)
            self._check_sys_variable_min("ob_query_timeout", 10000000000, "ob_query_timeout is recommended 10000000000. There are some tenant need change: {0}")

            # Check ob_trx_timeout (>= 100000000000)
            self._check_sys_variable_min("ob_trx_timeout", 100000000000, "ob_trx_timeout is recommended 100000000000. There are some tenant need change: {0}")

            # Check ob_sql_work_area_percentage (>= 30)
            self._check_sys_variable_min("ob_sql_work_area_percentage", 30, "ob_sql_work_area_percentage is recommended 30. There are some tenant need change: {0}")

            # Check max_allowed_packet (>= 67108864)
            self._check_sys_variable_min("max_allowed_packet", 67108864, "max_allowed_packet is recommended 67108864. There are some tenant need change: {0}")

            # Check default_table_store_format
            sql = 'SELECT GROUP_CONCAT(DISTINCT tenant_id) as tenant_ids from oceanbase.GV$OB_PARAMETERS where name="default_table_store_format" and value<>"column"'
            self._execute_check(sql, 'default_table_store_format is recommended "column". There are some tenant need change: {0}')

            # Check parallel_degree_policy (value 1 = auto)
            self._check_sys_variable("parallel_degree_policy", "1", 'parallel_degree_policy is recommended "auto". There are some tenant need change: {0}')

            # Check parallel_min_scan_time_threshold (<= 10)
            sql = 'SELECT GROUP_CONCAT(DISTINCT tenant_id) as tenant_ids from oceanbase.CDB_OB_SYS_VARIABLES where name="parallel_min_scan_time_threshold" and TENANT_ID>1000 AND TENANT_ID%2=0 and value>10'
            self._execute_check(sql, 'parallel_min_scan_time_threshold is recommended "10". There are some tenant need change: {0}')

            # Check parallel_servers_target (should be cpu_count*10)
            sql = '''SELECT GROUP_CONCAT(DISTINCT a.TENANT_ID) as tenant_ids from oceanbase.DBA_OB_TENANTS a, oceanbase.GV$OB_UNITS b, oceanbase.CDB_OB_SYS_VARIABLES c 
                     WHERE a.TENANT_ID=b.TENANT_ID and a.TENANT_ID=c.TENANT_ID and a.TENANT_ID>1000 AND a.TENANT_ID%2=0 
                     and b.MIN_CPU>0 and c.name="parallel_servers_target" and b.MIN_CPU*10<>c.value'''
            self._execute_check(sql, 'parallel_servers_target is recommended cpu_count*10. There are some tenant need change: {0}')

            # Check parallel_degree_limit (should be cpu_count*2)
            sql = '''SELECT GROUP_CONCAT(DISTINCT a.TENANT_ID) as tenant_ids from oceanbase.DBA_OB_TENANTS a, oceanbase.GV$OB_UNITS b, oceanbase.CDB_OB_SYS_VARIABLES c 
                     WHERE a.TENANT_ID=b.TENANT_ID and a.TENANT_ID=c.TENANT_ID and a.TENANT_ID>1000 AND a.TENANT_ID%2=0 
                     and b.MIN_CPU>0 and c.name="parallel_degree_limit" and b.MIN_CPU*2<>c.value'''
            self._execute_check(sql, 'parallel_degree_limit is recommended cpu_count*2. There are some tenant need change: {0}')

            # Check compaction_low_thread_score (should equal cpu_count)
            sql = '''SELECT GROUP_CONCAT(DISTINCT a.TENANT_ID) as tenant_ids FROM oceanbase.DBA_OB_TENANTS a, oceanbase.GV$OB_UNITS b, oceanbase.GV$OB_PARAMETERS c 
                     WHERE a.TENANT_ID=b.TENANT_ID and a.TENANT_ID=c.TENANT_ID and a.TENANT_ID>1000 AND a.TENANT_ID%2=0 
                     and b.MIN_CPU>0 and c.name="compaction_low_thread_score" and b.MIN_CPU<>c.value'''
            self._execute_check(sql, 'compaction_low_thread_score is recommended equal cpu_count. There are some tenant need change: {0}')

            # Check compaction_mid_thread_score (should equal cpu_count)
            sql = '''SELECT GROUP_CONCAT(DISTINCT a.TENANT_ID) as tenant_ids FROM oceanbase.DBA_OB_TENANTS a, oceanbase.GV$OB_UNITS b, oceanbase.GV$OB_PARAMETERS c 
                     WHERE a.TENANT_ID=b.TENANT_ID and a.TENANT_ID=c.TENANT_ID and a.TENANT_ID>1000 AND a.TENANT_ID%2=0 
                     and b.MIN_CPU>0 and c.name="compaction_mid_thread_score" and b.MIN_CPU<>c.value'''
            self._execute_check(sql, 'compaction_mid_thread_score is recommended equal cpu_count. There are some tenant need change: {0}')

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def _check_sys_variable(self, name, expected_value, err_msg):
        sql = 'SELECT GROUP_CONCAT(DISTINCT tenant_id) as tenant_ids from oceanbase.CDB_OB_SYS_VARIABLES where name="{0}" and TENANT_ID>1000 AND TENANT_ID%2=0 and VALUE<>{1}'.format(name, expected_value)
        self._execute_check(sql, err_msg)

    def _check_sys_variable_min(self, name, min_value, err_msg):
        sql = 'SELECT GROUP_CONCAT(DISTINCT tenant_id) as tenant_ids from oceanbase.CDB_OB_SYS_VARIABLES where name="{0}" and TENANT_ID>1000 AND TENANT_ID%2=0 and VALUE<{1}'.format(name, min_value)
        self._execute_check(sql, err_msg)

    def _execute_check(self, sql, err_msg):
        try:
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if result and len(result) > 0:
                tenant_ids = result[0].get('tenant_ids')
                if tenant_ids:
                    self.report.add_warning(err_msg.format(tenant_ids))
        except Exception as e:
            self.stdio.error("Failed to execute check: {0}".format(e))

    def get_task_info(self):
        return {"name": "tenant_parameters", "info": "Check for column storage poc on tenant parameters."}


tenant_parameters = TenantParametersTask()
