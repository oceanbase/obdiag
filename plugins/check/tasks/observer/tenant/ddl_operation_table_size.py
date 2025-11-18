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
@time: 2025/01/23
@file: ddl_operation_table_size.py
@desc: Check the size of tenant internal table __all_ddl_operation. When the number of records exceeds 10 million, prompt the user to pay attention.
"""

from src.handler.checker.check_task import TaskBase


class DdlOperationTableSizeTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            
            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("this version: {0} is not support this task".format(self.observer_version))

            # Query tenants with __all_ddl_operation table record count > 10 million
            # Reference SQL: select tenant_id,count(*) from __all_virtual_ddl_operation group by tenant_id having count(*)>10000000;
            sql = """
                SELECT 
                    tenant_id,
                    COUNT(*) as record_count
                FROM 
                    oceanbase.__all_virtual_ddl_operation
                GROUP BY 
                    tenant_id
                HAVING 
                    COUNT(*) > 10000000
            """
            
            self.stdio.verbose("Querying tenants with __all_ddl_operation table record count > 10 million")
            results = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            
            if results is None or len(results) == 0:
                self.stdio.verbose("No tenants found with __all_ddl_operation table record count > 10 million")
                return

            # Get tenant names for better reporting
            tenant_info_sql = """
                SELECT 
                    tenant_id,
                    tenant_name
                FROM 
                    oceanbase.__all_tenant
            """
            tenant_info_results = self.ob_connector.execute_sql_return_cursor_dictionary(tenant_info_sql).fetchall()
            
            tenant_name_dict = {}
            if tenant_info_results:
                for row in tenant_info_results:
                    tenant_id = row.get("tenant_id") or row.get("TENANT_ID")
                    tenant_name = row.get("tenant_name") or row.get("TENANT_NAME")
                    if tenant_id:
                        tenant_name_dict[tenant_id] = tenant_name

            # Check each tenant
            for row in results:
                tenant_id = row.get("tenant_id") or row.get("TENANT_ID")
                record_count = row.get("record_count") or row.get("RECORD_COUNT")

                if tenant_id is None or record_count is None:
                    continue

                tenant_id = int(tenant_id)
                record_count = int(record_count)
                
                # Get tenant name
                tenant_name = tenant_name_dict.get(tenant_id, "tenant_id_{0}".format(tenant_id))

                self.stdio.verbose("Tenant {0} (ID: {1}) has {2} records in __all_ddl_operation table".format(
                    tenant_name, tenant_id, record_count
                ))

                # Report warning if record count > 10 million
                if record_count > 10000000:
                    self.report.add_warning(
                        "Tenant {0} (ID: {1}) internal table __all_ddl_operation has {2} records, exceeding 10 million. "
                        "Please pay attention. In some scenarios, such as using this tenant's backup for database physical recovery, "
                        "you may need to increase the parameter internal_sql_execute_timeout. "
                        "Reference: https://ask.oceanbase.com/t/topic/35629018".format(
                            tenant_name, tenant_id, record_count
                        )
                    )

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "ddl_operation_table_size",
            "info": "Check the size of tenant internal table __all_ddl_operation. When the number of records exceeds 10 million, prompt the user to pay attention. issue #1061",
        }


ddl_operation_table_size = DdlOperationTableSizeTask()

