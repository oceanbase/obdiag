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
@file: tenant_memory_tablet_count.py
@desc: Check if tenant memory specification and tablet count per observer exceed 90% in health check
"""

from decimal import Decimal

from src.handler.check.check_task import TaskBase


class TenantMemoryTabletCountTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")

            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("this version: {0} is not support this task".format(self.observer_version))

            # Check tenant memory specification
            self._check_tenant_memory_specification()

            # Check tablet count per observer
            self._check_tablet_count_per_observer()

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _check_tenant_memory_specification(self):
        """Check if tenant memory usage exceeds 90% of memory specification"""
        try:
            # Query tenant memory specification from gv$ob_units
            # Reference: select MEMORY_SIZE/1024/1024/1024 from gv$ob_units where tenant_id=xxx;
            sql_memory_size = """
                SELECT 
                    u.tenant_id,
                    t.tenant_name,
                    u.svr_ip,
                    ROUND(u.MEMORY_SIZE / 1024 / 1024 / 1024, 2) as memory_size_gb
                FROM 
                    oceanbase.gv$ob_units u
                    LEFT JOIN oceanbase.__all_tenant t ON u.tenant_id = t.tenant_id
                WHERE 
                    (u.tenant_id > 1000 OR u.tenant_id = 1)
                    AND u.MEMORY_SIZE > 0
            """

            self.stdio.verbose("Querying tenant memory specification from gv$ob_units")
            memory_size_results = self.ob_connector.execute_sql_return_cursor_dictionary(sql_memory_size).fetchall()

            if memory_size_results is None or len(memory_size_results) == 0:
                self.stdio.verbose("No tenant memory specification data found")
                return

            # Query tenant memory usage from __all_virtual_memory_info
            sql_memory_usage = """
                SELECT 
                    tenant_id,
                    svr_ip,
                    ROUND(SUM(hold) / 1024 / 1024 / 1024, 2) as memory_hold_gb,
                    ROUND(SUM(used) / 1024 / 1024 / 1024, 2) as memory_used_gb
                FROM 
                    oceanbase.__all_virtual_memory_info
                WHERE 
                    (tenant_id > 1000 OR tenant_id = 1)
                GROUP BY 
                    tenant_id, svr_ip
            """

            self.stdio.verbose("Querying tenant memory usage from __all_virtual_memory_info")
            memory_usage_results = self.ob_connector.execute_sql_return_cursor_dictionary(sql_memory_usage).fetchall()

            # Create a dictionary for quick lookup of memory usage
            memory_usage_dict = {}
            if memory_usage_results:
                for usage_row in memory_usage_results:
                    tenant_id = usage_row.get("tenant_id") or usage_row.get("TENANT_ID")
                    svr_ip = usage_row.get("svr_ip") or usage_row.get("SVR_IP")
                    key = "{0}_{1}".format(tenant_id, svr_ip)
                    memory_usage_dict[key] = {"hold_gb": usage_row.get("memory_hold_gb") or usage_row.get("MEMORY_HOLD_GB") or 0, "used_gb": usage_row.get("memory_used_gb") or usage_row.get("MEMORY_USED_GB") or 0}

            # Check each tenant's memory usage
            for row in memory_size_results:
                tenant_id = row.get("tenant_id") or row.get("TENANT_ID")
                tenant_name = row.get("tenant_name") or row.get("TENANT_NAME")
                svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                memory_size_gb = row.get("memory_size_gb") or row.get("MEMORY_SIZE_GB")

                if memory_size_gb is None or memory_size_gb <= 0:
                    continue

                # Get memory usage from dictionary
                key = "{0}_{1}".format(tenant_id, svr_ip)
                memory_hold_gb = memory_usage_dict.get(key, {}).get("hold_gb", 0)
                memory_used_gb = memory_usage_dict.get(key, {}).get("used_gb", 0)

                # Use hold_gb as it represents the actual memory held by the tenant
                memory_usage_gb = memory_hold_gb if memory_hold_gb > 0 else memory_used_gb

                if memory_size_gb > 0:
                    usage_percentage = (Decimal(str(memory_usage_gb)) / Decimal(str(memory_size_gb))) * 100

                    self.stdio.verbose("Tenant {0} (ID: {1}) on {2}: memory_usage={3}GB, memory_size={4}GB, usage_percentage={5}%".format(tenant_name, tenant_id, svr_ip, memory_usage_gb, memory_size_gb, round(float(usage_percentage), 2)))

                    if usage_percentage > 90:
                        self.report.add_warning(
                            "Tenant {0} (ID: {1}) on {2}: memory usage is {3}%, which exceeds 90%. "
                            "Memory usage: {4}GB, Memory specification: {5}GB".format(tenant_name, tenant_id, svr_ip, round(float(usage_percentage), 2), memory_usage_gb, memory_size_gb)
                        )

        except Exception as e:
            self.stdio.warn("Error checking tenant memory specification: {0}".format(e))
            self.report.add_fail("Error checking tenant memory specification: {0}".format(e))

    def _check_tablet_count_per_observer(self):
        """Check if tablet count per observer exceeds 90% threshold (900,000 tablets)"""
        try:
            # Query tablet count per observer
            # The recommended maximum is 1,000,000 tablets per observer, so 90% is 900,000
            sql = """
                SELECT 
                    svr_ip,
                    svr_port,
                    COUNT(*) as tablet_count
                FROM 
                    oceanbase.CDB_OB_TABLE_LOCATIONS
                GROUP BY 
                    svr_ip, svr_port
            """

            self.stdio.verbose("Querying tablet count per observer")
            results = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if results is None or len(results) == 0:
                self.stdio.verbose("No tablet count data found")
                return

            # Recommended maximum tablet count per observer is 1,000,000
            # 90% threshold is 900,000
            max_tablet_count = 1000000
            threshold_tablet_count = 900000

            for row in results:
                svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                svr_port = row.get("svr_port") or row.get("SVR_PORT")
                tablet_count = row.get("tablet_count") or row.get("TABLET_COUNT")

                if tablet_count is None:
                    continue

                tablet_count = int(tablet_count)
                usage_percentage = (Decimal(str(tablet_count)) / Decimal(str(max_tablet_count))) * 100

                self.stdio.verbose("Observer {0}:{1} has {2} tablets, usage percentage: {3}%".format(svr_ip, svr_port, tablet_count, round(float(usage_percentage), 2)))

                if tablet_count > threshold_tablet_count:
                    self.report.add_warning(
                        "Observer {0}:{1} has {2} tablets, which exceeds 90% threshold ({3} tablets). "
                        "Current usage: {4}%. Recommended maximum: {5} tablets per observer".format(svr_ip, svr_port, tablet_count, threshold_tablet_count, round(float(usage_percentage), 2), max_tablet_count)
                    )

        except Exception as e:
            self.stdio.warn("Error checking tablet count per observer: {0}".format(e))
            self.report.add_fail("Error checking tablet count per observer: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "tenant_memory_tablet_count",
            "info": "Check if tenant memory specification and tablet count per observer exceed 90% in health check",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1104",
        }


tenant_memory_tablet_count = TenantMemoryTabletCountTask()
