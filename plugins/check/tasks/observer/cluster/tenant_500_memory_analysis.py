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
@time: 2025/01/01
@file: tenant_500_memory_analysis.py
@desc: Check tenant 500 memory usage and analyze memory modules for OceanBase.
       Tenant 500 is the internal system tenant responsible for system-level memory management.
       This task helps identify memory anomalies in tenant 500.
       issue: https://github.com/oceanbase/obdiag/issues/99
"""

from decimal import Decimal

from src.handler.check.check_task import TaskBase


class Tenant500MemoryAnalysisTask(TaskBase):
    # Default thresholds
    TENANT_500_TOTAL_MEMORY_WARNING_GB = 10  # Warning if tenant 500 total memory > 10GB
    TENANT_500_TOTAL_MEMORY_CRITICAL_GB = 20  # Critical if tenant 500 total memory > 20GB
    SINGLE_MOD_MEMORY_WARNING_GB = 2  # Warning if single module memory > 2GB
    SINGLE_MOD_MEMORY_CRITICAL_GB = 5  # Critical if single module memory > 5GB
    TOP_N_MODULES = 20  # Number of top modules to analyze

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("Database connection is not available.")

            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("This task requires OceanBase version >= 4.0.0.0, current version: {0}".format(self.observer_version))

            self.stdio.verbose("Starting tenant 500 memory analysis")

            # Step 1: Check overall tenant 500 memory usage
            self._check_tenant_500_total_memory()

            # Step 2: Analyze top memory-consuming modules in tenant 500
            self._analyze_top_memory_modules()

            # Step 3: Check specific known problematic modules
            self._check_known_problematic_modules()

            # Step 4: Compare tenant 500 memory with total cluster memory
            self._check_tenant_500_memory_ratio()

            self.stdio.verbose("Tenant 500 memory analysis completed")

        except Exception as e:
            self.stdio.error("Execute error: {0}".format(e))
            return self.report.add_fail("Tenant 500 memory analysis execution error: {0}".format(e))

    def _check_tenant_500_total_memory(self):
        """Check the total memory usage of tenant 500"""
        try:
            sql = """
                SELECT 
                    svr_ip,
                    svr_port,
                    ROUND(SUM(hold) / 1024 / 1024 / 1024, 2) AS total_hold_gb,
                    ROUND(SUM(used) / 1024 / 1024 / 1024, 2) AS total_used_gb,
                    COUNT(*) AS mod_count
                FROM oceanbase.__all_virtual_memory_info
                WHERE tenant_id = 500
                GROUP BY svr_ip, svr_port
                ORDER BY total_hold_gb DESC
            """

            self.stdio.verbose("Querying tenant 500 total memory usage")
            results = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not results:
                self.stdio.verbose("No memory data found for tenant 500")
                return

            for row in results:
                svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                svr_port = row.get("svr_port") or row.get("SVR_PORT")
                total_hold_gb = float(row.get("total_hold_gb") or row.get("TOTAL_HOLD_GB") or 0)
                total_used_gb = float(row.get("total_used_gb") or row.get("TOTAL_USED_GB") or 0)
                mod_count = row.get("mod_count") or row.get("MOD_COUNT") or 0

                self.stdio.verbose("Tenant 500 on {0}:{1}: hold={2}GB, used={3}GB, module_count={4}".format(svr_ip, svr_port, total_hold_gb, total_used_gb, mod_count))

                if total_hold_gb >= self.TENANT_500_TOTAL_MEMORY_CRITICAL_GB:
                    self.report.add_critical(
                        "Tenant 500 memory on {0}:{1} is {2}GB (hold), exceeding critical threshold {3}GB. "
                        "This may indicate memory leak or abnormal memory allocation in system modules. "
                        "Please check __all_virtual_memory_info for detailed module analysis.".format(svr_ip, svr_port, total_hold_gb, self.TENANT_500_TOTAL_MEMORY_CRITICAL_GB)
                    )
                elif total_hold_gb >= self.TENANT_500_TOTAL_MEMORY_WARNING_GB:
                    self.report.add_warning(
                        "Tenant 500 memory on {0}:{1} is {2}GB (hold), exceeding warning threshold {3}GB. " "Monitor this value and investigate if it continues to grow.".format(svr_ip, svr_port, total_hold_gb, self.TENANT_500_TOTAL_MEMORY_WARNING_GB)
                    )

        except Exception as e:
            self.stdio.warn("Error checking tenant 500 total memory: {0}".format(e))
            self.report.add_fail("Error checking tenant 500 total memory: {0}".format(e))

    def _analyze_top_memory_modules(self):
        """Analyze top memory-consuming modules in tenant 500"""
        try:
            sql = """
                SELECT 
                    svr_ip,
                    svr_port,
                    ctx_name,
                    mod_name,
                    ROUND(hold / 1024 / 1024 / 1024, 2) AS hold_gb,
                    ROUND(used / 1024 / 1024 / 1024, 2) AS used_gb,
                    count
                FROM oceanbase.__all_virtual_memory_info
                WHERE tenant_id = 500
                ORDER BY hold DESC
                LIMIT {0}
            """.format(
                self.TOP_N_MODULES
            )

            self.stdio.verbose("Querying top {0} memory modules in tenant 500".format(self.TOP_N_MODULES))
            results = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not results:
                self.stdio.verbose("No memory module data found for tenant 500")
                return

            for row in results:
                svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                svr_port = row.get("svr_port") or row.get("SVR_PORT")
                ctx_name = row.get("ctx_name") or row.get("CTX_NAME")
                mod_name = row.get("mod_name") or row.get("MOD_NAME")
                hold_gb = float(row.get("hold_gb") or row.get("HOLD_GB") or 0)
                used_gb = float(row.get("used_gb") or row.get("USED_GB") or 0)
                count = row.get("count") or row.get("COUNT") or 0

                self.stdio.verbose("Module {0}.{1} on {2}:{3}: hold={4}GB, used={5}GB, count={6}".format(ctx_name, mod_name, svr_ip, svr_port, hold_gb, used_gb, count))

                if hold_gb >= self.SINGLE_MOD_MEMORY_CRITICAL_GB:
                    self.report.add_critical(
                        "Tenant 500 module [{0}].{1} on {2}:{3} uses {4}GB memory (hold), "
                        "exceeding critical threshold {5}GB. "
                        "This module may have memory leak or abnormal allocation.".format(ctx_name, mod_name, svr_ip, svr_port, hold_gb, self.SINGLE_MOD_MEMORY_CRITICAL_GB)
                    )
                elif hold_gb >= self.SINGLE_MOD_MEMORY_WARNING_GB:
                    self.report.add_warning("Tenant 500 module [{0}].{1} on {2}:{3} uses {4}GB memory (hold), " "exceeding warning threshold {5}GB.".format(ctx_name, mod_name, svr_ip, svr_port, hold_gb, self.SINGLE_MOD_MEMORY_WARNING_GB))

        except Exception as e:
            self.stdio.warn("Error analyzing top memory modules: {0}".format(e))
            self.report.add_fail("Error analyzing top memory modules: {0}".format(e))

    def _check_known_problematic_modules(self):
        """Check specific modules that are commonly associated with memory issues"""
        try:
            # List of known modules that may cause memory issues
            known_modules = [
                "LIBEASY",
                "SqlPlanMgr",
                "SqlPlanCache",
                "OB_KVSTORE_CACHE",
                "SqlDtl",
                "SqlArea",
                "Schema",
                "LocationCache",
                "TabletHandle",
            ]

            module_conditions = " OR ".join(["UPPER(mod_name) LIKE '%{0}%'".format(mod.upper()) for mod in known_modules])

            sql = """
                SELECT 
                    svr_ip,
                    svr_port,
                    ctx_name,
                    mod_name,
                    ROUND(hold / 1024 / 1024 / 1024, 2) AS hold_gb,
                    ROUND(used / 1024 / 1024 / 1024, 2) AS used_gb,
                    count
                FROM oceanbase.__all_virtual_memory_info
                WHERE tenant_id = 500 
                  AND ({0})
                  AND hold > 1073741824
                ORDER BY hold DESC
            """.format(
                module_conditions
            )

            self.stdio.verbose("Checking known problematic modules in tenant 500")
            results = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not results:
                self.stdio.verbose("No known problematic modules found with high memory usage")
                return

            for row in results:
                svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                svr_port = row.get("svr_port") or row.get("SVR_PORT")
                ctx_name = row.get("ctx_name") or row.get("CTX_NAME")
                mod_name = row.get("mod_name") or row.get("MOD_NAME")
                hold_gb = float(row.get("hold_gb") or row.get("HOLD_GB") or 0)

                suggestion = self._get_module_suggestion(mod_name)

                self.report.add_warning("Known problematic module [{0}].{1} on {2}:{3} uses {4}GB memory. {5}".format(ctx_name, mod_name, svr_ip, svr_port, hold_gb, suggestion))

        except Exception as e:
            self.stdio.warn("Error checking known problematic modules: {0}".format(e))

    def _get_module_suggestion(self, mod_name):
        """Get troubleshooting suggestion based on module name"""
        mod_name_upper = mod_name.upper() if mod_name else ""

        if "LIBEASY" in mod_name_upper:
            return "LIBEASY module handles network connections. High memory may indicate connection leak or too many connections."
        elif "SQLPLAN" in mod_name_upper or "PLANCACHE" in mod_name_upper:
            return "SQL Plan Cache memory may be too large. Consider reducing ob_plan_cache_percentage or clearing plan cache."
        elif "SQLAREA" in mod_name_upper:
            return "SQL Area memory is high. Check for SQL memory leaks or reduce concurrent queries."
        elif "SCHEMA" in mod_name_upper:
            return "Schema memory is high. This may indicate schema leak. Check for long transactions or idle sessions."
        elif "LOCATIONCACHE" in mod_name_upper:
            return "Location Cache memory is high. This may relate to partition count or location refresh."
        elif "TABLETHANDLE" in mod_name_upper:
            return "TabletHandle memory is high. Check tablet count and potential tablet leaks."
        elif "KVSTORE" in mod_name_upper:
            return "KVStore Cache memory is high. Consider adjusting cache size parameters."
        else:
            return "Please investigate this module's memory usage pattern."

    def _check_tenant_500_memory_ratio(self):
        """Check the ratio of tenant 500 memory to total cluster memory"""
        try:
            # Get total memory across all tenants
            sql_total = """
                SELECT 
                    svr_ip,
                    svr_port,
                    ROUND(SUM(hold) / 1024 / 1024 / 1024, 2) AS total_hold_gb
                FROM oceanbase.__all_virtual_memory_info
                GROUP BY svr_ip, svr_port
            """

            # Get tenant 500 memory
            sql_tenant_500 = """
                SELECT 
                    svr_ip,
                    svr_port,
                    ROUND(SUM(hold) / 1024 / 1024 / 1024, 2) AS tenant_500_hold_gb
                FROM oceanbase.__all_virtual_memory_info
                WHERE tenant_id = 500
                GROUP BY svr_ip, svr_port
            """

            self.stdio.verbose("Calculating tenant 500 memory ratio")

            total_results = self.ob_connector.execute_sql_return_cursor_dictionary(sql_total).fetchall()
            tenant_500_results = self.ob_connector.execute_sql_return_cursor_dictionary(sql_tenant_500).fetchall()

            if not total_results or not tenant_500_results:
                self.stdio.verbose("Unable to calculate memory ratio due to missing data")
                return

            # Create lookup dictionary for tenant 500 memory
            tenant_500_dict = {}
            for row in tenant_500_results:
                svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                svr_port = row.get("svr_port") or row.get("SVR_PORT")
                hold_gb = float(row.get("tenant_500_hold_gb") or row.get("TENANT_500_HOLD_GB") or 0)
                key = "{0}:{1}".format(svr_ip, svr_port)
                tenant_500_dict[key] = hold_gb

            for row in total_results:
                svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                svr_port = row.get("svr_port") or row.get("SVR_PORT")
                total_hold_gb = float(row.get("total_hold_gb") or row.get("TOTAL_HOLD_GB") or 0)

                key = "{0}:{1}".format(svr_ip, svr_port)
                tenant_500_hold_gb = tenant_500_dict.get(key, 0)

                if total_hold_gb > 0:
                    ratio = (Decimal(str(tenant_500_hold_gb)) / Decimal(str(total_hold_gb))) * 100
                    ratio_float = float(ratio)

                    self.stdio.verbose("Server {0}: tenant 500 memory = {1}GB, total memory = {2}GB, ratio = {3}%".format(key, tenant_500_hold_gb, total_hold_gb, round(ratio_float, 2)))

                    # Warning if tenant 500 takes more than 30% of total memory
                    if ratio_float > 30:
                        self.report.add_warning(
                            "Tenant 500 on {0} consumes {1}% of total cluster memory ({2}GB / {3}GB). "
                            "This ratio is higher than expected. Please investigate system-level memory usage.".format(key, round(ratio_float, 2), tenant_500_hold_gb, total_hold_gb)
                        )

        except Exception as e:
            self.stdio.warn("Error checking tenant 500 memory ratio: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "tenant_500_memory_analysis",
            "info": "Analyze tenant 500 (internal system tenant) memory usage to identify memory anomalies. " "Checks total memory, top modules, known problematic modules, and memory ratio. " "issue: https://github.com/oceanbase/obdiag/issues/99",
        }


tenant_500_memory_analysis = Tenant500MemoryAnalysisTask()
