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
@time: 2026/03/02
@file: execute_memory_high.py
@desc: RCA scene for troubleshooting execute memory high (SQL work area memory)
       Reference: https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000000690996
       Issue: https://github.com/oceanbase/obdiag/issues/982
"""
import json
import os

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import DateTimeEncoder
from src.common.tool import StringUtils


class ExecuteMemoryHighScene(RcaScene):
    """
    RCA Scene for diagnosing execute memory high (SQL work area memory) issues.

    This scene checks for:
    1. SQL work area / execution memory usage from __all_virtual_memory_info
    2. GV$OB_SQL_WORKAREA_MEMORY_INFO (if available)
    3. GV$SQL_WORKAREA active work area (if available)
    4. ob_sql_work_area_percentage and related parameters
    5. Top memory-consuming SQL/modules
    """

    def __init__(self):
        super().__init__()
        self.work_path = None
        self.exec_memory_threshold_mb = 1024  # 1GB threshold for high execution memory

    def init(self, context):
        try:
            super().init(context)
            self.work_path = context.get_variable("store_dir")

            if self.observer_version is None:
                raise RCAInitException("observer version is None. Please check the NODES conf.")

            if not (self.observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0")):
                raise RCAInitException("observer version is {0}, which is less than 4.0.0.0. " "This RCA scene only supports OceanBase 4.x".format(self.observer_version))

            if self.ob_connector is None:
                raise RCAInitException("ob_connector is None. Please check the NODES conf.")

            if not os.path.exists(self.work_path):
                os.makedirs(self.work_path)

            self.record.add_record("ExecuteMemoryHighScene initialized successfully")
            self.stdio.verbose("[ExecuteMemoryHighScene] init completed, work_path: {0}".format(self.work_path))

        except Exception as e:
            raise RCAInitException("ExecuteMemoryHighScene init error: {0}".format(str(e)))

    def verbose(self, info):
        self.stdio.verbose("[ExecuteMemoryHighScene] {0}".format(info))

    def execute(self):
        """
        Execute the execute memory high diagnosis workflow.
        """
        try:
            self.record.add_record("Starting execute memory high diagnosis...")

            issue_detected = False

            # Step 1: Check execution-related memory from __all_virtual_memory_info
            if self._check_execution_memory():
                issue_detected = True

            # Step 2: Try GV$OB_SQL_WORKAREA_MEMORY_INFO (available in 4.3+)
            self._check_sql_workarea_memory_info()

            # Step 3: Try GV$SQL_WORKAREA for active work area
            self._check_sql_workarea()

            # Step 4: Check memory-related parameters
            self._check_memory_parameters()

            # Step 5: Collect tenant/server info and gather logs
            self._collect_diagnostic_info()

            # Step 6: Summary and suggestions
            if issue_detected:
                self.record.add_suggest("Execute memory high detected. Please review the diagnostic files in {0}. " "Reference: https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000000690996".format(self.work_path))
            else:
                self.record.add_record("No obvious execute memory high issue detected.")
                self.record.add_suggest(
                    "If you still experience execute memory issues, consider:\n" "1. Reducing ob_sql_work_area_percentage if it is too high\n" "2. Optimizing SQL with large sorts/hash joins\n" "3. Increasing memory_limit for the tenant if appropriate"
                )

        except RCANotNeedExecuteException as e:
            self.verbose("RCA not needed: {0}".format(str(e)))
            raise e
        except Exception as e:
            raise RCAExecuteException("ExecuteMemoryHighScene execute error: {0}".format(str(e)))
        finally:
            self.verbose("ExecuteMemoryHighScene execution completed")

    def _check_execution_memory(self):
        """
        Check execution-related memory from __all_virtual_memory_info.
        Focus on SQL/execution modules: ObSql, Sort, HashJoin, etc.
        """
        self.record.add_record("Step 1: Checking execution-related memory usage...")
        issue_detected = False

        try:
            sql = """
                SELECT
                    TENANT_ID,
                    SVR_IP,
                    SVR_PORT,
                    CTX_NAME,
                    MOD_NAME,
                    round(HOLD/1024/1024, 2) as HOLD_MB,
                    round(USED/1024/1024, 2) as USED_MB,
                    COUNT
                FROM oceanbase.__all_virtual_memory_info
                WHERE lower(MOD_NAME) LIKE '%sql%'
                   OR lower(MOD_NAME) LIKE '%sort%'
                   OR lower(MOD_NAME) LIKE '%hash%'
                   OR lower(MOD_NAME) LIKE '%join%'
                   OR lower(MOD_NAME) LIKE '%workarea%'
                   OR lower(MOD_NAME) LIKE '%work_area%'
                   OR lower(CTX_NAME) LIKE '%sql%'
                   OR lower(CTX_NAME) LIKE '%exec%'
                ORDER BY HOLD DESC
                LIMIT 200
            """
            self.verbose("Executing execution memory query")

            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            memory_data = cursor.fetchall()

            if memory_data:
                self._save_to_file("execution_memory_info.json", memory_data)

                total_sql_mb = sum(float(row.get("HOLD_MB", 0)) for row in memory_data)
                self.record.add_record("Execution-related memory usage: {0:.2f} MB".format(total_sql_mb))

                if total_sql_mb > self.exec_memory_threshold_mb:
                    issue_detected = True
                    self.record.add_record("WARNING: High execution memory detected ({0:.2f} MB > {1} MB threshold)".format(total_sql_mb, self.exec_memory_threshold_mb))

                # Top memory consumers
                top_5 = memory_data[:5]
                self.record.add_record("Top 5 execution memory consumers:")
                for row in top_5:
                    self.record.add_record(
                        "  {0} - {1}: {2} MB (tenant={3}, {4}:{5})".format(
                            row.get("MOD_NAME", ""),
                            row.get("CTX_NAME", ""),
                            row.get("HOLD_MB", 0),
                            row.get("TENANT_ID", ""),
                            row.get("SVR_IP", ""),
                            row.get("SVR_PORT", ""),
                        )
                    )
            else:
                self.record.add_record("No execution-related memory info found (query may need adjustment for your OB version)")

        except Exception as e:
            self.stdio.error("Error checking execution memory: {0}".format(str(e)))
            self.record.add_record("Failed to check execution memory: {0}".format(str(e)))

        return issue_detected

    def _check_sql_workarea_memory_info(self):
        """
        Query GV$OB_SQL_WORKAREA_MEMORY_INFO (available in OceanBase 4.3+).
        """
        self.record.add_record("Step 2: Checking GV$OB_SQL_WORKAREA_MEMORY_INFO...")

        try:
            sql = """
                SELECT * FROM oceanbase.GV$OB_SQL_WORKAREA_MEMORY_INFO
                ORDER BY hold_memory DESC
                LIMIT 100
            """
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            data = cursor.fetchall()

            if data:
                self._save_to_file("sql_workarea_memory_info.json", data)
                self.record.add_record("Found {0} work area memory records".format(len(data)))

                total_hold_mb = 0
                for row in data:
                    for key in ("hold_memory", "HOLD_MEMORY", "hold", "HOLD"):
                        if key in row and row[key] is not None:
                            try:
                                total_hold_mb += int(row[key]) / 1024 / 1024
                            except (ValueError, TypeError):
                                pass
                            break
                if total_hold_mb > 0:
                    self.record.add_record("Total work area hold: {0:.2f} MB".format(total_hold_mb))
                    if total_hold_mb > self.exec_memory_threshold_mb:
                        self.record.add_record("WARNING: High SQL work area memory usage detected")
            else:
                self.record.add_record("GV$OB_SQL_WORKAREA_MEMORY_INFO returned empty (or view not available in this version)")

        except Exception as e:
            self.verbose("GV$OB_SQL_WORKAREA_MEMORY_INFO not available or error: {0}".format(str(e)))
            self.record.add_record("GV$OB_SQL_WORKAREA_MEMORY_INFO: view may not exist in this version, skipped")

    def _check_sql_workarea(self):
        """
        Query GV$SQL_WORKAREA for active work area usage.
        """
        self.record.add_record("Step 3: Checking GV$SQL_WORKAREA...")

        try:
            sql = """
                SELECT * FROM oceanbase.GV$SQL_WORKAREA
                LIMIT 50
            """
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            data = cursor.fetchall()

            if data:
                self._save_to_file("sql_workarea_active.json", data)
                self.record.add_record("Found {0} work area records".format(len(data)))

                for row in data[:5]:
                    sql_id = row.get("sql_id") or row.get("SQL_ID", "")
                    op_type = row.get("operation_type") or row.get("OPERATION_TYPE", "")
                    size = row.get("workarea_size") or row.get("WORKAREA_SIZE", 0)
                    self.record.add_record("  sql_id={0}, operation_type={1}, workarea_size={2}".format(sql_id, op_type, size))
            else:
                self.record.add_record("No active work areas or view not available")

        except Exception as e:
            self.verbose("GV$SQL_WORKAREA not available or error: {0}".format(str(e)))
            self.record.add_record("GV$SQL_WORKAREA: view may not exist in this version, skipped")

    def _check_memory_parameters(self):
        """
        Check ob_sql_work_area_percentage and related memory parameters.
        """
        self.record.add_record("Step 4: Checking memory-related parameters...")

        try:
            sql = """
                SELECT TENANT_ID, SVR_IP, NAME, VALUE
                FROM oceanbase.GV$OB_PARAMETERS
                WHERE NAME IN (
                    'ob_sql_work_area_percentage',
                    'memory_limit',
                    'memstore_limit_percentage'
                )
                ORDER BY TENANT_ID, NAME
            """
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            params = cursor.fetchall()

            if params:
                self._save_to_file("memory_parameters.json", params)
                self.record.add_record("Memory parameters collected:")

                for p in params:
                    self.record.add_record(
                        "  tenant={0} {1} {2}={3}".format(
                            p.get("TENANT_ID", ""),
                            p.get("SVR_IP", ""),
                            p.get("NAME", ""),
                            p.get("VALUE", ""),
                        )
                    )

                work_area_pct = next((p for p in params if p.get("NAME") == "ob_sql_work_area_percentage"), None)
                if work_area_pct:
                    try:
                        pct_val = float(work_area_pct.get("VALUE", 0))
                        if pct_val > 50:
                            self.record.add_suggest("ob_sql_work_area_percentage is {0}%. Consider reducing if execute memory is high.".format(pct_val))
                    except (ValueError, TypeError):
                        pass
            else:
                self.record.add_record("No memory parameters found")

        except Exception as e:
            self.stdio.error("Error checking memory parameters: {0}".format(str(e)))
            self.record.add_record("Failed to check memory parameters: {0}".format(str(e)))

    def _collect_diagnostic_info(self):
        """
        Collect tenant/server info and optionally gather logs.
        """
        self.record.add_record("Step 5: Collecting diagnostic information...")

        try:
            # Tenant info
            sql = "SELECT TENANT_ID, TENANT_NAME, TENANT_TYPE, STATUS FROM oceanbase.DBA_OB_TENANTS ORDER BY TENANT_ID"
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            self._save_to_file("tenant_info.json", cursor.fetchall())

            # Server info
            sql = "SELECT SVR_IP, SVR_PORT, ZONE, STATUS FROM oceanbase.DBA_OB_SERVERS ORDER BY SVR_IP"
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            self._save_to_file("server_info.json", cursor.fetchall())

            # Tenant memory overview
            sql = """
                SELECT
                    TENANT_ID,
                    SVR_IP,
                    round(SUM(HOLD)/1024/1024/1024, 2) as TOTAL_HOLD_GB,
                    round(SUM(USED)/1024/1024/1024, 2) as TOTAL_USED_GB
                FROM oceanbase.__all_virtual_memory_info
                GROUP BY TENANT_ID, SVR_IP
                ORDER BY TOTAL_HOLD_GB DESC
            """
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            self._save_to_file("tenant_memory_overview.json", cursor.fetchall())

            self.record.add_record("Diagnostic info saved to {0}".format(self.work_path))

            # Gather observer logs (optional, for memory-related errors)
            try:
                self.gather_log.set_parameters("scope", "observer")
                log_path = os.path.join(self.work_path, "observer_logs")
                if not os.path.exists(log_path):
                    os.makedirs(log_path)
                logs = self.gather_log.execute(save_path=log_path)
                if logs:
                    self.record.add_record("Observer logs gathered to {0}".format(log_path))
            except Exception as log_e:
                self.verbose("Gather log skipped: {0}".format(str(log_e)))

        except Exception as e:
            self.verbose("Error collecting diagnostic info: {0}".format(str(e)))
            self.record.add_record("Some diagnostic information could not be collected: {0}".format(str(e)))

    def _save_to_file(self, filename, data):
        """Save data to a JSON file in the work directory."""
        try:
            filepath = os.path.join(self.work_path, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, cls=DateTimeEncoder, ensure_ascii=False)
            self.verbose("Saved data to {0}".format(filepath))
        except Exception as e:
            self.verbose("Failed to save {0}: {1}".format(filename, str(e)))

    def get_result(self):
        return self.Result

    def get_scene_info(self):
        return {
            "name": "execute_memory_high",
            "info_en": "Diagnose execute memory high (SQL work area) issues. Checks execution memory usage, work area, and related parameters. Supports OceanBase 4.0+.",
            "info_cn": "诊断执行内存高（SQL 工作区内存）问题。检查执行内存使用、work area 及相关参数。支持 OceanBase 4.0 及以上版本。",
            "example": "obdiag rca run --scene=execute_memory_high",
        }


execute_memory_high = ExecuteMemoryHighScene()
