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
@time: 2025/12/10
@file: schema_leak.py
@desc: RCA scene for troubleshooting schema or session leak issues
       Reference: https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000003210708
"""
import json
import os

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import DateTimeEncoder
from src.common.tool import StringUtils


class SchemaLeakScene(RcaScene):
    """
    RCA Scene for diagnosing schema or session leak issues.

    This scene checks for:
    1. Schema slot usage and potential leaks
    2. Long-running or idle sessions
    3. Schema-related memory consumption
    """

    def __init__(self):
        super().__init__()
        self.work_path = None
        # Thresholds for leak detection
        self.session_idle_threshold_seconds = 3600  # 1 hour
        self.schema_slot_ref_threshold = 100  # High reference count threshold

    def init(self, context):
        try:
            super().init(context)
            self.work_path = context.get_variable('store_dir')

            if self.observer_version is None:
                raise RCAInitException("observer version is None. Please check the NODES conf.")

            # Require OceanBase 4.0.0.0 or higher
            if not (self.observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(self.observer_version,
                                                                                               "4.0.0.0")):
                raise RCAInitException(
                    "observer version is {0}, which is less than 4.0.0.0. This RCA scene only supports OceanBase 4.x".format(
                        self.observer_version))

            if self.ob_connector is None:
                raise RCAInitException("ob_connector is None. Please check the NODES conf.")

            if not os.path.exists(self.work_path):
                os.makedirs(self.work_path)

            self.record.add_record("SchemaLeakScene initialized successfully")
            self.stdio.verbose("[SchemaLeakScene] init completed, work_path: {0}".format(self.work_path))

        except Exception as e:
            raise RCAInitException("SchemaLeakScene init error: {0}".format(str(e)))

    def verbose(self, info):
        self.stdio.verbose("[SchemaLeakScene] {0}".format(info))

    def execute(self):
        """
        Execute the schema leak diagnosis workflow:
        1. Check schema slot status
        2. Check for session leaks
        3. Check schema-related memory usage
        4. Analyze and provide suggestions
        """
        try:
            self.record.add_record("Starting schema/session leak diagnosis...")

            # Step 1: Check schema slot status
            schema_leak_detected = self._check_schema_slots()

            # Step 2: Check session status
            session_leak_detected = self._check_sessions()

            # Step 3: Check schema memory usage
            memory_issue_detected = self._check_schema_memory()

            # Step 4: Collect additional diagnostic info
            self._collect_diagnostic_info()

            # Step 5: Generate summary and suggestions
            if not schema_leak_detected and not session_leak_detected and not memory_issue_detected:
                self.record.add_record("No obvious schema or session leak detected.")
                self.record.add_suggest(
                    "Current system appears healthy. If you still suspect a leak, please check application connection pool settings and ensure connections are properly closed.")
            else:
                self.record.add_suggest(
                    "Please review the diagnostic files in {0} for detailed analysis.".format(self.work_path))

        except RCANotNeedExecuteException as e:
            self.verbose("RCA not needed: {0}".format(str(e)))
            raise e
        except Exception as e:
            raise RCAExecuteException("SchemaLeakScene execute error: {0}".format(str(e)))
        finally:
            self.verbose("SchemaLeakScene execution completed")

    def _check_schema_slots(self):
        """
        Check schema slot usage for potential leaks.
        Reference: GV$OB_SCHEMA_SLOT view
        """
        self.record.add_record("Step 1: Checking schema slot status...")
        leak_detected = False

        try:
            # Query schema slot status
            sql = """
                SELECT 
                    TENANT_ID,
                    SVR_IP,
                    SVR_PORT,
                    SLOT_ID,
                    SCHEMA_VERSION,
                    SCHEMA_COUNT,
                    TOTAL_REF_CNT,
                    REF_INFO
                FROM oceanbase.GV$OB_SCHEMA_SLOT
                WHERE TOTAL_REF_CNT > 0
                ORDER BY TOTAL_REF_CNT DESC
                LIMIT 100
            """
            self.verbose("Executing schema slot query: {0}".format(sql))

            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            schema_slot_data = cursor.fetchall()

            # Save to file
            self._save_to_file("schema_slot_status.json", schema_slot_data)

            if not schema_slot_data:
                self.record.add_record("No active schema slots found.")
                return False

            self.record.add_record("Found {0} active schema slots".format(len(schema_slot_data)))

            # Analyze for potential leaks
            high_ref_slots = []
            for slot in schema_slot_data:
                total_ref_cnt = int(slot.get('TOTAL_REF_CNT', 0))
                if total_ref_cnt > self.schema_slot_ref_threshold:
                    high_ref_slots.append(slot)
                    self.verbose("High reference count slot found: tenant_id={0}, slot_id={1}, ref_cnt={2}".format(
                        slot.get('TENANT_ID'), slot.get('SLOT_ID'), total_ref_cnt))

            if high_ref_slots:
                leak_detected = True
                self.record.add_record("WARNING: Found {0} schema slots with high reference count (>{1})".format(
                    len(high_ref_slots), self.schema_slot_ref_threshold))
                self._save_to_file("high_ref_schema_slots.json", high_ref_slots)

                # Provide suggestions
                self.record.add_suggest(
                    "High schema slot reference count detected. This may indicate:\n"
                    "1. Schema version not being released properly\n"
                    "2. Long-running transactions holding old schema versions\n"
                    "3. Application connection pool not refreshing connections\n"
                    "Suggestion: Check REF_INFO column for details on what is holding references."
                )

            # Check for schema version fragmentation
            self._check_schema_version_fragmentation()

        except Exception as e:
            self.verbose("Error checking schema slots: {0}".format(str(e)))
            self.record.add_record("Failed to check schema slots: {0}".format(str(e)))

        return leak_detected

    def _check_schema_version_fragmentation(self):
        """Check for schema version fragmentation across tenants"""
        try:
            sql = """
                SELECT 
                    TENANT_ID,
                    COUNT(DISTINCT SCHEMA_VERSION) as VERSION_COUNT,
                    MIN(SCHEMA_VERSION) as MIN_VERSION,
                    MAX(SCHEMA_VERSION) as MAX_VERSION
                FROM oceanbase.GV$OB_SCHEMA_SLOT
                GROUP BY TENANT_ID
                HAVING COUNT(DISTINCT SCHEMA_VERSION) > 10
                ORDER BY VERSION_COUNT DESC
            """
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            fragmentation_data = cursor.fetchall()

            if fragmentation_data:
                self.record.add_record(
                    "Schema version fragmentation detected in {0} tenants".format(len(fragmentation_data)))
                self._save_to_file("schema_fragmentation.json", fragmentation_data)
                self.record.add_suggest(
                    "Schema version fragmentation found. Too many schema versions being held may indicate:\n"
                    "1. Frequent DDL operations\n"
                    "2. Long-running queries preventing schema garbage collection\n"
                    "Suggestion: Review DDL frequency and check for long-running transactions."
                )
        except Exception as e:
            self.verbose("Error checking schema fragmentation: {0}".format(str(e)))

    def _check_sessions(self):
        """
        Check for session leaks - idle or long-running sessions.
        """
        self.record.add_record("Step 2: Checking session status...")
        leak_detected = False

        try:
            # Query active sessions
            sql = """
                SELECT 
                    ID,
                    TENANT_ID,
                    USER,
                    HOST,
                    DB,
                    COMMAND,
                    TIME,
                    STATE,
                    INFO,
                    SVR_IP,
                    SVR_PORT,
                    SQL_ID,
                    TRANS_ID,
                    THREAD_ID,
                    SSL_CIPHER,
                    TRACE_ID,
                    TRANS_STATE,
                    TOTAL_TIME,
                    RETRY_CNT,
                    RETRY_INFO
                FROM oceanbase.GV$OB_PROCESSLIST
                ORDER BY TIME DESC
                LIMIT 500
            """
            self.verbose("Executing session query")

            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            session_data = cursor.fetchall()

            self._save_to_file("all_sessions.json", session_data)
            self.record.add_record("Found {0} active sessions".format(len(session_data)))

            # Analyze for potential leaks
            idle_sessions = []
            long_running_sessions = []

            for session in session_data:
                session_time = int(session.get('TIME', 0))
                command = session.get('COMMAND', '')
                state = session.get('STATE', '')

                # Check for idle sessions (Sleep command for too long)
                if command == 'Sleep' and session_time > self.session_idle_threshold_seconds:
                    idle_sessions.append(session)

                # Check for long-running queries
                elif command == 'Query' and session_time > 3600:  # > 1 hour
                    long_running_sessions.append(session)

            if idle_sessions:
                leak_detected = True
                self.record.add_record("WARNING: Found {0} idle sessions (idle > {1} seconds)".format(
                    len(idle_sessions), self.session_idle_threshold_seconds))
                self._save_to_file("idle_sessions.json", idle_sessions)

                # Group by host for analysis
                host_counts = {}
                for s in idle_sessions:
                    host = s.get('HOST', 'unknown')
                    host_counts[host] = host_counts.get(host, 0) + 1

                self.record.add_record("Idle sessions by host: {0}".format(host_counts))
                self.record.add_suggest(
                    "Idle session leak detected. Suggestions:\n"
                    "1. Check application connection pool configuration (max idle time, validation query)\n"
                    "2. Ensure connections are properly returned to pool after use\n"
                    "3. Consider setting wait_timeout parameter to automatically close idle connections\n"
                    "4. Top idle session hosts: {0}".format(str(host_counts)[:500])
                )

            if long_running_sessions:
                leak_detected = True
                self.record.add_record(
                    "WARNING: Found {0} long-running sessions (> 1 hour)".format(len(long_running_sessions)))
                self._save_to_file("long_running_sessions.json", long_running_sessions)
                self.record.add_suggest(
                    "Long-running sessions detected. This may cause:\n"
                    "1. Schema versions being held (preventing schema GC)\n"
                    "2. Lock contention\n"
                    "3. Memory pressure\n"
                    "Suggestion: Review and optimize long-running queries, or split them into smaller transactions."
                )

            # Check session count per host
            self._check_session_distribution(session_data)

        except Exception as e:
            self.verbose("Error checking sessions: {0}".format(str(e)))
            self.record.add_record("Failed to check sessions: {0}".format(str(e)))

        return leak_detected

    def _check_session_distribution(self, session_data):
        """Check session distribution across hosts for potential connection leaks"""
        try:
            host_counts = {}
            for session in session_data:
                host = session.get('HOST', 'unknown')
                if host:
                    # Extract IP part
                    host_ip = host.split(':')[0] if ':' in host else host
                    host_counts[host_ip] = host_counts.get(host_ip, 0) + 1

            # Sort by count
            sorted_hosts = sorted(host_counts.items(), key=lambda x: x[1], reverse=True)

            if sorted_hosts:
                self.record.add_record("Session distribution by client host (top 10):")
                for host, count in sorted_hosts[:10]:
                    self.record.add_record("  {0}: {1} sessions".format(host, count))

                # Warn if single host has too many connections
                if sorted_hosts[0][1] > 100:
                    self.record.add_suggest(
                        "Host {0} has {1} connections. This may indicate:\n"
                        "1. Connection pool misconfiguration\n"
                        "2. Connection leak in application\n"
                        "3. Missing connection close in application code".format(
                            sorted_hosts[0][0], sorted_hosts[0][1])
                    )

        except Exception as e:
            self.verbose("Error analyzing session distribution: {0}".format(str(e)))

    def _check_schema_memory(self):
        """
        Check schema-related memory consumption.
        """
        self.record.add_record("Step 3: Checking schema-related memory usage...")
        issue_detected = False

        try:
            # Query schema-related memory
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
                WHERE MOD_NAME LIKE '%schema%' 
                   OR MOD_NAME LIKE '%Schema%'
                   OR CTX_NAME LIKE '%schema%'
                   OR CTX_NAME LIKE '%Schema%'
                ORDER BY HOLD DESC
                LIMIT 100
            """
            self.verbose("Executing schema memory query")

            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            memory_data = cursor.fetchall()

            self._save_to_file("schema_memory_usage.json", memory_data)

            if memory_data:
                total_schema_memory_mb = sum(float(m.get('HOLD_MB', 0)) for m in memory_data)
                self.record.add_record("Total schema-related memory: {0:.2f} MB".format(total_schema_memory_mb))

                # Check for unusually high memory usage
                for mem in memory_data:
                    hold_mb = float(mem.get('HOLD_MB', 0))
                    if hold_mb > 1024:  # > 1GB
                        issue_detected = True
                        self.record.add_record("WARNING: High schema memory usage: {0} - {1} MB on {2}:{3}".format(
                            mem.get('MOD_NAME'), hold_mb, mem.get('SVR_IP'), mem.get('SVR_PORT')))

                if issue_detected:
                    self.record.add_suggest(
                        "High schema memory usage detected. This may be caused by:\n"
                        "1. Many schema versions being retained\n"
                        "2. Large number of tables/indexes\n"
                        "3. Schema leak preventing garbage collection\n"
                        "Suggestion: Review schema slot status and clear long-running sessions."
                    )

            # Also check general memory to provide context
            self._check_tenant_memory_overview()

        except Exception as e:
            self.verbose("Error checking schema memory: {0}".format(str(e)))
            self.record.add_record("Failed to check schema memory: {0}".format(str(e)))

        return issue_detected

    def _check_tenant_memory_overview(self):
        """Check overall tenant memory for context"""
        try:
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
            memory_overview = cursor.fetchall()

            if memory_overview:
                self._save_to_file("tenant_memory_overview.json", memory_overview)
                self.record.add_record("Tenant memory overview saved to tenant_memory_overview.json")

        except Exception as e:
            self.verbose("Error getting tenant memory overview: {0}".format(str(e)))

    def _collect_diagnostic_info(self):
        """Collect additional diagnostic information"""
        self.record.add_record("Step 4: Collecting additional diagnostic information...")

        try:
            # Collect tenant information
            sql = "SELECT TENANT_ID, TENANT_NAME, TENANT_TYPE, STATUS FROM oceanbase.DBA_OB_TENANTS ORDER BY TENANT_ID"
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            tenant_data = cursor.fetchall()
            self._save_to_file("tenant_info.json", tenant_data)

            # Collect server information
            sql = "SELECT SVR_IP, SVR_PORT, ZONE, STATUS, START_SERVICE_TIME FROM oceanbase.DBA_OB_SERVERS ORDER BY SVR_IP"
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            server_data = cursor.fetchall()
            self._save_to_file("server_info.json", server_data)

            # Collect wait_timeout parameter
            sql = "SELECT TENANT_ID, NAME, VALUE FROM oceanbase.GV$OB_PARAMETERS WHERE NAME IN ('wait_timeout', 'interactive_timeout', 'net_read_timeout', 'net_write_timeout') ORDER BY TENANT_ID, NAME"
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            timeout_params = cursor.fetchall()
            self._save_to_file("timeout_parameters.json", timeout_params)

            if timeout_params:
                self.record.add_record(
                    "Timeout parameters collected. Review these settings if session leaks are suspected.")

            # Collect schema history info (for understanding schema change frequency)
            try:
                sql = """
                    SELECT 
                        TENANT_ID,
                        COUNT(*) as DDL_COUNT
                    FROM oceanbase.__all_virtual_core_all_table
                    WHERE GMT_MODIFIED > DATE_SUB(NOW(), INTERVAL 24 HOUR)
                    GROUP BY TENANT_ID
                """
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
                ddl_frequency = cursor.fetchall()
                if ddl_frequency:
                    self._save_to_file("recent_ddl_frequency.json", ddl_frequency)
                    self.record.add_record("Recent DDL activity (last 24h) saved.")
            except Exception:
                self.verbose("Could not collect DDL frequency information")

        except Exception as e:
            self.verbose("Error collecting diagnostic info: {0}".format(str(e)))
            self.record.add_record("Some diagnostic information could not be collected: {0}".format(str(e)))

    def _save_to_file(self, filename, data):
        """Save data to a JSON file in the work directory"""
        try:
            filepath = os.path.join(self.work_path, filename)
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, cls=DateTimeEncoder, ensure_ascii=False)
            self.verbose("Saved data to {0}".format(filepath))
        except Exception as e:
            self.verbose("Failed to save {0}: {1}".format(filename, str(e)))

    def get_scene_info(self):
        return {
            "name": "schema_leak",
            "info_en": "Diagnose schema or session leak issues. Checks schema slot usage, session status, and schema-related memory consumption.",
            "info_cn": "诊断 schema 或 session 泄露问题。检查 schema slot 使用情况、session 状态和 schema 相关内存消耗。"
        }


# Register the scene
schema_leak = SchemaLeakScene()

