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
@time: 2024/1/2
@file: major_hold.py
@desc: RCA scene for diagnosing major compaction hold issues
       Reference: https://open.oceanbase.com/blog/14847857236
"""
import json
import os.path
import re
from src.handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
    RCANotNeedExecuteException,
)
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import DateTimeEncoder
from src.common.tool import StringUtils


class MajorHoldScene(RcaScene):
    """
    RCA Scene for diagnosing major compaction hold issues.

    This scene checks for:
    1. Compaction errors (IS_ERROR='YES')
    2. Compaction failures from diagnose info
    3. Long-running compaction tasks
    4. Suspended compactions
    5. Compaction speed analysis
    6. Duplicate index names across tables (Issue #607)
    7. DDL task status that may block compaction
    8. Memory throttling issues causing merge failures (Issue #1107)

    References:
    - https://open.oceanbase.com/blog/14847857236
    - https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000001843060
    - https://github.com/oceanbase/obdiag/issues/1107
    """

    # Configurable thresholds
    COMPACTION_TIMEOUT_MINUTES = 20  # Consider compaction stuck if running longer than this
    SPEED_RATIO_THRESHOLD = 5  # Alert if current speed is 5x slower than previous

    def __init__(self):
        super().__init__()
        self.local_path = ""

    def _find_observer_node(self, svr_ip, svr_port=None):
        """
        Find observer node by IP and optional port

        Args:
            svr_ip: Server IP address
            svr_port: Server port (optional)

        Returns:
            Tuple of (node, ssh_client) or (None, None) if not found
        """
        for observer_node in self.observer_nodes:
            if observer_node.get("ip") == svr_ip:
                if svr_port is None or str(observer_node.get("port")) == str(svr_port):
                    return observer_node, observer_node.get("ssher")
        return None, None

    def _save_to_file(self, filename, data, tenant_id=None):
        """
        Save data to file in the local path

        Args:
            filename: Base filename
            data: Data to save (will be JSON serialized)
            tenant_id: Optional tenant ID to include in filename
        """
        try:
            if tenant_id:
                file_path = os.path.join(self.local_path, "rca_major_hold_{0}_{1}".format(tenant_id, filename))
            else:
                file_path = os.path.join(self.local_path, "rca_major_hold_{0}".format(filename))

            with open(file_path, "w", encoding="utf-8") as f:
                if isinstance(data, (list, dict)):
                    json.dump(data, f, cls=DateTimeEncoder, indent=2, ensure_ascii=False)
                else:
                    f.write(str(data))
            self.stdio.verbose("Saved data to {0}".format(file_path))
            return file_path
        except Exception as e:
            self.stdio.warn("Failed to save file {0}: {1}".format(filename, e))
            return None

    def _execute_sql_safe(self, sql, description=""):
        """
        Execute SQL safely with error handling

        Args:
            sql: SQL statement to execute
            description: Description for logging

        Returns:
            Query results or empty list on error
        """
        try:
            self.stdio.verbose("Executing SQL ({0}): {1}".format(description, sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            return cursor.fetchall()
        except Exception as e:
            self.stdio.warn("SQL execution failed ({0}): {1}".format(description, e))
            return []

    def init(self, context):
        try:
            super().init(context)
            self.local_path = context.get_variable('store_dir')

            if self.observer_version is None:
                raise Exception("observer version is None. Please check the NODES conf.")

            if not (self.observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0")):
                raise Exception("observer version must be greater than 4.0.0.0. Please check the NODES conf.")

            if not os.path.exists(self.local_path):
                os.makedirs(self.local_path)

            self.stdio.verbose("MajorHoldScene initialized, work_path: {0}".format(self.local_path))

        except Exception as e:
            raise RCAInitException("MajorHoldScene RCAInitException: {0}".format(e))

    def execute(self):
        need_tag = False
        err_tenant_ids = []
        self.record.add_record("Starting major compaction hold diagnosis...")

        # Step 1: Check for compaction errors (IS_ERROR='YES')
        try:
            self.record.add_record("Step 1: Checking CDB_OB_MAJOR_COMPACTION for errors")
            sql = 'SELECT * FROM oceanbase.CDB_OB_MAJOR_COMPACTION WHERE IS_ERROR="YES";'
            error_data = self._execute_sql_safe(sql, "check IS_ERROR")

            if len(error_data) == 0:
                self.record.add_record("No compaction errors found (IS_ERROR='YES')")
            else:
                need_tag = True
                error_tenant_ids = [str(data.get('TENANT_ID')) for data in error_data]
                self.record.add_record("Found compaction errors in tenants: {0}".format(error_tenant_ids))
                self._save_to_file("compaction_errors.json", error_data)
                err_tenant_ids.extend(error_tenant_ids)
        except Exception as e:
            self.stdio.warn("Error checking CDB_OB_MAJOR_COMPACTION: {0}".format(e))
            raise RCAExecuteException("Error checking CDB_OB_MAJOR_COMPACTION: {0}".format(e))

        # Step 2: Check for suspended compactions
        try:
            self.record.add_record("Step 2: Checking for suspended compactions")
            sql = 'SELECT * FROM oceanbase.CDB_OB_MAJOR_COMPACTION WHERE IS_SUSPENDED="YES";'
            suspended_data = self._execute_sql_safe(sql, "check IS_SUSPENDED")

            if len(suspended_data) == 0:
                self.record.add_record("No suspended compactions found")
            else:
                need_tag = True
                suspended_tenant_ids = [str(data.get('TENANT_ID')) for data in suspended_data]
                self.record.add_record("Found suspended compactions in tenants: {0}".format(suspended_tenant_ids))
                self._save_to_file("suspended_compactions.json", suspended_data)
                self.record.add_suggest("Suspended compaction detected. To resume, execute: ALTER SYSTEM RESUME MERGE TENANT = tenant_name;")
                err_tenant_ids.extend(suspended_tenant_ids)
        except Exception as e:
            self.stdio.warn("Error checking suspended compactions: {0}".format(e))

        # Step 3: Check diagnose info for failures
        try:
            self.record.add_record("Step 3: Checking __all_virtual_compaction_diagnose_info for failures")
            sql = 'SELECT * FROM oceanbase.__all_virtual_compaction_diagnose_info WHERE status="FAILED";'
            diagnose_data = self._execute_sql_safe(sql, "check diagnose_info FAILED")

            if len(diagnose_data) == 0:
                self.record.add_record("No failed compaction tasks in diagnose info")
            else:
                need_tag = True
                diagnose_tenant_ids = list(set([str(data.get("tenant_id")) for data in diagnose_data]))
                self.record.add_record("Found failed compaction tasks in tenants: {0}".format(diagnose_tenant_ids))
                self._save_to_file("diagnose_info_failed.json", diagnose_data)
                err_tenant_ids.extend(diagnose_tenant_ids)
        except Exception as e:
            self.stdio.warn("Error checking diagnose info: {0}".format(e))
            raise RCAExecuteException("Error checking diagnose info: {0}".format(e))

        # Step 4: Check for long-running compaction tasks
        try:
            self.record.add_record("Step 4: Checking for long-running compaction tasks (>{0} minutes)".format(self.COMPACTION_TIMEOUT_MINUTES))
            sql = """
                SELECT * FROM oceanbase.GV$OB_COMPACTION_PROGRESS 
                WHERE STATUS <> 'FINISH' 
                AND START_TIME <= NOW() - INTERVAL {0} MINUTE 
                ORDER BY TENANT_ID, SVR_IP;
            """.format(
                self.COMPACTION_TIMEOUT_MINUTES
            )
            running_data = self._execute_sql_safe(sql, "check long-running tasks")

            if len(running_data) == 0:
                self.record.add_record("No long-running compaction tasks found")
            else:
                need_tag = True
                timeout_tenant_ids = list(set([str(data.get("TENANT_ID")) for data in running_data]))
                self.record.add_record("Found long-running compaction tasks in tenants: {0}".format(timeout_tenant_ids))
                self._save_to_file("long_running_tasks.json", running_data)
                err_tenant_ids.extend(timeout_tenant_ids)
        except Exception as e:
            self.stdio.warn("Error checking long-running tasks: {0}".format(e))
            raise RCAExecuteException("Error checking GV$OB_COMPACTION_PROGRESS: {0}".format(e))

        # Step 5: Check compaction speed (compare current vs last)
        try:
            self.record.add_record("Step 5: Analyzing compaction speed")
            self._check_compaction_speed()
        except Exception as e:
            self.stdio.warn("Error analyzing compaction speed: {0}".format(e))

        if not need_tag:
            self.record.add_suggest("No major compaction issues detected")
            raise RCANotNeedExecuteException("MajorHoldScene not need execute - no issues found")

        err_tenant_ids = list(set(err_tenant_ids))
        self.record.add_record("Tenants requiring detailed diagnosis: {0}".format(err_tenant_ids))

        # Detailed diagnosis for each tenant
        for err_tenant_id in err_tenant_ids:
            self._diagnose_tenant(err_tenant_id)

    def _check_compaction_speed(self):
        """
        Check compaction speed by comparing current vs last compaction
        """
        sql = """
            SELECT 
                TENANT_ID,
                SVR_IP,
                SVR_PORT,
                COMPACTION_SCN,
                STATUS,
                DATA_SIZE,
                UNFINISHED_DATA_SIZE,
                TIMESTAMPDIFF(SECOND, START_TIME, NOW()) as ELAPSED_SECONDS,
                START_TIME,
                ESTIMATED_FINISH_TIME
            FROM oceanbase.GV$OB_COMPACTION_PROGRESS 
            WHERE STATUS <> 'FINISH'
            ORDER BY TENANT_ID, SVR_IP;
        """
        current_data = self._execute_sql_safe(sql, "current compaction progress")

        if current_data:
            self._save_to_file("current_compaction_progress.json", current_data)

            for item in current_data:
                tenant_id = item.get("TENANT_ID")
                data_size = item.get("DATA_SIZE", 0) or 0
                unfinished = item.get("UNFINISHED_DATA_SIZE", 0) or 0
                elapsed = item.get("ELAPSED_SECONDS", 0) or 0

                if elapsed > 0 and data_size > 0:
                    processed = data_size - unfinished
                    speed_mbps = (processed / 1024 / 1024) / elapsed if elapsed > 0 else 0
                    progress_pct = (processed / data_size * 100) if data_size > 0 else 0

                    self.record.add_record("Tenant {0} on {1}:{2}: Progress {3:.1f}%, Speed {4:.2f} MB/s, Elapsed {5}s".format(tenant_id, item.get("SVR_IP"), item.get("SVR_PORT"), progress_pct, speed_mbps, elapsed))

                    # Alert if speed is very low
                    if speed_mbps < 1 and elapsed > 600:  # < 1MB/s after 10 minutes
                        self.record.add_suggest("Very slow compaction detected for tenant {0} ({1:.2f} MB/s). " "Check disk I/O, memory pressure, or DAG scheduler status.".format(tenant_id, speed_mbps))

    def _diagnose_tenant(self, err_tenant_id):
        """
        Perform detailed diagnosis for a specific tenant
        """
        tenant_record = RCA_ResultRecord(self.stdio)
        first_record_records = self.record.records.copy()
        tenant_record.records.extend(first_record_records)

        self.stdio.verbose("Starting detailed diagnosis for tenant_id: {0}".format(err_tenant_id))
        tenant_record.add_record("=== Detailed diagnosis for tenant_id: {0} ===".format(err_tenant_id))

        # 1. Get compaction status
        try:
            tenant_record.add_record("Step 1: Getting CDB_OB_MAJOR_COMPACTION status")
            sql = 'SELECT * FROM oceanbase.CDB_OB_MAJOR_COMPACTION WHERE TENANT_ID="{0}";'.format(err_tenant_id)
            compaction_data = self._execute_sql_safe(sql, "tenant compaction status")

            if compaction_data:
                self._save_to_file("compaction_status.json", compaction_data, err_tenant_id)
                tenant_record.add_record("Compaction status: {0}".format(json.dumps(compaction_data, cls=DateTimeEncoder)))
        except Exception as e:
            tenant_record.add_record("Failed to get compaction status: {0}".format(e))

        # 2. Get and analyze diagnose info
        try:
            tenant_record.add_record("Step 2: Analyzing diagnose info")
            sql = 'SELECT * FROM oceanbase.__all_virtual_compaction_diagnose_info WHERE tenant_id="{0}";'.format(err_tenant_id)
            diagnose_data = self._execute_sql_safe(sql, "tenant diagnose info")

            if diagnose_data:
                self._save_to_file("diagnose_info.json", diagnose_data, err_tenant_id)
                for data in diagnose_data:
                    if data.get("status") == "FAILED":
                        self._analyze_diagnose_info(data, tenant_record)
        except Exception as e:
            tenant_record.add_record("Failed to analyze diagnose info: {0}".format(e))

        # 3. Get compaction progress details
        try:
            tenant_record.add_record("Step 3: Getting compaction progress details")

            # Get global broadcast SCN
            sql = "SELECT GLOBAL_BROADCAST_SCN, LAST_SCN FROM oceanbase.CDB_OB_MAJOR_COMPACTION WHERE TENANT_ID='{0}';".format(err_tenant_id)
            scn_data = self._execute_sql_safe(sql, "get SCN")

            if scn_data and len(scn_data) > 0:
                global_broadcast_scn = scn_data[0].get("GLOBAL_BROADCAST_SCN")
                last_scn = scn_data[0].get("LAST_SCN")
                tenant_record.add_record("GLOBAL_BROADCAST_SCN: {0}, LAST_SCN: {1}".format(global_broadcast_scn, last_scn))

                # Get progress for current compaction
                sql = "SELECT * FROM oceanbase.GV$OB_COMPACTION_PROGRESS WHERE TENANT_ID='{0}' AND COMPACTION_SCN='{1}';".format(err_tenant_id, global_broadcast_scn)
                progress_data = self._execute_sql_safe(sql, "current progress")
                if progress_data:
                    self._save_to_file("progress_current.json", progress_data, err_tenant_id)

                # Get progress for last compaction (for comparison)
                sql = "SELECT * FROM oceanbase.GV$OB_COMPACTION_PROGRESS WHERE TENANT_ID='{0}' AND COMPACTION_SCN='{1}';".format(err_tenant_id, last_scn)
                last_progress_data = self._execute_sql_safe(sql, "last progress")
                if last_progress_data:
                    self._save_to_file("progress_last.json", last_progress_data, err_tenant_id)
        except Exception as e:
            tenant_record.add_record("Failed to get progress details: {0}".format(e))

        # 4. Get compaction history
        try:
            tenant_record.add_record("Step 4: Getting compaction history")
            sql = """
                SELECT * FROM oceanbase.GV$OB_TABLET_COMPACTION_HISTORY 
                WHERE TENANT_ID='{0}' 
                ORDER BY START_TIME DESC 
                LIMIT 100;
            """.format(
                err_tenant_id
            )
            history_data = self._execute_sql_safe(sql, "compaction history")
            if history_data:
                self._save_to_file("compaction_history.json", history_data, err_tenant_id)
                tenant_record.add_record("Saved {0} compaction history records".format(len(history_data)))
        except Exception as e:
            tenant_record.add_record("Failed to get compaction history: {0}".format(e))

        # 5. Get compaction suggestions
        try:
            tenant_record.add_record("Step 5: Getting compaction suggestions")
            sql = 'SELECT * FROM oceanbase.GV$OB_COMPACTION_SUGGESTIONS WHERE tenant_id="{0}";'.format(err_tenant_id)
            suggestions_data = self._execute_sql_safe(sql, "compaction suggestions")
            if suggestions_data:
                self._save_to_file("compaction_suggestions.json", suggestions_data, err_tenant_id)
                tenant_record.add_record("Found {0} compaction suggestions".format(len(suggestions_data)))
                for sug in suggestions_data:
                    tenant_record.add_record("Suggestion: {0}".format(sug.get("SUGGESTION", "N/A")))
        except Exception as e:
            tenant_record.add_record("Failed to get suggestions: {0}".format(e))

        # 6. Get DAG scheduler status
        try:
            tenant_record.add_record("Step 6: Getting DAG scheduler status")
            sql = "SELECT * FROM oceanbase.__all_virtual_dag_scheduler WHERE tenant_id='{0}';".format(err_tenant_id)
            dag_data = self._execute_sql_safe(sql, "DAG scheduler")
            if dag_data:
                self._save_to_file("dag_scheduler.json", dag_data, err_tenant_id)
                tenant_record.add_record("DAG scheduler info collected")
        except Exception as e:
            tenant_record.add_record("Failed to get DAG scheduler status: {0}".format(e))

        # 7. Get DAG task status
        try:
            tenant_record.add_record("Step 7: Getting DAG task status")
            sql = "SELECT * FROM oceanbase.__all_virtual_dag WHERE tenant_id='{0}' LIMIT 100;".format(err_tenant_id)
            dag_task_data = self._execute_sql_safe(sql, "DAG tasks")
            if dag_task_data:
                self._save_to_file("dag_tasks.json", dag_task_data, err_tenant_id)
                tenant_record.add_record("Found {0} DAG tasks".format(len(dag_task_data)))
        except Exception as e:
            tenant_record.add_record("Failed to get DAG task status: {0}".format(e))

        # 8. Collect dmesg logs from all nodes
        try:
            tenant_record.add_record("Step 8: Collecting dmesg logs")
            self._collect_dmesg_logs(tenant_record)
        except Exception as e:
            tenant_record.add_record("Failed to collect dmesg logs: {0}".format(e))

        # 9. Collect relevant observer logs
        try:
            tenant_record.add_record("Step 9: Collecting relevant observer logs")
            self._collect_observer_logs(err_tenant_id, tenant_record)
        except Exception as e:
            tenant_record.add_record("Failed to collect observer logs: {0}".format(e))

        # 10. Check for duplicate index names (Issue #607)
        try:
            tenant_record.add_record("Step 10: Checking for duplicate index names")
            self._check_duplicate_index_names(err_tenant_id, tenant_record)
        except Exception as e:
            tenant_record.add_record("Failed to check duplicate index names: {0}".format(e))

        # 11. Check for DDL task status
        try:
            tenant_record.add_record("Step 11: Checking DDL task status")
            self._check_ddl_task_status(err_tenant_id, tenant_record)
        except Exception as e:
            tenant_record.add_record("Failed to check DDL task status: {0}".format(e))

        # 12. Check memory throttling issues (Issue #1107)
        try:
            tenant_record.add_record("Step 12: Checking memory throttling issues")
            self._check_memory_throttling(err_tenant_id, tenant_record)
        except Exception as e:
            tenant_record.add_record("Failed to check memory throttling: {0}".format(e))

        tenant_record.add_suggest("Please review diagnostic files in {0} for detailed analysis".format(self.local_path))
        self.Result.records.append(tenant_record)

    def _analyze_diagnose_info(self, sql_data, tenant_record):
        """
        Analyze diagnose info and provide specific suggestions
        """
        svr_ip = sql_data.get("svr_ip")
        svr_port = sql_data.get("svr_port")
        tenant_id = sql_data.get("tenant_id")
        ls_id = sql_data.get("ls_id")
        tablet_id = sql_data.get("tablet_id")
        create_time = sql_data.get("create_time")
        diagnose_info = sql_data.get("diagnose_info", "")

        tenant_record.add_record("Analyzing diagnose_info: {0}".format(diagnose_info[:200] if diagnose_info else "N/A"))

        # Handle different diagnose types
        if "schedule medium failed" in diagnose_info:
            self._handle_schedule_medium_failed(svr_ip, svr_port, tenant_id, create_time, tenant_record)

        elif "error_no=" in diagnose_info and "error_trace=" in diagnose_info:
            self._handle_error_no(sql_data, tenant_record)

        elif "weak read ts is not ready" in diagnose_info:
            self._handle_weak_read_not_ready(tenant_id, ls_id, tenant_record)

        elif "memtable can not create dag successfully" in diagnose_info:
            self._handle_memtable_dag_failure(sql_data, tenant_record)

        elif "medium wait for freeze" in diagnose_info or "major wait for freeze" in diagnose_info:
            self._handle_wait_for_freeze(svr_ip, svr_port, tenant_id, tenant_record)

        elif "major not schedule for long time" in diagnose_info:
            self._handle_major_not_scheduled(sql_data, tenant_record)

        elif "tablet has been deleted" in diagnose_info:
            tenant_record.add_record("Tablet {0} has been deleted, this is expected during DDL operations".format(tablet_id))
            tenant_record.add_suggest("Tablet deletion detected. If DDL is in progress, this is normal. Otherwise, check for unexpected tablet drops.")

        elif "table not exist" in diagnose_info:
            tenant_record.add_record("Table associated with tablet {0} does not exist".format(tablet_id))
            tenant_record.add_suggest("Table not exist error. The table may have been dropped during compaction.")

        elif "log disk space is almost full" in diagnose_info:
            tenant_record.add_record("Log disk space is almost full!")
            tenant_record.add_suggest(
                "CRITICAL: Log disk space is almost full. Actions:\n" "1. Check log disk usage: SELECT * FROM GV$OB_SERVERS;\n" "2. Clean up old logs or expand disk capacity\n" "3. Check for stuck transactions that prevent log recycling"
            )

        elif "index table is invalid" in diagnose_info:
            tenant_record.add_record("Index table is invalid for tablet {0}".format(tablet_id))
            tenant_record.add_suggest("Invalid index detected. Consider rebuilding the index.")

        else:
            tenant_record.add_record("Unknown diagnose_info type: {0}".format(diagnose_info[:100] if diagnose_info else "empty"))

    def _handle_schedule_medium_failed(self, svr_ip, svr_port, tenant_id, create_time, tenant_record):
        """Handle 'schedule medium failed' diagnose type"""
        tenant_record.add_record("Diagnose type: schedule medium failed")

        node, ssh_client = self._find_observer_node(svr_ip, svr_port)
        if node is None:
            tenant_record.add_record("Cannot find observer node for {0}:{1}".format(svr_ip, svr_port))
            return

        log_name = "/tmp/rca_schedule_medium_failed_{0}_{1}_{2}.txt".format(tenant_id, svr_ip, svr_port)
        try:
            ssh_client.exec_cmd('grep "schedule_medium_failed" {0}/log/observer.log* | grep -P "\\[\\d+\\]" -m 1 -o > {1}'.format(node.get("home_path"), log_name))
            local_file_path = os.path.join(self.local_path, os.path.basename(log_name))
            ssh_client.download(log_name, local_file_path)
            tenant_record.add_record("Downloaded schedule_medium_failed logs to {0}".format(local_file_path))
            ssh_client.exec_cmd("rm -rf {0}".format(log_name))
        except Exception as e:
            tenant_record.add_record("Failed to collect schedule_medium_failed logs: {0}".format(e))

        tenant_record.add_suggest("Schedule medium failed. Possible causes:\n" "1. Memory pressure - check tenant memory usage\n" "2. DAG scheduler overloaded - check __all_virtual_dag_scheduler\n" "3. Disk I/O bottleneck - check disk throughput")

    def _handle_error_no(self, sql_data, tenant_record):
        """Handle 'error_no' diagnose type"""
        diagnose_info = sql_data.get("diagnose_info", "")
        svr_ip = sql_data.get("svr_ip")
        svr_port = sql_data.get("svr_port")
        tenant_id = sql_data.get("tenant_id")
        tablet_id = sql_data.get("tablet_id")

        tenant_record.add_record("Diagnose type: error_no")

        try:
            err_no_match = re.search(r'error_no=([^,\s]+)', diagnose_info)
            err_trace_match = re.search(r'error_trace=([^,\s]+)', diagnose_info)

            err_no = err_no_match.group(1) if err_no_match else "unknown"
            err_trace = err_trace_match.group(1) if err_trace_match else "unknown"

            tenant_record.add_record("Error number: {0}, Error trace: {1}".format(err_no, err_trace))

            # Get SCN information for comparison
            sql = "SELECT GLOBAL_BROADCAST_SCN FROM oceanbase.CDB_OB_MAJOR_COMPACTION WHERE TENANT_ID='{0}';".format(tenant_id)
            scn_data = self._execute_sql_safe(sql, "get broadcast SCN")

            if scn_data and len(scn_data) > 0:
                global_broadcast_scn = scn_data[0].get("GLOBAL_BROADCAST_SCN")

                sql = "SELECT snapshot_version FROM oceanbase.__all_virtual_tablet_meta_table WHERE tablet_id='{0}' AND tenant_id='{1}';".format(tablet_id, tenant_id)
                tablet_data = self._execute_sql_safe(sql, "get tablet snapshot")

                if tablet_data and len(tablet_data) > 0:
                    compaction_scn = tablet_data[0].get("snapshot_version", 0)
                    tenant_record.add_record("Tablet compaction_scn: {0}, global_broadcast_scn: {1}".format(compaction_scn, global_broadcast_scn))

            # Collect error trace logs
            node, ssh_client = self._find_observer_node(svr_ip, svr_port)
            if node and err_trace != "unknown":
                log_name = "/tmp/rca_error_trace_{0}_{1}_{2}.txt".format(tenant_id, svr_ip, svr_port)
                try:
                    ssh_client.exec_cmd('grep "{0}" {1}/log/observer.log* > {2}'.format(err_trace, node.get("home_path"), log_name))
                    local_file_path = os.path.join(self.local_path, os.path.basename(log_name))
                    ssh_client.download(log_name, local_file_path)
                    tenant_record.add_record("Downloaded error trace logs to {0}".format(local_file_path))
                    ssh_client.exec_cmd("rm -rf {0}".format(log_name))
                except Exception as e:
                    tenant_record.add_record("Failed to collect error trace logs: {0}".format(e))

        except Exception as e:
            tenant_record.add_record("Failed to parse error_no: {0}".format(e))

        tenant_record.add_suggest("Compaction error occurred. Check the error trace in observer logs for root cause.")

    def _handle_weak_read_not_ready(self, tenant_id, ls_id, tenant_record):
        """Handle 'weak read ts is not ready' diagnose type"""
        tenant_record.add_record("Diagnose type: weak read ts is not ready")

        # Get LS info
        sql = "SELECT * FROM oceanbase.__all_virtual_ls_info WHERE tenant_id='{0}' AND ls_id='{1}';".format(tenant_id, ls_id)
        ls_info = self._execute_sql_safe(sql, "get LS info")

        if ls_info and len(ls_info) > 0:
            self._save_to_file("ls_info.json", ls_info, tenant_id)
            weak_read_scn = ls_info[0].get("weak_read_scn")
            tenant_record.add_record("weak_read_scn: {0}".format(weak_read_scn))

            # Collect relevant logs
            work_path = os.path.join(self.local_path, "weak_read_ts_not_ready")
            if not os.path.exists(work_path):
                os.makedirs(work_path)

            if weak_read_scn:
                self.gather_log.grep(str(int(weak_read_scn) + 1))
            self.gather_log.grep("generate_weak_read_timestamp_")
            self.gather_log.grep("log disk space is almost full")
            self.gather_log.execute(save_path=work_path)
            tenant_record.add_record("Gathered weak read related logs to {0}".format(work_path))

        tenant_record.add_suggest(
            "Weak read timestamp not ready. Possible causes:\n"
            "1. Log replay is lagging - check __all_virtual_replay_stat\n"
            "2. Network issues between replicas\n"
            "3. Disk I/O bottleneck affecting log synchronization\n"
            "4. Check if log disk is almost full"
        )

    def _handle_memtable_dag_failure(self, sql_data, tenant_record):
        """Handle 'memtable can not create dag successfully' diagnose type"""
        svr_ip = sql_data.get("svr_ip")
        svr_port = sql_data.get("svr_port")
        tenant_id = sql_data.get("tenant_id")

        tenant_record.add_record("Diagnose type: memtable can not create dag successfully")

        # Get DAG scheduler status
        sql = "SELECT * FROM oceanbase.__all_virtual_dag_scheduler WHERE svr_ip='{0}' AND svr_port='{1}' AND tenant_id='{2}';".format(svr_ip, svr_port, tenant_id)
        dag_data = self._execute_sql_safe(sql, "get DAG scheduler")

        if dag_data:
            self._save_to_file("dag_scheduler_detail.json", dag_data, tenant_id)
            tenant_record.add_record("DAG scheduler status collected")

        tenant_record.add_suggest(
            "Memtable cannot create DAG. Possible causes:\n"
            "1. DAG scheduler queue is full - check __all_virtual_dag_scheduler\n"
            "2. Too many concurrent compaction tasks\n"
            "3. System resource exhaustion (CPU/memory)\n"
            "Consider adjusting: compaction_dag_thread_num, compaction_dag_net_thread_num"
        )

    def _handle_wait_for_freeze(self, svr_ip, svr_port, tenant_id, tenant_record):
        """Handle 'wait for freeze' diagnose type"""
        tenant_record.add_record("Diagnose type: medium/major wait for freeze")

        # Get DAG scheduler and freeze info
        sql = "SELECT * FROM oceanbase.__all_virtual_dag_scheduler WHERE svr_ip='{0}' AND svr_port='{1}' AND tenant_id='{2}';".format(svr_ip, svr_port, tenant_id)
        dag_data = self._execute_sql_safe(sql, "get DAG scheduler")

        if dag_data:
            self._save_to_file("dag_scheduler_freeze.json", dag_data, tenant_id)

        # Check minor freeze info
        sql = "SELECT * FROM oceanbase.__all_virtual_minor_freeze_info WHERE tenant_id='{0}';".format(tenant_id)
        freeze_data = self._execute_sql_safe(sql, "get minor freeze info")

        if freeze_data:
            self._save_to_file("minor_freeze_info.json", freeze_data, tenant_id)
            tenant_record.add_record("Minor freeze info collected")

        tenant_record.add_suggest("Compaction waiting for freeze. Possible causes:\n" "1. Active memtable freeze is slow - check memstore usage\n" "2. High write pressure preventing freeze\n" "3. Check __all_virtual_minor_freeze_info for freeze status")

    def _handle_major_not_scheduled(self, sql_data, tenant_record):
        """Handle 'major not schedule for long time' diagnose type"""
        svr_ip = sql_data.get("svr_ip")
        svr_port = sql_data.get("svr_port")
        tenant_id = sql_data.get("tenant_id")
        ls_id = sql_data.get("ls_id")
        tablet_id = sql_data.get("tablet_id")
        create_time = sql_data.get("create_time")

        tenant_record.add_record("Diagnose type: major not schedule for long time")

        # Get tablet compaction info
        sql = """
            SELECT * FROM oceanbase.__all_virtual_tablet_compaction_info 
            WHERE svr_ip='{0}' AND svr_port='{1}' AND tenant_id='{2}' AND ls_id='{3}' AND tablet_id='{4}';
        """.format(
            svr_ip, svr_port, tenant_id, ls_id, tablet_id
        )
        compaction_info = self._execute_sql_safe(sql, "get tablet compaction info")

        if compaction_info:
            self._save_to_file("tablet_compaction_info.json", compaction_info, tenant_id)
            tenant_record.add_record("Tablet compaction info collected")

        # Collect MediumLoop logs
        node, ssh_client = self._find_observer_node(svr_ip, svr_port)
        if node:
            log_name = "/tmp/rca_major_not_scheduled_{0}_{1}_{2}.txt".format(tenant_id, svr_ip, svr_port)
            try:
                # Get thread ID for MediumLoop
                thread_id = ssh_client.exec_cmd('grep "MediumLoo" {0}/log/observer.log* -m 1 | grep -P "\\[\\d+\\]" -m 1 -o | grep -oP "\\d+"'.format(node.get("home_path"))).strip()

                if thread_id:
                    ssh_client.exec_cmd('grep "{0}" {1}/log/observer.log -m 100 > {2}'.format(thread_id, node.get("home_path"), log_name))
                    local_file_path = os.path.join(self.local_path, os.path.basename(log_name))
                    ssh_client.download(log_name, local_file_path)
                    tenant_record.add_record("Downloaded MediumLoop logs to {0}".format(local_file_path))
                    ssh_client.exec_cmd("rm -rf {0}".format(log_name))
            except Exception as e:
                tenant_record.add_record("Failed to collect MediumLoop logs: {0}".format(e))

        tenant_record.add_suggest(
            "Major compaction not scheduled for long time. Possible causes:\n" "1. Medium compaction prerequisites not met\n" "2. Tablet has pending minor compactions\n" "3. Check __all_virtual_tablet_compaction_info for tablet status"
        )

    def _check_duplicate_index_names(self, tenant_id, tenant_record):
        """
        Check for duplicate index names across different tables.
        Reference: https://github.com/oceanbase/obdiag/issues/607
        https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000001843060

        This issue can cause compaction hold and DDL failures.
        """
        tenant_record.add_record("Checking for duplicate index names that may cause compaction hold...")

        # Query to find duplicate index names across different tables
        sql = """
            SELECT 
                INDEX_NAME,
                COUNT(DISTINCT TABLE_NAME) as TABLE_COUNT,
                GROUP_CONCAT(DISTINCT TABLE_NAME SEPARATOR ', ') as TABLES,
                GROUP_CONCAT(DISTINCT CONCAT(TABLE_NAME, '.', INDEX_NAME) SEPARATOR ', ') as FULL_INDEX_NAMES
            FROM oceanbase.CDB_INDEXES 
            WHERE TENANT_ID = '{0}'
              AND INDEX_TYPE != 'LOB'
              AND INDEX_NAME NOT LIKE '%_RECYCLE_%'
            GROUP BY INDEX_NAME
            HAVING COUNT(DISTINCT TABLE_NAME) > 1
            ORDER BY TABLE_COUNT DESC
            LIMIT 50;
        """.format(
            tenant_id
        )

        duplicate_indexes = self._execute_sql_safe(sql, "check duplicate index names")

        if duplicate_indexes:
            self._save_to_file("duplicate_index_names.json", duplicate_indexes, tenant_id)
            tenant_record.add_record("WARNING: Found {0} duplicate index names across different tables!".format(len(duplicate_indexes)))

            for idx in duplicate_indexes:
                index_name = idx.get("INDEX_NAME", "N/A")
                tables = idx.get("TABLES", "N/A")
                table_count = idx.get("TABLE_COUNT", 0)
                tenant_record.add_record("Duplicate index '{0}' found in {1} tables: {2}".format(index_name, table_count, tables))

            tenant_record.add_suggest(
                "CRITICAL: Duplicate index names detected across different tables!\n"
                "This is a known issue that can cause:\n"
                "1. Major compaction stuck (cannot proceed)\n"
                "2. DDL operations blocked\n"
                "3. Schema inconsistency\n\n"
                "Resolution steps:\n"
                "1. Identify the duplicate indexes from the diagnostic file\n"
                "2. Rename or drop the conflicting indexes:\n"
                "   ALTER TABLE table_name RENAME INDEX old_index_name TO new_index_name;\n"
                "   or DROP INDEX index_name ON table_name;\n"
                "3. After fixing, trigger a new major compaction:\n"
                "   ALTER SYSTEM MAJOR FREEZE TENANT = tenant_name;\n\n"
                "Reference: https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000001843060"
            )
        else:
            tenant_record.add_record("No duplicate index names found across different tables")

        # Also check for index status issues
        sql = """
            SELECT 
                TABLE_NAME,
                INDEX_NAME,
                INDEX_TYPE,
                STATUS,
                VISIBILITY
            FROM oceanbase.CDB_INDEXES 
            WHERE TENANT_ID = '{0}'
              AND STATUS != 'VALID'
            ORDER BY TABLE_NAME
            LIMIT 100;
        """.format(
            tenant_id
        )

        invalid_indexes = self._execute_sql_safe(sql, "check invalid indexes")

        if invalid_indexes:
            self._save_to_file("invalid_indexes.json", invalid_indexes, tenant_id)
            tenant_record.add_record("Found {0} indexes with non-VALID status".format(len(invalid_indexes)))
            for idx in invalid_indexes[:10]:  # Show first 10
                tenant_record.add_record("Invalid index: {0}.{1} (status: {2})".format(idx.get("TABLE_NAME"), idx.get("INDEX_NAME"), idx.get("STATUS")))

    def _check_ddl_task_status(self, tenant_id, tenant_record):
        """
        Check DDL task status that may be blocking compaction.
        """
        tenant_record.add_record("Checking DDL task status...")

        # Check running DDL tasks
        sql = """
            SELECT 
                TASK_ID,
                DDL_TYPE,
                STATUS,
                TENANT_ID,
                TABLE_ID,
                OBJECT_ID,
                TARGET_OBJECT_ID,
                CREATE_TIME,
                TRACE_ID,
                MESSAGE
            FROM oceanbase.__all_virtual_ddl_task_status
            WHERE TENANT_ID = '{0}'
            ORDER BY CREATE_TIME DESC
            LIMIT 50;
        """.format(
            tenant_id
        )

        ddl_tasks = self._execute_sql_safe(sql, "get DDL task status")

        if ddl_tasks:
            self._save_to_file("ddl_task_status.json", ddl_tasks, tenant_id)
            tenant_record.add_record("Found {0} DDL tasks".format(len(ddl_tasks)))

            # Check for stuck DDL tasks
            pending_tasks = [t for t in ddl_tasks if t.get("STATUS") in ("PREPARE", "REDEFINITION", "COPY_TABLE_DATA", "TAKE_EFFECT", "WAIT_TRANS_END")]
            if pending_tasks:
                tenant_record.add_record("Found {0} pending DDL tasks that may block compaction".format(len(pending_tasks)))
                for task in pending_tasks[:5]:  # Show first 5
                    tenant_record.add_record("Pending DDL: type={0}, status={1}, table_id={2}, trace_id={3}".format(task.get("DDL_TYPE"), task.get("STATUS"), task.get("TABLE_ID"), task.get("TRACE_ID")))
                tenant_record.add_suggest(
                    "Pending DDL tasks detected. These may block major compaction.\n"
                    "1. Check if DDL is making progress: query __all_virtual_ddl_task_status periodically\n"
                    "2. If stuck, check observer logs for DDL errors using the TRACE_ID\n"
                    "3. Consider canceling stuck DDL: ALTER SYSTEM CANCEL DDL task_id;"
                )
        else:
            tenant_record.add_record("No DDL tasks found")

        # Check DDL error table
        sql = """
            SELECT * FROM oceanbase.__all_virtual_ddl_error_message
            WHERE TENANT_ID = '{0}'
            ORDER BY GMT_CREATE DESC
            LIMIT 20;
        """.format(
            tenant_id
        )

        ddl_errors = self._execute_sql_safe(sql, "get DDL errors")

        if ddl_errors:
            self._save_to_file("ddl_errors.json", ddl_errors, tenant_id)
            tenant_record.add_record("Found {0} DDL error records".format(len(ddl_errors)))
            for err in ddl_errors[:5]:  # Show first 5
                tenant_record.add_record("DDL error: task_id={0}, ret_code={1}, message={2}".format(err.get("TASK_ID"), err.get("RET_CODE"), str(err.get("DDL_ERROR_MESSAGE", ""))[:100]))

    def _collect_dmesg_logs(self, tenant_record):
        """Collect dmesg logs from all observer nodes"""
        dmesg_log_path = os.path.join(self.local_path, "dmesg_log")
        if not os.path.exists(dmesg_log_path):
            os.makedirs(dmesg_log_path)

        for observer_node in self.observer_nodes:
            try:
                ssh_client = observer_node.get("ssher")
                if ssh_client is None:
                    continue

                node_name = ssh_client.get_name()
                remote_log = "/tmp/dmesg_{0}.log".format(node_name)
                local_log = os.path.join(dmesg_log_path, "dmesg_{0}.log".format(node_name))

                ssh_client.exec_cmd("dmesg -T > {0}".format(remote_log))
                ssh_client.download(remote_log, local_log)
                ssh_client.exec_cmd("rm -rf {0}".format(remote_log))

                tenant_record.add_record("Collected dmesg from {0}".format(node_name))
            except Exception as e:
                self.stdio.warn("Failed to collect dmesg from node: {0}".format(e))

    def _collect_observer_logs(self, tenant_id, tenant_record):
        """Collect relevant observer logs for the tenant"""
        # Check if we need to collect logs based on RS location
        try:
            sql = "SELECT SVR_IP, SVR_PORT FROM oceanbase.DBA_OB_TABLET_REPLICAS WHERE TENANT_ID='{0}' AND LS_ID=1 LIMIT 1;".format(tenant_id)
            rs_data = self._execute_sql_safe(sql, "get RS location")

            if rs_data and len(rs_data) > 0:
                svr_ip = rs_data[0].get("SVR_IP")
                svr_port = rs_data[0].get("SVR_PORT")

                node, ssh_client = self._find_observer_node(svr_ip, svr_port)
                if node:
                    log_name = "/tmp/major_merge_progress_checker_{0}.log".format(tenant_id)
                    local_log = os.path.join(self.local_path, "major_merge_progress_checker_{0}.log".format(tenant_id))

                    ssh_client.exec_cmd('grep "major_merge_progress_checker" {0}/log/rootservice.log* | grep "T{1}" -m 500 > {2}'.format(node.get("home_path"), tenant_id, log_name))
                    ssh_client.download(log_name, local_log)
                    ssh_client.exec_cmd("rm -rf {0}".format(log_name))

                    tenant_record.add_record("Collected major_merge_progress_checker logs")
        except Exception as e:
            self.stdio.warn("Failed to collect observer logs: {0}".format(e))

    def _check_memory_throttling(self, tenant_id, tenant_record):
        """
        Check memory throttling issues that may cause merge failures.
        Issue #1107: Memory insufficient leading to throttling, which causes transaction timeout
        and inner SQL timeout failures.

        Root cause: In scenarios with multiple memory expansions, meta tenant memory limit
        increases multiple times, but TxShare throttling limit is not dynamically adjusted,
        causing continuous throttling.
        """
        tenant_record.add_record("Checking memory throttling issues (Issue #1107)...")

        # Step 1: Check tenant memory usage and throttling status
        self._check_tenant_memory_usage(tenant_id, tenant_record)

        # Step 2: Check TxShare throttling limit vs meta tenant memory limit
        self._check_txshare_throttling_limit(tenant_record)

        # Step 3: Check for memory expansion bug scenario
        self._check_memory_expansion_bug(tenant_id, tenant_record)

        # Step 4: Check transaction timeout issues related to throttling
        self._check_throttling_related_timeouts(tenant_id, tenant_record)

    def _check_tenant_memory_usage(self, tenant_id, tenant_record):
        """Check tenant memory usage and throttling status"""
        try:
            # Get tenant memory usage from __all_virtual_memory_info
            # Note: __all_virtual_memory_info does not have limit_value field
            # We need to get memory limit from DBA_OB_UNITS instead
            # Fields to confirm: tenant_id, svr_ip, svr_port, hold, used
            # Verify via: DESC oceanbase.__all_virtual_memory_info;
            sql_memory = """
                SELECT 
                    tenant_id,
                    svr_ip,
                    svr_port,
                    ROUND(SUM(hold) / 1024 / 1024 / 1024, 2) as hold_gb,
                    ROUND(SUM(used) / 1024 / 1024 / 1024, 2) as used_gb
                FROM oceanbase.__all_virtual_memory_info 
                WHERE tenant_id = '{0}'
                GROUP BY tenant_id, svr_ip, svr_port
                ORDER BY hold_gb DESC;
            """.format(
                tenant_id
            )

            memory_data = self._execute_sql_safe(sql_memory, "check tenant memory usage")

            # Get memory limit from DBA_OB_UNITS
            # Note: DBA_OB_UNITS has MEMORY_SIZE (not max_memory/min_memory), and fields are uppercase
            # Fields confirmed: TENANT_ID, SVR_IP, SVR_PORT, MEMORY_SIZE
            # Verify via: DESC oceanbase.DBA_OB_UNITS;
            sql_limit = """
                SELECT 
                    u.TENANT_ID as tenant_id,
                    u.SVR_IP as svr_ip,
                    u.SVR_PORT as svr_port,
                    ROUND(SUM(u.MEMORY_SIZE) / 1024 / 1024 / 1024, 2) as max_memory_gb
                FROM oceanbase.DBA_OB_UNITS u
                WHERE u.TENANT_ID = '{0}'
                GROUP BY u.TENANT_ID, u.SVR_IP, u.SVR_PORT
                ORDER BY u.SVR_IP, u.SVR_PORT;
            """.format(
                tenant_id
            )

            limit_data = self._execute_sql_safe(sql_limit, "get tenant memory limit")

            # Create a dictionary for quick lookup
            limit_dict = {}
            if limit_data:
                for row in limit_data:
                    svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                    svr_port = row.get("svr_port") or row.get("SVR_PORT")
                    try:
                        max_memory_gb = float(row.get("max_memory_gb") or row.get("MAX_MEMORY_GB") or 0)
                    except (ValueError, TypeError):
                        max_memory_gb = 0
                    key = "{0}:{1}".format(svr_ip, svr_port)
                    limit_dict[key] = max_memory_gb

            if memory_data:
                self._save_to_file("tenant_memory_usage.json", memory_data, tenant_id)
                if limit_data:
                    self._save_to_file("tenant_memory_limit.json", limit_data, tenant_id)

                tenant_record.add_record("Tenant memory usage:")
                for row in memory_data:
                    # Handle both uppercase and lowercase field names
                    try:
                        hold_gb = float(row.get("hold_gb") or row.get("HOLD_GB") or 0)
                        used_gb = float(row.get("used_gb") or row.get("USED_GB") or 0)
                    except (ValueError, TypeError):
                        hold_gb = 0
                        used_gb = 0
                    svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                    svr_port = row.get("svr_port") or row.get("SVR_PORT")
                    key = "{0}:{1}".format(svr_ip, svr_port)
                    max_memory_gb = limit_dict.get(key, 0)

                    if max_memory_gb > 0:
                        usage_percent = (hold_gb / max_memory_gb * 100) if max_memory_gb > 0 else 0
                        tenant_record.add_record("  Server {0}:{1}: hold={2}GB, limit={3}GB, usage={4}%".format(svr_ip, svr_port, hold_gb, max_memory_gb, round(usage_percent, 2)))

                        if usage_percent > 90:
                            tenant_record.add_record("[WARNING] Tenant {0} on {1}:{2} has high memory usage: {3}%".format(tenant_id, svr_ip, svr_port, round(usage_percent, 2)))
                            tenant_record.add_suggest("Tenant memory usage is very high ({0}%). This may cause memory throttling. " "Consider expanding tenant memory or reducing workload.".format(round(usage_percent, 2)))
                    else:
                        # If we can't find memory limit, just show hold and used
                        tenant_record.add_record("  Server {0}:{1}: hold={2}GB, used={3}GB (memory limit not found in DBA_OB_UNITS)".format(svr_ip, svr_port, hold_gb, used_gb))

            # Check memstore info for throttling triggers
            # Fields confirmed: tenant_id, svr_ip, svr_port, memstore_used, memstore_limit, freeze_trigger
            # Note: writing_throttling_trigger_percentage is in GV$OB_PARAMETERS, not in this table
            # Verify via: DESC oceanbase.__all_virtual_tenant_memstore_info;
            sql = """
                SELECT 
                    tenant_id,
                    svr_ip,
                    svr_port,
                    ROUND(memstore_used / 1024 / 1024 / 1024, 2) as memstore_used_gb,
                    ROUND(memstore_limit / 1024 / 1024 / 1024, 2) as memstore_limit_gb,
                    CASE 
                        WHEN memstore_limit > 0 THEN ROUND(memstore_used / memstore_limit * 100, 2)
                        ELSE 0
                    END as memstore_usage_percent,
                    ROUND(freeze_trigger / 1024 / 1024 / 1024, 2) as freeze_trigger_gb
                FROM oceanbase.__all_virtual_tenant_memstore_info
                WHERE tenant_id = '{0}';
            """.format(
                tenant_id
            )

            memstore_data = self._execute_sql_safe(sql, "check memstore info")

            # Get writing_throttling_trigger_percentage from GV$OB_PARAMETERS
            sql_throttling_param = """
                SELECT 
                    SVR_IP as svr_ip,
                    SVR_PORT as svr_port,
                    VALUE as writing_throttling_trigger_percentage
                FROM oceanbase.GV$OB_PARAMETERS 
                WHERE NAME = 'writing_throttling_trigger_percentage'
                LIMIT 1;
            """
            throttling_param_data = self._execute_sql_safe(sql_throttling_param, "get throttling trigger percentage")
            writing_throttling_trigger = 60  # Default value
            if throttling_param_data and len(throttling_param_data) > 0:
                try:
                    trigger_value = throttling_param_data[0].get("writing_throttling_trigger_percentage") or "60"
                    writing_throttling_trigger = float(trigger_value)
                except (ValueError, TypeError) as e:
                    self.stdio.warn("Failed to parse writing_throttling_trigger_percentage, using default 60: {0}".format(e))
                    writing_throttling_trigger = 60

            if memstore_data:
                self._save_to_file("memstore_info.json", memstore_data, tenant_id)
                tenant_record.add_record("Memstore info:")
                for row in memstore_data:
                    try:
                        memstore_usage = float(row.get("memstore_usage_percent") or row.get("MEMSTORE_USAGE_PERCENT") or 0)
                    except (ValueError, TypeError):
                        memstore_usage = 0
                    svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                    svr_port = row.get("svr_port") or row.get("SVR_PORT")

                    tenant_record.add_record("  Server {0}:{1}: memstore_usage={2}%, writing_throttling_trigger={3}%".format(svr_ip, svr_port, memstore_usage, writing_throttling_trigger))

                    if memstore_usage >= writing_throttling_trigger:
                        tenant_record.add_record("[WARNING] Writing throttling may be triggered on {0}:{1} " "(memstore_usage={2}% >= trigger={3}%)".format(svr_ip, svr_port, memstore_usage, writing_throttling_trigger))
                        tenant_record.add_suggest(
                            "Writing throttling is likely active on {0}:{1} due to high memstore usage. " "This can cause transaction timeouts and merge failures. " "Check if tenant memory needs to be expanded.".format(svr_ip, svr_port)
                        )

        except Exception as e:
            tenant_record.add_record("Error checking tenant memory usage: {0}".format(e))
            self.stdio.warn("Error checking tenant memory usage: {0}".format(e))

    def _check_txshare_throttling_limit(self, tenant_record):
        """
        Check TxShare throttling limit vs meta tenant memory limit.
        Issue #1107: In multiple expansion scenarios, meta tenant memory limit increases
        multiple times, but TxShare throttling limit is not dynamically adjusted.
        """
        try:
            tenant_record.add_record("Checking TxShare throttling limit vs meta tenant memory limit...")

            # Get meta tenant (usually tenant_id = 1) memory limit
            # Note: __all_virtual_memory_info does not have limit_value field
            # We need to get memory limit from DBA_OB_UNITS instead
            # Fields to confirm: tenant_id, svr_ip, svr_port, hold, used
            # Verify via: DESC oceanbase.__all_virtual_memory_info;
            sql_memory = """
                SELECT 
                    tenant_id,
                    svr_ip,
                    svr_port,
                    ROUND(SUM(hold) / 1024 / 1024 / 1024, 2) as memory_hold_gb,
                    ROUND(SUM(used) / 1024 / 1024 / 1024, 2) as memory_used_gb
                FROM oceanbase.__all_virtual_memory_info 
                WHERE tenant_id = 1
                GROUP BY tenant_id, svr_ip, svr_port
                ORDER BY svr_ip, svr_port;
            """

            # Fields confirmed: TENANT_ID, SVR_IP, SVR_PORT, MEMORY_SIZE (not max_memory/min_memory)
            # Verify via: DESC oceanbase.DBA_OB_UNITS;
            sql_limit = """
                SELECT 
                    u.TENANT_ID as tenant_id,
                    u.SVR_IP as svr_ip,
                    u.SVR_PORT as svr_port,
                    ROUND(SUM(u.MEMORY_SIZE) / 1024 / 1024 / 1024, 2) as memory_limit_gb
                FROM oceanbase.DBA_OB_UNITS u
                WHERE u.TENANT_ID = 1
                GROUP BY u.TENANT_ID, u.SVR_IP, u.SVR_PORT
                ORDER BY u.SVR_IP, u.SVR_PORT;
            """

            meta_tenant_memory = self._execute_sql_safe(sql_memory, "get meta tenant memory usage")
            meta_tenant_limit = self._execute_sql_safe(sql_limit, "get meta tenant memory limit")

            # Create a dictionary for quick lookup
            limit_dict = {}
            if meta_tenant_limit:
                for row in meta_tenant_limit:
                    svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                    svr_port = row.get("svr_port") or row.get("SVR_PORT")
                    try:
                        memory_limit_gb = float(row.get("memory_limit_gb") or row.get("MEMORY_LIMIT_GB") or 0)
                    except (ValueError, TypeError):
                        memory_limit_gb = 0
                    key = "{0}:{1}".format(svr_ip, svr_port)
                    limit_dict[key] = memory_limit_gb

            if meta_tenant_memory:
                self._save_to_file("meta_tenant_memory.json", meta_tenant_memory)
                if meta_tenant_limit:
                    self._save_to_file("meta_tenant_memory_limit.json", meta_tenant_limit)

                tenant_record.add_record("Meta tenant (tenant_id=1) memory limits:")
                for row in meta_tenant_memory:
                    try:
                        memory_hold_gb = float(row.get("memory_hold_gb") or row.get("MEMORY_HOLD_GB") or 0)
                        memory_used_gb = float(row.get("memory_used_gb") or row.get("MEMORY_USED_GB") or 0)
                    except (ValueError, TypeError):
                        memory_hold_gb = 0
                        memory_used_gb = 0
                    svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                    svr_port = row.get("svr_port") or row.get("SVR_PORT")
                    key = "{0}:{1}".format(svr_ip, svr_port)
                    memory_limit_gb = limit_dict.get(key, 0)

                    if memory_limit_gb > 0:
                        tenant_record.add_record("  Server {0}:{1}: limit={2}GB, hold={3}GB".format(svr_ip, svr_port, memory_limit_gb, memory_hold_gb))
                    else:
                        tenant_record.add_record("  Server {0}:{1}: hold={2}GB, used={3}GB (memory limit not found in DBA_OB_UNITS)".format(svr_ip, svr_port, memory_hold_gb, memory_used_gb))

            # Check for TxShare throttling configuration
            # Note: The actual table/view name may need to be confirmed via obclient
            # Try to find throttling-related parameters
            # Fields confirmed: SVR_IP, SVR_PORT, NAME, VALUE, INFO (uppercase)
            # Verify via: DESC oceanbase.GV$OB_PARAMETERS;
            sql = """
                SELECT 
                    SVR_IP as svr_ip,
                    SVR_PORT as svr_port,
                    NAME as name,
                    VALUE as value,
                    INFO as info
                FROM oceanbase.GV$OB_PARAMETERS 
                WHERE NAME LIKE '%throttl%' 
                   OR NAME LIKE '%TxShare%'
                   OR NAME LIKE '%tx_share%'
                ORDER BY NAME, SVR_IP;
            """

            throttling_params = self._execute_sql_safe(sql, "get throttling parameters")
            if throttling_params:
                self._save_to_file("throttling_parameters.json", throttling_params)
                tenant_record.add_record("Throttling-related parameters:")
                for row in throttling_params:
                    name = row.get("name") or row.get("NAME")
                    value = row.get("value") or row.get("VALUE")
                    svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                    tenant_record.add_record("  Server {0}: {1} = {2}".format(svr_ip, name, value))

            # Check if there's a mismatch between meta tenant memory and throttling limits
            if limit_dict and len(limit_dict) > 0:
                # Calculate average memory limit for meta tenant
                try:
                    total_limit = sum(limit_dict.values())
                    avg_limit = total_limit / len(limit_dict) if len(limit_dict) > 0 else 0
                except (TypeError, ZeroDivisionError):
                    avg_limit = 0

                tenant_record.add_record("Average meta tenant memory limit: {0}GB".format(round(avg_limit, 2)))

                # If meta tenant memory is large (>50GB), it might indicate multiple expansions
                if avg_limit > 50:
                    tenant_record.add_record("[WARNING] Meta tenant memory limit is very large ({0}GB), " "which may indicate multiple memory expansions.".format(round(avg_limit, 2)))
                    tenant_record.add_suggest(
                        "CRITICAL: Meta tenant memory limit is {0}GB, suggesting multiple memory expansions. "
                        "This may trigger Issue #1107 bug: TxShare throttling limit may not have been "
                        "dynamically adjusted to match the increased meta tenant memory limit, causing "
                        "continuous throttling. Please check:\n"
                        "1. Check if throttling is active: SELECT * FROM oceanbase.__all_virtual_tenant_memstore_info;\n"
                        "2. Check transaction timeout errors in logs\n"
                        "3. Consider restarting observer to reset throttling limits\n"
                        "4. Contact OceanBase support for a fix in version 435bp4+".format(round(avg_limit, 2))
                    )

        except Exception as e:
            tenant_record.add_record("Error checking TxShare throttling limit: {0}".format(e))
            self.stdio.warn("Error checking TxShare throttling limit: {0}".format(e))

    def _check_memory_expansion_bug(self, tenant_id, tenant_record):
        """
        Check for memory expansion bug scenario (Issue #1107).
        Scenario: Multiple memory expansions (e.g., 50G -> 80G -> 16G -> 64G) may trigger
        a bug where meta tenant memory limit increases multiple times, but TxShare throttling
        limit is not dynamically adjusted.
        """
        try:
            tenant_record.add_record("Checking for memory expansion bug scenario...")

            # Check tenant memory history or current configuration
            # Get tenant unit configuration to see memory settings
            # Fields confirmed:
            #   DBA_OB_TENANTS: TENANT_ID, TENANT_NAME (uppercase)
            #   DBA_OB_UNITS: UNIT_ID, SVR_IP, SVR_PORT, MEMORY_SIZE (uppercase, not max_memory/min_memory)
            # Verify via: DESC oceanbase.DBA_OB_TENANTS; DESC oceanbase.DBA_OB_UNITS;
            sql = """
                SELECT 
                    t.TENANT_ID as tenant_id,
                    t.TENANT_NAME as tenant_name,
                    u.UNIT_ID as unit_id,
                    u.SVR_IP as svr_ip,
                    u.SVR_PORT as svr_port,
                    ROUND(u.MEMORY_SIZE / 1024 / 1024 / 1024, 2) as max_memory_gb
                FROM oceanbase.DBA_OB_TENANTS t
                LEFT JOIN oceanbase.DBA_OB_UNITS u ON t.TENANT_ID = u.TENANT_ID
                WHERE t.TENANT_ID = '{0}'
                ORDER BY u.SVR_IP, u.SVR_PORT;
            """.format(
                tenant_id
            )

            unit_config = self._execute_sql_safe(sql, "get tenant unit config")
            if unit_config:
                self._save_to_file("tenant_unit_config.json", unit_config, tenant_id)
                tenant_record.add_record("Tenant unit configuration:")
                for row in unit_config:
                    try:
                        max_memory_gb = float(row.get("max_memory_gb") or row.get("MAX_MEMORY_GB") or 0)
                    except (ValueError, TypeError):
                        max_memory_gb = 0
                    svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                    tenant_record.add_record("  Server {0}: memory_size={1}GB".format(svr_ip, max_memory_gb))

            # Check for signs of multiple memory expansions
            # Look for large differences between servers (might indicate expansion at different times)
            if unit_config and len(unit_config) > 1:
                memory_sizes = []
                for row in unit_config:
                    try:
                        mem_size = float(row.get("max_memory_gb") or row.get("MAX_MEMORY_GB") or 0)
                        if mem_size > 0:
                            memory_sizes.append(mem_size)
                    except (ValueError, TypeError):
                        continue
                if memory_sizes:
                    max_val = max(memory_sizes)
                    min_val = min(memory_sizes)
                    if max_val > 0 and min_val > 0:
                        ratio = max_val / min_val
                        if ratio > 1.5:  # More than 50% difference
                            tenant_record.add_record("[WARNING] Large variation in memory limits across servers " "(max={0}GB, min={1}GB, ratio={2:.2f})".format(max_val, min_val, ratio))
                            tenant_record.add_suggest(
                                "Large variation in memory limits detected. This may indicate " "multiple memory expansions, which could trigger Issue #1107 bug. " "Please check if throttling is active and if transactions are timing out."
                            )

        except Exception as e:
            tenant_record.add_record("Error checking memory expansion bug: {0}".format(e))
            self.stdio.warn("Error checking memory expansion bug: {0}".format(e))

    def _check_throttling_related_timeouts(self, tenant_id, tenant_record):
        """
        Check for transaction timeout issues related to throttling.
        Issue #1107: Throttling causes transaction timeout, which kills transactions,
        ultimately causing inner SQL timeout failures.
        """
        try:
            tenant_record.add_record("Checking for throttling-related transaction timeouts...")

            # Check for transaction timeout errors in recent logs
            # This would typically be done by checking logs, but we can also check
            # transaction status and error codes

            # Check for inner SQL timeout errors
            # Fields confirmed: TENANT_ID, SVR_IP, SVR_PORT, SQL_ID, RET_CODE, ELAPSED_TIME,
            #                    EXECUTE_TIME, IS_INNER_SQL, REQUEST_TIME (uppercase)
            # Verify via: DESC oceanbase.GV$OB_SQL_AUDIT;
            sql = """
                SELECT 
                    TENANT_ID as tenant_id,
                    SVR_IP as svr_ip,
                    SVR_PORT as svr_port,
                    SQL_ID as sql_id,
                    RET_CODE as ret_code,
                    ELAPSED_TIME as elapsed_time,
                    EXECUTE_TIME as execute_time,
                    IS_INNER_SQL as is_inner_sql,
                    usec_to_time(REQUEST_TIME) as request_time
                FROM oceanbase.GV$OB_SQL_AUDIT
                WHERE TENANT_ID = '{0}'
                  AND IS_INNER_SQL = 1
                  AND RET_CODE != 0
                  AND REQUEST_TIME > time_to_usec(now() - INTERVAL 1 HOUR)
                ORDER BY REQUEST_TIME DESC
                LIMIT 50;
            """.format(
                tenant_id
            )

            inner_sql_errors = self._execute_sql_safe(sql, "check inner sql errors")
            if inner_sql_errors:
                self._save_to_file("inner_sql_errors.json", inner_sql_errors, tenant_id)
                # Handle both string and integer error codes
                target_error_codes = {-4012, -4013, -4019, -4030, "-4012", "-4013", "-4019", "-4030"}
                timeout_errors = []
                for e in inner_sql_errors:
                    ret_code = e.get("ret_code")
                    # Try to convert to int if it's a string
                    if isinstance(ret_code, str):
                        try:
                            ret_code = int(ret_code)
                        except (ValueError, TypeError):
                            pass
                    if ret_code in target_error_codes:
                        timeout_errors.append(e)

                if timeout_errors:
                    tenant_record.add_record("[WARNING] Found {0} inner SQL timeout/error codes in the last hour".format(len(timeout_errors)))
                    for err in timeout_errors[:10]:  # Show first 10
                        ret_code = err.get("ret_code") or err.get("RET_CODE") or "unknown"
                        request_time = err.get("request_time") or err.get("REQUEST_TIME") or "unknown"
                        tenant_record.add_record("  Error code {0} at {1}".format(ret_code, request_time))

                    tenant_record.add_suggest(
                        "Inner SQL timeout errors detected. This may be caused by memory throttling. "
                        "Error codes -4012, -4013, -4019, -4030 are related to memory/timeout issues. "
                        "Please check:\n"
                        "1. If throttling is active (check __all_virtual_tenant_memstore_info)\n"
                        "2. If tenant memory is insufficient\n"
                        "3. If TxShare throttling limit needs adjustment (Issue #1107)"
                    )

            # Check transaction status for timeouts
            # Fields confirmed: TENANT_ID, SVR_IP, SVR_PORT, STATE, CTX_CREATE_TIME (uppercase)
            # Verify via: DESC oceanbase.GV$OB_TRANSACTION_PARTICIPANTS;
            sql = """
                SELECT 
                    TENANT_ID as tenant_id,
                    SVR_IP as svr_ip,
                    SVR_PORT as svr_port,
                    COUNT(*) as timeout_count
                FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS
                WHERE TENANT_ID = '{0}'
                  AND STATE = 'ABORT'
                  AND CTX_CREATE_TIME < date_sub(now(), INTERVAL 1 HOUR)
                GROUP BY TENANT_ID, SVR_IP, SVR_PORT;
            """.format(
                tenant_id
            )

            timeout_transactions = self._execute_sql_safe(sql, "check timeout transactions")
            if timeout_transactions:
                self._save_to_file("timeout_transactions.json", timeout_transactions, tenant_id)
                total_timeouts = 0
                for row in timeout_transactions:
                    try:
                        count = int(row.get("timeout_count") or row.get("TIMEOUT_COUNT") or 0)
                        total_timeouts += count
                    except (ValueError, TypeError):
                        continue
                if total_timeouts > 0:
                    tenant_record.add_record("[WARNING] Found {0} aborted transactions in the last hour".format(total_timeouts))
                    tenant_record.add_suggest("Multiple transaction timeouts detected. This may be caused by memory throttling. " "When throttling is active, transactions may timeout and be killed, leading to " "merge failures and inner SQL timeouts.")

        except Exception as e:
            tenant_record.add_record("Error checking throttling-related timeouts: {0}".format(e))
            self.stdio.warn("Error checking throttling-related timeouts: {0}".format(e))

    def get_info__all_virtual_compaction_diagnose_info(self, tenant_record):
        """Legacy method for backward compatibility"""
        try:
            sql = "SELECT * FROM oceanbase.__all_virtual_compaction_diagnose_info WHERE IS_ERROR = 'NO' OR IS_SUSPENDED = 'NO';"
            data = self._execute_sql_safe(sql, "get diagnose info")
            if data:
                tenant_record.add_record("Diagnose info: {0}".format(str(data)))
        except Exception as e:
            raise RCAExecuteException("Error getting diagnose info: {0}".format(e))

    def diagnose_info_switch(self, sql_data, tenant_record):
        """Legacy method - delegates to _analyze_diagnose_info"""
        self._analyze_diagnose_info(sql_data, tenant_record)

    def get_scene_info(self):
        return {
            "name": "major_hold",
            "info_en": "Root cause analysis for major compaction hold issues. Checks for errors, suspensions, long-running tasks, duplicate index names, DDL task status, memory throttling issues (Issue #1107), and provides detailed diagnostics.",
            "info_cn": "针对合并卡住场景的根因分析。检查合并错误、暂停状态、长时间运行的任务、同名索引冲突、DDL任务状态、内存限速问题（Issue #1107），并提供详细的诊断信息。",
        }


major_hold = MajorHoldScene()
