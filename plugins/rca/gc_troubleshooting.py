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
@time: 2025/01/XX
@file: gc_troubleshooting.py
@desc: Troubleshooting of GC (Garbage Collection) issues in OceanBase 4.x
"""

import os
import json
from typing import List, Dict, Optional, Any

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils


class GCTroubleshooting(RcaScene):
    def __init__(self):
        super().__init__()
        self.tenant_id = None
        self.work_path = None
        self.local_path = None

    def verbose(self, info):
        self.stdio.verbose("[GCTroubleshooting] {0}".format(info))

    def get_scene_info(self):
        return {
            "name": "gc_troubleshooting",
            "info_en": "Troubleshooting of GC (Garbage Collection) issues in OceanBase 4.x",
            "info_cn": "OceanBase 4.x GC问题排查",
            "example": "obdiag rca run --scene=gc_troubleshooting --env tenant_id=1001",
        }

    def init(self, context):
        super().init(context)
        min_supported_version = "4.0.0.0"
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            self.stdio.error("observer version is {0}, which is less than {1}.".format(observer_version, min_supported_version))
            raise RCAInitException("observer version is {0}, which is less than {1}.".format(observer_version, min_supported_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.work_path = context.get_variable('store_dir')
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        self.local_path = self.work_path

        # Get tenant_id from input parameters
        tenant_id_str = self.input_parameters.get("tenant_id")
        if tenant_id_str:
            try:
                self.tenant_id = int(tenant_id_str)
            except Exception as e:
                raise RCAInitException("tenant_id is not a valid number: {0}".format(tenant_id_str))
        else:
            # If not specified, check all tenants
            self.tenant_id = None

        self.record.add_record("Starting GC troubleshooting. tenant_id: {0}".format(self.tenant_id if self.tenant_id else "all"))

    def execute(self):
        try:
            # Check 1: LS GC status
            self._check_ls_gc_status()

            # Check 2: RS state transition issues
            self._check_rs_state_transition()

            # Check 3: LS resource safe destroy issues
            self._check_ls_safe_destroy()

            # Check 4: GC thread hang issues
            self._check_gc_thread_hang()

            # Check 5: GC dependency issues
            self._check_gc_dependency()

            # Check 6: GC timeout issues
            self._check_gc_timeout()

        except RCANotNeedExecuteException as e:
            self.stdio.print("[Not Need Execute]GCTroubleshooting need not execute: {0}".format(e))
        except Exception as e:
            self.stdio.error("GCTroubleshooting execute error: {0}".format(e))
            raise RCAExecuteException("GCTroubleshooting execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end GCTroubleshooting execute")

    def _check_ls_gc_status(self):
        """Check LS GC status"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 1: LS GC status")
        self.record.add_record("=" * 60)

        try:
            # Check __all_virtual_ls_info for LS status
            if self.tenant_id:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, status, ls_state from oceanbase.__all_virtual_ls_info where tenant_id={0}".format(self.tenant_id)
            else:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, status, ls_state from oceanbase.__all_virtual_ls_info"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            ls_info_data = cursor.fetchall()

            if not ls_info_data:
                self.record.add_record("No log stream information found")
                return

            # Save results
            self._save_sql_result(ls_info_data, "ls_gc_status")

            # Check for LS in abnormal states
            issues_found = False
            abnormal_states = ["WAIT_OFFLINE", "OFFLINE", "CREATING", "CREATED", "CREATING_ABORT"]

            for ls_info in ls_info_data:
                tenant_id = ls_info.get("tenant_id")
                ls_id = ls_info.get("ls_id")
                svr_ip = ls_info.get("svr_ip")
                svr_port = ls_info.get("svr_port")
                status = ls_info.get("status")
                ls_state = ls_info.get("ls_state")

                # Check if LS is in abnormal state for GC
                if status and status in abnormal_states:
                    issues_found = True
                    self.record.add_record("WARNING: tenant_id={0}, ls_id={1}, svr_ip={2}:{3}, status={4}, ls_state={5}".format(tenant_id, ls_id, svr_ip, svr_port, status, ls_state))
                    self.record.add_suggest("LS is in abnormal state {0}. This may indicate GC is blocked or hanging.".format(status))

            if not issues_found:
                self.record.add_record("No abnormal LS GC status found")
            else:
                # Gather related logs
                self._gather_gc_logs("ls_gc_status")

        except Exception as e:
            self.record.add_record("Error checking LS GC status: {0}".format(str(e)))
            self.stdio.error("Error in _check_ls_gc_status: {0}".format(e))

    def _check_rs_state_transition(self):
        """Check RS state transition issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 2: RS state transition")
        self.record.add_record("=" * 60)

        try:
            # Check __all_rootservice_event_history for RS state transition events
            sql = "select * from oceanbase.__all_rootservice_event_history where module like '%gc%' or event like '%gc%' or event like '%state%' order by gmt_create desc limit 50"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            rs_events = cursor.fetchall()

            if rs_events:
                self._save_sql_result(rs_events, "rs_state_transition_events")

                # Check for failed or error events
                failed_events = []
                for event in rs_events:
                    event_name = event.get("event", "")
                    value1 = event.get("value1", "")
                    value2 = event.get("value2", "")
                    gmt_create = event.get("gmt_create", "")

                    # Check for error indicators
                    if any(keyword in str(value1).lower() + str(value2).lower() for keyword in ["fail", "error", "timeout", "hang"]):
                        failed_events.append(event)
                        self.record.add_record("WARNING: RS event at {0}: event={1}, value1={2}, value2={3}".format(gmt_create, event_name, value1, value2))

                if failed_events:
                    self.record.add_record("Found {0} failed RS state transition events".format(len(failed_events)))
                    self.record.add_suggest("RS state transition issues detected. Check RS logs for details.")
                else:
                    self.record.add_record("No failed RS state transition events found")
            else:
                self.record.add_record("No RS state transition events found")

        except Exception as e:
            self.record.add_record("Error checking RS state transition: {0}".format(str(e)))
            self.stdio.error("Error in _check_rs_state_transition: {0}".format(e))

    def _check_ls_safe_destroy(self):
        """Check LS safe destroy issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 3: LS safe destroy")
        self.record.add_record("=" * 60)

        try:
            # Check for LS that cannot be safely destroyed
            # First, check LS in WAIT_OFFLINE or OFFLINE status
            if self.tenant_id:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, status from oceanbase.__all_virtual_ls_info where tenant_id={0} and status in ('WAIT_OFFLINE', 'OFFLINE')".format(self.tenant_id)
            else:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, status from oceanbase.__all_virtual_ls_info where status in ('WAIT_OFFLINE', 'OFFLINE')"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            waiting_ls = cursor.fetchall()

            if waiting_ls:
                self._save_sql_result(waiting_ls, "ls_waiting_destroy")

                self.record.add_record("Found {0} LS waiting for destroy".format(len(waiting_ls)))
                for ls in waiting_ls[:10]:  # Show first 10
                    self.record.add_record("tenant_id={0}, ls_id={1}, svr_ip={2}:{3}, status={4}".format(ls.get("tenant_id"), ls.get("ls_id"), ls.get("svr_ip"), ls.get("svr_port"), ls.get("status")))

                self.record.add_suggest("LS are waiting for safe destroy. This may indicate GC is blocked. Check logs for 'this ls is not safe to destroy'.")
                # Gather related logs
                # If we have tenant_id, use it to search GC thread logs with pattern: T{tenant_id}_GC
                if self.tenant_id:
                    self._gather_gc_logs("ls_safe_destroy", tenant_id=self.tenant_id)
                else:
                    # If no tenant_id, gather general GC logs
                    self._gather_gc_logs("ls_safe_destroy")

            # Check for LS that have been removed from map (query result is empty)
            # This means LS has entered remove ls stage and been removed from ls map
            # Need to search GC thread logs for these LS
            if self.tenant_id:
                # Query all LS for the tenant to see if there are any missing
                sql_all = "select tenant_id, ls_id from oceanbase.__all_virtual_ls_info where tenant_id={0}".format(self.tenant_id)
                self.verbose("Execute SQL: {0}".format(sql_all))
                cursor_all = self.ob_connector.execute_sql_return_cursor_dictionary(sql_all)
                existing_ls = cursor_all.fetchall()
                existing_ls_ids = {ls.get("ls_id") for ls in existing_ls}

                # If we have tenant_id, we can search for GC thread logs
                # Format: grep T{tenant_id}_GC observer.log | grep id:{ls_id}
                self.record.add_record("Checking for LS removed from map (GC thread logs)")
                self.record.add_record("If LS query result is empty, it means LS has entered remove ls stage")
                self.record.add_record("Searching GC thread logs with pattern: T{0}_GC and id:xxxx".format(self.tenant_id))

                # Gather GC thread logs for this tenant
                self._gather_gc_thread_logs_for_tenant(self.tenant_id)
            else:
                # For all tenants, we need to get tenant list first
                sql_tenants = "select distinct tenant_id from oceanbase.__all_virtual_ls_info where tenant_id > 1000"
                self.verbose("Execute SQL: {0}".format(sql_tenants))
                cursor_tenants = self.ob_connector.execute_sql_return_cursor_dictionary(sql_tenants)
                tenants = cursor_tenants.fetchall()

                if tenants:
                    self.record.add_record("Checking GC thread logs for {0} tenants".format(len(tenants)))
                    for tenant in tenants[:10]:  # Limit to first 10 tenants
                        tenant_id = tenant.get("tenant_id")
                        if tenant_id:
                            self._gather_gc_thread_logs_for_tenant(tenant_id)

            if not waiting_ls:
                self.record.add_record("No LS waiting for destroy found in __all_virtual_ls_info")
                self.record.add_record("If LS has been removed from map, check GC thread logs for root cause")

        except Exception as e:
            self.record.add_record("Error checking LS safe destroy: {0}".format(str(e)))
            self.stdio.error("Error in _check_ls_safe_destroy: {0}".format(e))

    def _check_gc_thread_hang(self):
        """Check GC thread hang issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 4: GC thread hang")
        self.record.add_record("=" * 60)

        try:
            # Check for long-running GC operations
            # Query __all_virtual_dag_scheduler for GC-related tasks
            if self.tenant_id:
                sql = "select tenant_id, svr_ip, svr_port, dag_type, status, start_time from oceanbase.__all_virtual_dag_scheduler where tenant_id={0} and dag_type like '%gc%' order by start_time desc limit 50".format(self.tenant_id)
            else:
                sql = "select tenant_id, svr_ip, svr_port, dag_type, status, start_time from oceanbase.__all_virtual_dag_scheduler where dag_type like '%gc%' order by start_time desc limit 50"

            self.verbose("Execute SQL: {0}".format(sql))
            try:
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
                gc_tasks = cursor.fetchall()

                if gc_tasks:
                    self._save_sql_result(gc_tasks, "gc_dag_tasks")

                    # Check for long-running tasks
                    long_running = []
                    for task in gc_tasks:
                        status = task.get("status", "")
                        if status and status not in ["FINISH", "FINISHED"]:
                            long_running.append(task)

                    if long_running:
                        self.record.add_record("Found {0} long-running GC tasks".format(len(long_running)))
                        for task in long_running[:5]:  # Show first 5
                            self.record.add_record(
                                "tenant_id={0}, svr_ip={1}:{2}, dag_type={3}, status={4}, start_time={5}".format(task.get("tenant_id"), task.get("svr_ip"), task.get("svr_port"), task.get("dag_type"), task.get("status"), task.get("start_time"))
                            )
                        self.record.add_suggest("GC tasks are long-running or stuck. This may indicate GC thread hang. Check logs for GC-related errors.")
                        # Gather related logs
                        self._gather_gc_logs("gc_thread_hang")
                    else:
                        self.record.add_record("No long-running GC tasks found")
                else:
                    self.record.add_record("No GC tasks found in DAG scheduler")
            except Exception as e:
                self.verbose("DAG scheduler table may not be available: {0}".format(e))
                self.record.add_record("DAG scheduler information not available (table may not exist in this version)")

            # Gather logs to check for GC hang indicators
            self._gather_gc_logs("gc_thread_hang")

        except Exception as e:
            self.record.add_record("Error checking GC thread hang: {0}".format(str(e)))
            self.stdio.error("Error in _check_gc_thread_hang: {0}".format(e))

    def _check_gc_dependency(self):
        """Check GC dependency issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 5: GC dependency")
        self.record.add_record("=" * 60)

        try:
            # Check for transfer dependency issues
            # This is typically found in logs, so we'll gather logs first
            self._gather_gc_logs("gc_dependency")

            # Check __all_rootservice_event_history for dependency-related events
            sql = "select * from oceanbase.__all_rootservice_event_history where event like '%dependent%' or event like '%transfer%' or value2 like '%dependent%' order by gmt_create desc limit 30"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            dependency_events = cursor.fetchall()

            if dependency_events:
                self._save_sql_result(dependency_events, "gc_dependency_events")

                self.record.add_record("Found {0} dependency-related events".format(len(dependency_events)))
                for event in dependency_events[:5]:  # Show first 5
                    self.record.add_record("Event at {0}: {1}, value1={2}, value2={3}".format(event.get("gmt_create"), event.get("event"), event.get("value1"), event.get("value2")))

                self.record.add_suggest("GC dependency issues detected. Check logs for 'The ls is dependent and is not allowed to be GC'.")
            else:
                self.record.add_record("No GC dependency events found")

        except Exception as e:
            self.record.add_record("Error checking GC dependency: {0}".format(str(e)))
            self.stdio.error("Error in _check_gc_dependency: {0}".format(e))

    def _check_gc_timeout(self):
        """Check GC timeout issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 6: GC timeout")
        self.record.add_record("=" * 60)

        try:
            # Check for GC timeout in logs
            self._gather_gc_logs("gc_timeout")

            # Check __all_rootservice_event_history for timeout events
            sql = "select * from oceanbase.__all_rootservice_event_history where (event like '%timeout%' or event like '%gc%') and (value1 like '%timeout%' or value2 like '%timeout%') order by gmt_create desc limit 30"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            timeout_events = cursor.fetchall()

            if timeout_events:
                self._save_sql_result(timeout_events, "gc_timeout_events")

                self.record.add_record("Found {0} GC timeout events".format(len(timeout_events)))
                for event in timeout_events[:5]:  # Show first 5
                    self.record.add_record("Timeout event at {0}: {1}, value1={2}, value2={3}".format(event.get("gmt_create"), event.get("event"), event.get("value1"), event.get("value2")))

                self.record.add_suggest("GC timeout issues detected. Check system load and resource availability.")
            else:
                self.record.add_record("No GC timeout events found")

        except Exception as e:
            self.record.add_record("Error checking GC timeout: {0}".format(str(e)))
            self.stdio.error("Error in _check_gc_timeout: {0}".format(e))

    def _gather_gc_logs(self, log_type: str, tenant_id: Optional[int] = None, ls_id: Optional[int] = None):
        """Gather logs related to GC issues
        For GC thread logs, search pattern is: grep T{tenant_id}_GC observer.log | grep id:{ls_id}
        This searches for GC thread logs where the module reporting errors in a loop is the root cause
        """
        try:
            work_path_gc = os.path.join(self.local_path, "{0}_logs".format(log_type))
            if not os.path.exists(work_path_gc):
                os.makedirs(work_path_gc)

            self.gather_log.set_parameters("scope", "observer")

            # Common GC-related keywords
            gc_keywords = []

            # If tenant_id is provided, add GC thread log pattern: T{tenant_id}_GC
            # Format: grep T{tenant_id}_GC observer.log | grep id:{ls_id}
            if tenant_id:
                gc_thread_pattern = "T{0}_GC".format(tenant_id)
                gc_keywords.append(gc_thread_pattern)
                self.record.add_record("Adding GC thread log pattern: {0}".format(gc_thread_pattern))

                # If ls_id is also provided, add id pattern
                if ls_id:
                    ls_id_pattern = "id:{0}".format(ls_id)
                    gc_keywords.append(ls_id_pattern)
                    self.record.add_record("Adding LS ID pattern: {0}".format(ls_id_pattern))
                else:
                    # If ls_id is not provided, add generic id: pattern to match any LS
                    gc_keywords.append("id:")
                    self.record.add_record("Adding generic LS ID pattern: id:")

            for keyword in gc_keywords:
                self.gather_log.grep(keyword)

            logs_name = self.gather_log.execute(save_path=work_path_gc)

            if logs_name and len(logs_name) > 0:
                self.record.add_record("Gathered GC related logs: {0} files in {1}".format(len(logs_name), work_path_gc))
                if tenant_id:
                    self.record.add_record("Note: Search pattern is 'grep T{0}_GC observer.log | grep id:xxxx'".format(tenant_id))
                    self.record.add_record("The module that reports errors in a loop is the root cause of not satisfying safe_to_destroy.")
                for log_name in logs_name[:5]:  # Show first 5 log files
                    self.record.add_record("  - {0}".format(log_name))
            else:
                self.record.add_record("No GC related logs found")

        except Exception as e:
            self.stdio.error("Error gathering GC logs: {0}".format(e))
            self.record.add_record("Error gathering GC logs: {0}".format(str(e)))

    def _gather_gc_thread_logs_for_tenant(self, tenant_id: int):
        """Gather GC thread logs for specific tenant
        Format: grep T{tenant_id}_GC observer.log | grep id:{ls_id}
        This is used when LS has been removed from map and we need to find the root cause
        """
        try:
            work_path_gc_thread = os.path.join(self.local_path, "gc_thread_logs_tenant_{0}".format(tenant_id))
            if not os.path.exists(work_path_gc_thread):
                os.makedirs(work_path_gc_thread)

            self.gather_log.set_parameters("scope", "observer")

            # Search for GC thread logs with pattern: T{tenant_id}_GC
            # The pattern should match lines containing both T{tenant_id}_GC and id:{ls_id}
            # We use a pattern that matches GC thread logs for this tenant
            gc_thread_pattern = "T{0}_GC".format(tenant_id)
            self.gather_log.grep(gc_thread_pattern)

            logs_name = self.gather_log.execute(save_path=work_path_gc_thread)

            if logs_name and len(logs_name) > 0:
                self.record.add_record("Gathered GC thread logs for tenant {0}: {1} files in {2}".format(tenant_id, len(logs_name), work_path_gc_thread))
                self.record.add_record("Search pattern: {0}".format(gc_thread_pattern))
                self.record.add_record("Note: These logs contain GC thread information. Look for 'id:xxxx' patterns to identify specific LS.")
                self.record.add_record("The module that reports errors in a loop is the root cause of not satisfying safe_to_destroy.")
                for log_name in logs_name[:5]:  # Show first 5 log files
                    self.record.add_record("  - {0}".format(log_name))
            else:
                self.record.add_record("No GC thread logs found for tenant {0}".format(tenant_id))

        except Exception as e:
            self.stdio.error("Error gathering GC thread logs for tenant {0}: {1}".format(tenant_id, e))
            self.record.add_record("Error gathering GC thread logs for tenant {0}: {1}".format(tenant_id, str(e)))

    def _save_sql_result(self, data: List[Dict], filename: str):
        """Save SQL query result to file"""
        try:
            if not data:
                return

            file_path = os.path.join(self.work_path, "{0}.txt".format(filename))
            with open(file_path, 'w', encoding='utf-8') as f:
                if data:
                    # Write header
                    columns = list(data[0].keys())
                    f.write('\t'.join(columns) + '\n')
                    # Write data
                    for row in data:
                        line = '\t'.join([str(row.get(col, '')) for col in columns])
                        f.write(line + '\n')

            self.verbose("Saved SQL result to {0}".format(file_path))

        except Exception as e:
            self.stdio.error("Error saving SQL result: {0}".format(e))


gc_troubleshooting = GCTroubleshooting()
