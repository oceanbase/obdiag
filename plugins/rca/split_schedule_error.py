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
@file: split_schedule_error.py
@desc: Diagnose partition auto-split schedule errors
"""
import os
import re
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils


class SplitScheduleError(RcaScene):
    def __init__(self):
        super().__init__()
        self.work_path = None
        # Ignorable error codes
        self.ignorable_error_codes = [-5285, -4007, -4723, -5146, -4767]
        # Error code meanings
        self.error_code_meanings = {
            -5285: "OB_EER_NULL_IN_VALUES_LESS_THAN: partition key column contains null values, split not supported",
            -4007: "OB_NOT_SUPPORTED",
            -4723: "OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST: tablet may be deleted during scheduling",
            -5146: "OB_UNKNOWN_PARTITION: tablet may be deleted during scheduling",
            -4767: "OB_LS_NOT_LEADER",
            -4023: "OB_EAGAIN: retry needed, may indicate partition key sampling issue",
        }

    def init(self, context):
        super().init(context)
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.work_path = context.get_variable('store_dir')
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)

    def verbose(self, info):
        self.stdio.verbose("[SplitScheduleError] {0}".format(info))

    def execute(self):
        try:
            if self.ob_connector is None:
                raise RCAInitException("ob_connector is None. Please check the NODES conf.")

            # Query recent split schedule errors
            self.record.add_record("Start checking partition auto-split schedule errors")
            sql = "select * from oceanbase.__all_rootservice_event_history where module = 'ddl scheduler' and event = 'schedule split task' order by gmt_create desc limit 10"
            self.verbose("Execute SQL: {0}".format(sql))

            split_errors = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if len(split_errors) == 0:
                self.record.add_record("No split schedule errors found in recent records")
                self.record.add_suggest("No split schedule errors detected. The system is operating normally.")
                return

            self.record.add_record("Found {0} recent split schedule error records".format(len(split_errors)))

            # Parse and analyze each error record
            for error_record in split_errors:
                self.__analyze_error_record(error_record)

        except RCANotNeedExecuteException as e:
            self.stdio.print("[Not Need Execute]SplitScheduleError need not execute: {0}".format(e))
        except Exception as e:
            self.stdio.error("SplitScheduleError execute error: {0}; trace:{1}".format(e, traceback.format_exc()))
            raise RCAExecuteException("SplitScheduleError execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end SplitScheduleError execute")

    def __analyze_error_record(self, error_record: Dict):
        """Analyze a single error record"""
        gmt_create = error_record.get('gmt_create')
        rs_svr_ip = error_record.get('rs_svr_ip')
        rs_svr_port = error_record.get('rs_svr_port')
        value2 = error_record.get('value2', '')

        self.record.add_record("Analyzing error record at {0} from RS node {1}:{2}".format(gmt_create, rs_svr_ip, rs_svr_port))

        # Parse task_list from value2
        # Format: (tenant_id,tablet_id,ret_code)(tenant_id,tablet_id,ret_code)...
        task_tuples = self.__parse_task_list(value2)

        if len(task_tuples) == 0:
            self.record.add_record("No task tuples found in value2: {0}".format(value2))
            return

        self.record.add_record("Found {0} task tuples in this error record".format(len(task_tuples)))

        # Group tasks by error code
        tasks_by_error = {}
        for tenant_id, tablet_id, ret_code in task_tuples:
            if ret_code not in tasks_by_error:
                tasks_by_error[ret_code] = []
            tasks_by_error[ret_code].append((tenant_id, tablet_id))

        # Analyze each error code
        for ret_code, tasks in tasks_by_error.items():
            self.__analyze_error_code(ret_code, tasks, gmt_create, rs_svr_ip, rs_svr_port)

    def __parse_task_list(self, value2: str) -> List[Tuple[int, int, int]]:
        """Parse task list from value2 string
        Format: (tenant_id,tablet_id,ret_code)(tenant_id,tablet_id,ret_code)...
        """
        task_tuples = []
        if not value2:
            return task_tuples

        # Match pattern: (number,number,number)
        pattern = r'\((\d+),(\d+),(-?\d+)\)'
        matches = re.findall(pattern, value2)

        for match in matches:
            try:
                tenant_id = int(match[0])
                tablet_id = int(match[1])
                ret_code = int(match[2])
                task_tuples.append((tenant_id, tablet_id, ret_code))
            except ValueError as e:
                self.verbose("Failed to parse task tuple: {0}, error: {1}".format(match, e))

        return task_tuples

    def __analyze_error_code(self, ret_code: int, tasks: List[Tuple[int, int]], gmt_create, rs_svr_ip: str, rs_svr_port: int):
        """Analyze tasks with specific error code"""
        error_msg = self.error_code_meanings.get(ret_code, "Unknown error code")
        self.record.add_record("Error code {0}: {1}, affecting {2} tablets".format(ret_code, error_msg, len(tasks)))

        # Check if error code is ignorable
        if ret_code in self.ignorable_error_codes:
            self.record.add_record("Error code {0} is ignorable, system will retry automatically".format(ret_code))
            self.record.add_suggest("Error code {0} ({1}) can be ignored. The system will retry automatically.".format(ret_code, error_msg))
            return

        # Handle -4023 (OB_EAGAIN) specially
        if ret_code == -4023:
            self.__handle_eagain_error(tasks, gmt_create, rs_svr_ip, rs_svr_port)
        else:
            # Handle other error codes
            self.__handle_other_error_codes(ret_code, tasks, gmt_create, rs_svr_ip, rs_svr_port)

    def __handle_eagain_error(self, tasks: List[Tuple[int, int]], gmt_create, rs_svr_ip: str, rs_svr_port: int):
        """Handle -4023 (OB_EAGAIN) error code"""
        self.record.add_record("Handling -4023 (OB_EAGAIN) error code")

        for tenant_id, tablet_id in tasks:
            self.record.add_record("Checking tablet {0} in tenant {1}".format(tablet_id, tenant_id))

            # Step 1: Check if tablet successfully split after retry
            sql = "select * from oceanbase.__all_virtual_tablet_reorganize_history where src_tablet_id = {0}".format(tablet_id)
            self.verbose("Execute SQL: {0}".format(sql))

            try:
                reorganize_history = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if len(reorganize_history) > 0:
                    self.record.add_record("Tablet {0} successfully split after retry".format(tablet_id))
                    self.record.add_suggest("Tablet {0} in tenant {1} has been successfully split. No further action needed.".format(tablet_id, tenant_id))
                    continue
            except Exception as e:
                self.verbose("Failed to query reorganize history: {0}".format(e))

            # Step 2: Check observer logs for partition key sampling issues
            self.record.add_record("Tablet {0} still failed after retry, checking logs for partition key sampling issues".format(tablet_id))

            # Find RS node
            rs_node = self.__find_rs_node(rs_svr_ip, rs_svr_port)
            if rs_node is None:
                self.record.add_record("Cannot find RS node {0}:{1} in observer_nodes".format(rs_svr_ip, rs_svr_port))
                continue

            # Grep observer logs for partition key sampling error
            self.__grep_observer_logs_for_sampling_error(rs_node, tablet_id, gmt_create)

            # Step 3: Check for other error codes in event history
            sql = "select * from oceanbase.__all_rootservice_event_history where module = 'ddl scheduler' and event = 'schedule split task' and value2 like '%{0}%'".format(tablet_id)
            self.verbose("Execute SQL: {0}".format(sql))

            try:
                other_errors = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                for other_error in other_errors:
                    other_value2 = other_error.get('value2', '')
                    other_tasks = self.__parse_task_list(other_value2)
                    for other_tenant_id, other_tablet_id, other_ret_code in other_tasks:
                        if other_tablet_id == tablet_id and other_ret_code != -4023:
                            self.record.add_record("Found other error code {0} for tablet {1} in tenant {2}".format(other_ret_code, tablet_id, tenant_id))
                            self.__handle_other_error_codes(other_ret_code, [(tenant_id, tablet_id)], other_error.get('gmt_create'), rs_svr_ip, rs_svr_port)
            except Exception as e:
                self.verbose("Failed to query other errors: {0}".format(e))

    def __handle_other_error_codes(self, ret_code: int, tasks: List[Tuple[int, int]], gmt_create, rs_svr_ip: str, rs_svr_port: int):
        """Handle other error codes (not -4023 and not ignorable)"""
        self.record.add_record("Handling error code {0} for detailed diagnosis".format(ret_code))

        # Find RS node
        rs_node = self.__find_rs_node(rs_svr_ip, rs_svr_port)
        if rs_node is None:
            self.record.add_record("Cannot find RS node {0}:{1} in observer_nodes".format(rs_svr_ip, rs_svr_port))
            return

        for tenant_id, tablet_id in tasks:
            self.record.add_record("Diagnosing error code {0} for tablet {1} in tenant {2}".format(ret_code, tablet_id, tenant_id))

            # Step 1: Get table_id from tablet_id
            sql = "select table_id from oceanbase.__all_virtual_tablet_to_table_history where tablet_id = {0} and tenant_id = {1}".format(tablet_id, tenant_id)
            self.verbose("Execute SQL: {0}".format(sql))

            try:
                table_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if len(table_data) == 0:
                    self.record.add_record("Cannot find table_id for tablet {0} in tenant {1}".format(tablet_id, tenant_id))
                    continue

                table_id = table_data[0]['table_id']

                # Step 2: Get table_name from table_id
                sql = "select table_name from oceanbase.__all_virtual_table where table_id = {0}".format(table_id)
                self.verbose("Execute SQL: {0}".format(sql))

                table_name_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if len(table_name_data) == 0:
                    self.record.add_record("Cannot find table_name for table_id {0}".format(table_id))
                    continue

                table_name = table_name_data[0]['table_name']
                self.record.add_record("Tablet {0} belongs to table {1} (table_id: {2})".format(tablet_id, table_name, table_id))

                # Step 3: Grep observer logs for partition_auto_split
                self.__grep_observer_logs_for_split_error(rs_node, tablet_id, gmt_create)

                # Step 4: Grep rootservice logs for alter_table
                self.__grep_rootservice_logs_for_alter_table(rs_node, table_name, gmt_create)

            except Exception as e:
                self.verbose("Failed to get table information: {0}".format(e))
                self.record.add_record("Failed to get table information for tablet {0}: {1}".format(tablet_id, str(e)))

    def __find_rs_node(self, rs_svr_ip: str, rs_svr_port: int):
        """Find RS node from observer_nodes"""
        if self.observer_nodes is None:
            return None

        for node in self.observer_nodes:
            node_ip = node.get("ip")
            node_port = node.get("port")
            if node_ip == rs_svr_ip and node_port == rs_svr_port:
                return node

        return None

    def __grep_observer_logs_for_sampling_error(self, rs_node, tablet_id: int, gmt_create):
        """Grep observer logs for partition key sampling error"""
        self.record.add_record("Grepping observer logs for partition key sampling error around {0}".format(gmt_create))

        try:
            # Calculate time range (gmt_create ± 5 minutes)
            if isinstance(gmt_create, str):
                try:
                    gmt_time = datetime.strptime(gmt_create.split('.')[0], '%Y-%m-%d %H:%M:%S')
                except:
                    gmt_time = datetime.now()
            else:
                gmt_time = gmt_create

            time_from = (gmt_time - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            time_to = (gmt_time + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')

            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.set_parameters("from", time_from)
            self.gather_log.set_parameters("to", time_to)
            self.gather_log.conf_map["filter_nodes_list"] = [rs_node]
            self.gather_log.grep("partition is empty or all data have same partition key")

            logs_name = self.gather_log.execute(save_path="observer_logs_sampling_error")

            if len(logs_name) > 0:
                self.record.add_record("Found partition key sampling error logs for tablet {0}".format(tablet_id))
                self.record.add_suggest("Partition key sampling issue detected. Possible causes:")
                self.record.add_suggest("1. Partition may be empty")
                self.record.add_suggest("2. All partition key values may be the same, causing sampling failure")
                self.record.add_suggest("Please check partition key values using 'SHOW CREATE TABLE' in the tenant")
            else:
                self.verbose("No partition key sampling error logs found")

        except Exception as e:
            self.verbose("Failed to grep observer logs for sampling error: {0}".format(e))

    def __grep_observer_logs_for_split_error(self, rs_node, tablet_id: int, gmt_create):
        """Grep observer logs for partition_auto_split errors"""
        self.record.add_record("Grepping observer logs for partition_auto_split errors around {0}".format(gmt_create))

        try:
            # Calculate time range (gmt_create ± 5 minutes)
            if isinstance(gmt_create, str):
                try:
                    gmt_time = datetime.strptime(gmt_create.split('.')[0], '%Y-%m-%d %H:%M:%S')
                except:
                    gmt_time = datetime.now()
            else:
                gmt_time = gmt_create

            time_from = (gmt_time - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            time_to = (gmt_time + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')

            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.set_parameters("from", time_from)
            self.gather_log.set_parameters("to", time_to)
            self.gather_log.conf_map["filter_nodes_list"] = [rs_node]
            self.gather_log.grep("partition_auto_split")

            logs_name = self.gather_log.execute(save_path="observer_logs_split_error")

            if len(logs_name) > 0:
                self.record.add_record("Found partition_auto_split error logs for tablet {0}".format(tablet_id))
            else:
                self.verbose("No partition_auto_split error logs found")

        except Exception as e:
            self.verbose("Failed to grep observer logs for split error: {0}".format(e))

    def __grep_rootservice_logs_for_alter_table(self, rs_node, table_name: str, gmt_create):
        """Grep rootservice logs for alter_table operations"""
        self.record.add_record("Grepping rootservice logs for alter_table operations for table {0} around {1}".format(table_name, gmt_create))

        try:
            # Calculate time range (gmt_create ± 5 minutes)
            if isinstance(gmt_create, str):
                try:
                    gmt_time = datetime.strptime(gmt_create.split('.')[0], '%Y-%m-%d %H:%M:%S')
                except:
                    gmt_time = datetime.now()
            else:
                gmt_time = gmt_create

            time_from = (gmt_time - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            time_to = (gmt_time + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')

            self.gather_log.set_parameters("scope", "rootservice")
            self.gather_log.set_parameters("from", time_from)
            self.gather_log.set_parameters("to", time_to)
            self.gather_log.conf_map["filter_nodes_list"] = [rs_node]
            self.gather_log.grep("alter_table.*{0}".format(table_name))

            logs_name = self.gather_log.execute(save_path="rootservice_logs_alter_table")

            if len(logs_name) > 0:
                self.record.add_record("Found alter_table logs for table {0}".format(table_name))
                # Try to extract trace_id and grep again
                self.__extract_and_grep_trace_id(rs_node, logs_name, gmt_create)
            else:
                self.verbose("No alter_table logs found for table {0}".format(table_name))

        except Exception as e:
            self.verbose("Failed to grep rootservice logs for alter_table: {0}".format(e))

    def __extract_and_grep_trace_id(self, rs_node, log_files: List[str], gmt_create):
        """Extract trace_id from logs and grep again"""
        self.record.add_record("Extracting trace_id from logs and grepping related logs")

        try:
            trace_ids = set()
            for log_file in log_files:
                try:
                    with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        for line in f:
                            # Try to extract trace_id (format may vary)
                            trace_pattern = r'trace_id[=:]\s*([0-9a-fA-F-]+)'
                            matches = re.findall(trace_pattern, line)
                            trace_ids.update(matches)
                except Exception as e:
                    self.verbose("Failed to read log file {0}: {1}".format(log_file, e))

            if len(trace_ids) == 0:
                self.verbose("No trace_id found in logs")
                return

            # Grep logs with trace_id
            for trace_id in trace_ids:
                self.record.add_record("Grepping logs for trace_id: {0}".format(trace_id))

                # Calculate time range
                if isinstance(gmt_create, str):
                    try:
                        gmt_time = datetime.strptime(gmt_create.split('.')[0], '%Y-%m-%d %H:%M:%S')
                    except:
                        gmt_time = datetime.now()
                else:
                    gmt_time = gmt_create

                time_from = (gmt_time - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
                time_to = (gmt_time + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')

                self.gather_log.set_parameters("scope", "rootservice")
                self.gather_log.set_parameters("from", time_from)
                self.gather_log.set_parameters("to", time_to)
                self.gather_log.conf_map["filter_nodes_list"] = [rs_node]
                self.gather_log.grep(trace_id)

                trace_logs = self.gather_log.execute(save_path="rootservice_logs_trace_{0}".format(trace_id))

                if len(trace_logs) > 0:
                    self.record.add_record("Found {0} log entries for trace_id {1}".format(len(trace_logs), trace_id))

        except Exception as e:
            self.verbose("Failed to extract and grep trace_id: {0}".format(e))

    def get_scene_info(self):
        return {
            "name": "split_schedule_error",
            "info_en": "Diagnose partition auto-split schedule errors",
            "info_cn": "诊断分区自动分裂调度过程中的错误",
            "example": "obdiag rca run --scene=split_schedule_error",
        }


split_schedule_error = SplitScheduleError()
