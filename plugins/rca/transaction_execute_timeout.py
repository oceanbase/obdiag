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
@time: 2024/05/20
@file: transaction_execute_timeout_scene.py
@desc: Root cause analysis for transaction/statement timeout errors.
       Error code: 4012 (internal: -6212 statement timeout, -6210 transaction timeout)
       Reference: [4.0] 事务问题通用排查手册
"""
import os
import re

from src.handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
)
from src.handler.rca.rca_handler import RcaScene
from src.common.tool import StringUtils

# Common retry error codes that may cause timeout
RETRY_ERROR_CODES = {
    "4038": "Location cache stale, LS or leader not on this server",
    "4225": "Location cache stale, LS or leader not on this server",
    "4653": "Location info not found during table plan generation",
    "4654": "Location info not found during table plan generation",
    "6005": "Row lock conflict, waiting for lock",
}


class TransactionExecuteTimeoutScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.tenant_id = None
        self.err_type = None
        self.trace_id = None
        self.work_path = self.store_dir

    def init(self, context):
        super().init(context)
        ## observer version>4.0.0.0
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            self.stdio.error("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
            raise RCAInitException("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.work_path = context.get_variable("store_dir")
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)

        self.err_type = self.input_parameters.get('err_type')
        if self.err_type is None or len(self.err_type.strip()) == 0:
            raise RCAInitException("err_type is None. Please provide --env err_type='statement is timeout' or 'transaction is timeout'")
        self.err_type = self.err_type.strip().lower()
        if self.err_type not in ["statement is timeout", "transaction is timeout"]:
            raise RCAInitException("err_type is {0}. It should be 'statement is timeout' or 'transaction is timeout'.".format(self.err_type))
        self.record.add_record("err_type is {0}".format(self.err_type))

    def verbose(self, info):
        self.stdio.verbose("[TransactionExecuteTimeoutScene] {0}".format(info))

    def execute(self):
        try:
            syslog_level_data = self.ob_connector.execute_sql_return_cursor_dictionary('SHOW PARAMETERS like "syslog_level"').fetchall()
            self.record.add_record("syslog_level data is {0}".format(syslog_level_data[0].get("value") or None))

            if self.err_type == "statement is timeout":
                self.verbose("Analyzing statement timeout")
                self.execute_statement()
            elif self.err_type == "transaction is timeout":
                self.verbose("Analyzing transaction timeout")
                self.execute_transaction()

        except Exception as e:
            raise RCAExecuteException("TransactionExecuteTimeoutScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionExecuteTimeoutScene execute")

    def execute_statement(self):
        """Analyze statement timeout"""
        # Get tenant_id
        self.tenant_id = self.input_parameters.get('tenant_id')
        if self.tenant_id is not None:
            if type(self.tenant_id) in [int, str]:
                self.tenant_id = int(self.tenant_id)
        else:
            raise RCAInitException("tenant_id is None. 'statement is timeout' analysis needs it. Please provide --env tenant_id=xxx")

        self.record.add_record("tenant_id is {0}".format(self.tenant_id))

        # Get ob_query_timeout
        ob_query_timeout_cursor = self.ob_connector.execute_sql_return_cursor_dictionary(
            "SELECT * FROM oceanbase.CDB_OB_SYS_VARIABLES WHERE tenant_id='{0}' AND NAME='ob_query_timeout';".format(self.tenant_id)
        )
        ob_query_timeout_data = ob_query_timeout_cursor.fetchall()
        if len(ob_query_timeout_data) == 0:
            raise RCAInitException("ob_query_timeout not found. Please check the tenant_id.")
        ob_query_timeout = ob_query_timeout_data[0].get("VALUE")
        self.record.add_record("ob_query_timeout is {0} (microseconds)".format(ob_query_timeout))

        # Get trace_id
        self.trace_id = self.input_parameters.get('trace_id')
        if self.trace_id is None or len(self.trace_id.strip()) == 0:
            self.record.add_record("trace_id not provided, will analyze general timeout causes")
            self._analyze_general_timeout_causes()
            return

        self.record.add_record("trace_id is {0}".format(self.trace_id))

        # Gather log about trace_id
        work_path_trace_id = self.work_path + "/" + self.trace_id
        self.gather_log.grep("{0}".format(self.trace_id))
        logs_name = self.gather_log.execute(save_path=work_path_trace_id)

        if not logs_name or len(logs_name) == 0:
            self.record.add_record("No logs found for trace_id: {0}".format(self.trace_id))
            self._analyze_general_timeout_causes()
            return

        # Find cur_query_start_time and timeout_timestamp
        cur_query_start_time = None
        timeout_timestamp = None

        for log_name in logs_name:
            if cur_query_start_time and timeout_timestamp:
                break
            try:
                with open(log_name, 'r', encoding='utf-8') as f:
                    content = f.read()

                    # Find query start time
                    if cur_query_start_time is None:
                        match = re.search(r'current_time=(\d+)', content)
                        if match:
                            cur_query_start_time = match.group(1)
                        else:
                            match = re.search(r'query_start_time=(\d+)', content)
                            if match:
                                cur_query_start_time = match.group(1)

                    # Find timeout timestamp
                    if timeout_timestamp is None:
                        match = re.search(r'timeout_timestamp=(\d+)', content)
                        if match:
                            timeout_timestamp = match.group(1)

                    # Check for retry errors
                    self._check_retry_errors_in_log(content)

            except Exception as e:
                self.verbose("Error reading log file {0}: {1}".format(log_name, e))

        if cur_query_start_time:
            self.record.add_record("cur_query_start_time is {0}".format(cur_query_start_time))
        if timeout_timestamp:
            self.record.add_record("timeout_timestamp is {0}".format(timeout_timestamp))

        if cur_query_start_time is None or timeout_timestamp is None:
            self.record.add_record("Cannot find cur_query_start_time or timeout_timestamp")
            self.record.add_suggest("Cannot find timing information in logs. Please provide more context.")
            return

        # Check if timeout is expected
        time_diff = int(timeout_timestamp or 0) - int(cur_query_start_time or 0)
        self.record.add_record("Time difference: {0} microseconds".format(time_diff))

        if time_diff >= int(ob_query_timeout or 0):
            self.record.add_record("timeout_timestamp - cur_query_start_time >= ob_query_timeout")
            self.record.add_suggest(
                "The timeout duration matches ob_query_timeout setting. This is expected behavior. "
                "If you need longer query time, please adjust ob_query_timeout."
            )
        else:
            self.record.add_record("timeout_timestamp - cur_query_start_time < ob_query_timeout")
            self.record.add_suggest(
                "The timeout occurred before ob_query_timeout was reached. This is unexpected. "
                "Please contact OceanBase community for further analysis."
            )

    def execute_transaction(self):
        """Analyze transaction timeout"""
        self.tenant_id = self.input_parameters.get('tenant_id')

        # Check for queue backlog
        self._check_queue_backlog()

        # Check for retry errors
        self._check_retry_errors()

        # Analyze general timeout causes
        self._analyze_general_timeout_causes()

    def _check_queue_backlog(self):
        """Check for tenant queue backlog using dump tenant logs"""
        work_path_dump_tenant = self.work_path + "/dump_tenant"
        self.gather_log.grep("dump tenant")
        logs_name = self.gather_log.execute(save_path=work_path_dump_tenant)

        if logs_name is None or len(logs_name) == 0:
            self.record.add_record("No 'dump tenant' logs found")
            return

        for log_name in logs_name:
            try:
                with open(log_name, 'r', encoding='utf-8') as f:
                    content = f.readlines()
                    for line in content:
                        if 'dump tenant' in line:
                            # Check req_queue total_size
                            match = re.search(r'req_queue:total_size=(\d+)', line)
                            if match:
                                total_size = int(match.group(1))
                                if total_size != 0:
                                    # Get tenant_id
                                    match_tenant_id = re.search(r'tenant=\{id:(\d+)', line)
                                    if match_tenant_id:
                                        tenant_id = match_tenant_id.group(1)
                                        self.record.add_record(
                                            "Found queue backlog: tenant_id={0}, total_size={1}".format(tenant_id, total_size)
                                        )
                                        self.record.add_suggest(
                                            "Tenant {0} has queue backlog (total_size={1}). "
                                            "This may cause request delays and timeouts. "
                                            "Consider increasing tenant worker resources.".format(tenant_id, total_size)
                                        )
                                        return
            except Exception as e:
                self.verbose("Error reading log file {0}: {1}".format(log_name, e))

        self.record.add_record("No queue backlog found in dump tenant logs")

    def _check_retry_errors(self):
        """Check for common retry errors that may cause timeout"""
        self.record.add_record("Checking for retry errors...")

        for error_code, description in RETRY_ERROR_CODES.items():
            work_path_error = self.work_path + "/error_{0}".format(error_code)
            self.gather_log.grep("ret=-{0}".format(error_code))
            logs_name = self.gather_log.execute(save_path=work_path_error)

            if logs_name and len(logs_name) > 0:
                # Count occurrences
                count = 0
                for log_name in logs_name:
                    try:
                        with open(log_name, 'r') as f:
                            count += len(f.readlines())
                    except Exception:
                        pass

                if count > 10:  # Significant number of retries
                    self.record.add_record("Found {0} occurrences of error -{1}: {2}".format(count, error_code, description))
                    self.record.add_suggest(
                        "Error -{0} ({1}) occurred {2} times. "
                        "Frequent retries due to this error may cause timeout. "
                        "Please check location cache freshness and leader distribution.".format(
                            error_code, description, count
                        )
                    )

    def _check_retry_errors_in_log(self, content):
        """Check for retry errors in specific log content"""
        for error_code, description in RETRY_ERROR_CODES.items():
            if "ret=-{0}".format(error_code) in content:
                self.record.add_record("Found retry error -{0}: {1}".format(error_code, description))

    def _analyze_general_timeout_causes(self):
        """Analyze general causes of timeout"""
        self.record.add_record("Analyzing general timeout causes...")

        # Check for lock conflicts
        work_path_lock = self.work_path + "/lock_conflict"
        self.gather_log.grep("mvcc_write conflict")
        logs_name = self.gather_log.execute(save_path=work_path_lock)

        if logs_name and len(logs_name) > 0:
            self.record.add_record("Found lock conflict logs")
            self.record.add_suggest(
                "Lock conflicts detected. Use 'obdiag rca run --scene=lock_conflict' for detailed analysis."
            )

        # Provide general suggestions
        self.record.add_suggest(
            "Common causes of statement/transaction timeout: "
            "1) ob_query_timeout or ob_trx_timeout set too low; "
            "2) Queue backlog due to insufficient tenant workers; "
            "3) Frequent retries due to location cache issues; "
            "4) Lock conflicts with other transactions; "
            "5) Slow queries or full table scans."
        )

    def get_scene_info(self):
        return {
            "name": "transaction_execute_timeout",
            "info_en": "Root cause analysis for statement/transaction timeout. Analyzes ob_query_timeout, queue backlog, retry errors, and lock conflicts. Error code: 4012 (internal: -6212, -6210)",
            "info_cn": "语句/事务执行超时的根因分析，分析ob_query_timeout设置、队列堆积、重试错误和锁冲突。对应错误码4012（内部错误码-6212/-6210）",
        }


transaction_execute_timeout = TransactionExecuteTimeoutScene()
