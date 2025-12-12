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
@time: 2024/05/28
@file: transaction_disconnection_scene.py
@desc: Root cause analysis for session disconnection during transaction.
       Common causes: ob_trx_idle_timeout, ob_trx_timeout exceeded
       Reference: [4.0] 事务问题通用排查手册
"""
import os
import re

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException
from src.handler.rca.rca_handler import RcaScene
from src.common.tool import StringUtils


class TransactionDisconnectionScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.work_path = self.store_dir
        self.tenant_id = None

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

        self.tenant_id = self.input_parameters.get('tenant_id')

    def verbose(self, info):
        self.stdio.verbose("[TransactionDisconnectionScene] {0}".format(info))

    def execute(self):
        try:
            # Step 1: Check session/system timeout configuration
            self.verbose("Step 1: Checking timeout configuration")
            self._check_timeout_configuration()

            # get the syslog_level
            syslog_level_data = self.ob_connector.execute_sql_return_cursor_dictionary(' SHOW PARAMETERS like "syslog_level"').fetchall()
            self.record.add_record("syslog_level data is {0}".format(syslog_level_data[0].get("value") or None))

            # Step 2: Gather log about "session is kill"
            self.verbose("Step 2: Searching for 'session is kill' logs")
            work_path_session_killed_log = self.work_path + "/session_killed_log"
            self.gather_log.grep("session is kill")
            if self.input_parameters.get("since") is not None:
                since = self.input_parameters.get("since")
                self.gather_log.set_parameters("since", since)
            logs_name = self.gather_log.execute(save_path=work_path_session_killed_log)

            if logs_name is None or len(logs_name) <= 0:
                self.record.add_record("No 'session is kill' logs found")
                self.record.add_suggest("No 'session is kill' logs found. The disconnection may have occurred " "for other reasons (network issues, client-side timeout, etc.). " "Please check network connectivity and client configuration.")
                return

            # Step 3: Extract session IDs and analyze
            self.verbose("Step 3: Extracting session IDs")
            sessid_list = []
            check_nu = 10  # Limit to 10 sessions

            for log_name in logs_name:
                if check_nu == 0:
                    break
                try:
                    with open(log_name, "r") as f:
                        lines = f.readlines()
                        for line in lines:
                            if "session is kill" in line and "sessid_=" in line:
                                # Extract session ID
                                match = re.search(r'sessid_=(\d+)', line)
                                if match:
                                    sessid = match.group(1)
                                    if sessid not in sessid_list:
                                        sessid_list.append(sessid)
                                        check_nu -= 1
                                        if check_nu == 0:
                                            break

                                # Try to extract the reason
                                self._analyze_kill_reason(line)
                except Exception as e:
                    self.verbose("Error reading log file {0}: {1}".format(log_name, e))

            if len(sessid_list) == 0:
                self.record.add_record("No session IDs found in 'session is kill' logs")
                self.record.add_suggest("Please check the log files in {0}".format(work_path_session_killed_log))
                return

            self.record.add_record("Found {0} killed sessions: {1}".format(len(sessid_list), sessid_list))

            # Step 4: Gather detailed logs for each session
            self.verbose("Step 4: Gathering detailed logs for each session")
            for sessid in sessid_list[:5]:  # Limit to first 5 sessions
                work_path_session_id = self.work_path + "/session_killed_log_{0}".format(sessid)
                self.gather_log.grep(sessid)
                if self.input_parameters.get("since") is not None:
                    since = self.input_parameters.get("since")
                    self.gather_log.set_parameters("since", since)
                self.gather_log.execute(save_path=work_path_session_id)
                self.record.add_record("Session {0} logs gathered to {1}".format(sessid, work_path_session_id))

            self.record.add_suggest("Session disconnection logs gathered. Please check the log files in {0}. " "Common causes: ob_trx_idle_timeout or ob_trx_timeout exceeded.".format(work_path_session_killed_log))

        except Exception as e:
            raise RCAExecuteException("TransactionDisconnectionScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionDisconnectionScene execute")

    def _check_timeout_configuration(self):
        """Check session/system timeout configuration"""
        try:
            # Check system-level parameters
            params_to_check = ['ob_trx_idle_timeout', 'ob_trx_timeout', 'ob_query_timeout']

            for param in params_to_check:
                sql = "SHOW PARAMETERS LIKE '{0}'".format(param)
                try:
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    if result and len(result) > 0:
                        value = result[0].get("value") or result[0].get("VALUE")
                        self.record.add_record("System parameter {0} = {1}".format(param, value))
                except Exception:
                    pass

            # Check tenant-level variables if tenant_id is provided
            if self.tenant_id:
                for var in ['ob_trx_idle_timeout', 'ob_trx_timeout', 'ob_query_timeout']:
                    sql = "SELECT * FROM oceanbase.CDB_OB_SYS_VARIABLES WHERE tenant_id={0} AND NAME='{1}'".format(self.tenant_id, var)
                    try:
                        result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                        if result and len(result) > 0:
                            value = result[0].get("VALUE") or result[0].get("value")
                            self.record.add_record("Tenant {0} variable {1} = {2}".format(self.tenant_id, var, value))
                    except Exception:
                        pass

            self.record.add_suggest(
                "Timeout configuration checked. Key parameters: "
                "ob_trx_idle_timeout (idle transaction timeout), "
                "ob_trx_timeout (transaction timeout), "
                "ob_query_timeout (query timeout). "
                "If these values are too low, they may cause unexpected disconnections."
            )

        except Exception as e:
            self.verbose("Error checking timeout configuration: {0}".format(e))

    def _analyze_kill_reason(self, log_line):
        """Analyze the reason for session kill from log line"""
        # Check for specific error codes
        if "ret=-5066" in log_line:
            self.record.add_record("Session killed with error -5066 (SESSION_KILLED)")

        # Check for idle timeout
        if "ob_trx_idle_timeout" in log_line.lower() or "idle" in log_line.lower():
            self.record.add_record("Session may have been killed due to idle timeout")
            self.record.add_suggest("Session was killed due to ob_trx_idle_timeout. " "The transaction was idle for too long. " "Consider increasing ob_trx_idle_timeout if this is expected behavior.")

        # Check for transaction timeout
        if "ob_trx_timeout" in log_line.lower() or "trx_timeout" in log_line.lower():
            self.record.add_record("Session may have been killed due to transaction timeout")
            self.record.add_suggest("Session was killed due to ob_trx_timeout. " "The transaction took too long to complete. " "Consider optimizing the transaction or increasing ob_trx_timeout.")

        # Check state information
        match = re.search(r'state=(\d+)', log_line)
        if match:
            state = match.group(1)
            # State 4 typically indicates killed state
            if state == "4":
                self.record.add_record("Session state is 4 (KILLED)")

    def get_scene_info(self):
        return {
            "name": "transaction_disconnection",
            "info_en": "Root cause analysis for session disconnection during transaction. Checks timeout configuration (ob_trx_idle_timeout, ob_trx_timeout) and analyzes session kill logs.",
            "info_cn": "事务断连场景的根因分析，检查超时配置（ob_trx_idle_timeout、ob_trx_timeout）并分析session被kill的日志",
        }


transaction_disconnection = TransactionDisconnectionScene()
