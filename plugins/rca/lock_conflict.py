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
@time: 2024/07/29
@file: lock_conflict_scene.py
@desc: Root cause analysis for lock conflict issues.
       Supports:
       - GV$OB_LOCKS based analysis (4.2+)
       - __all_virtual_lock_wait_stat based analysis (older versions)
       - Log-based analysis for "Shared lock conflict" and "Lock wait timeout exceeded"
       Reference: [4.0] 事务问题通用排查手册
"""
import json
import os
import re

from src.handler.rca.rca_exception import RCAInitException, RCANotNeedExecuteException, RCAExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils, DateTimeEncoder


class LockConflictScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.tenant_id = None
        self.error_msg = None
        self.work_path = None

    def init(self, context):
        try:
            super().init(context)
            self.work_path = context.get_variable("store_dir")
            if not os.path.exists(self.work_path):
                os.makedirs(self.work_path)
            if self.observer_version is None or len(self.observer_version.strip()) == 0 or self.observer_version == "":
                raise Exception("observer version is None. Please check the NODES conf.")

            # Check for error_msg parameter (for log-based analysis)
            self.error_msg = self.input_parameters.get("error_msg")

        except Exception as e:
            raise RCAInitException("LockConflictScene RCAInitException: ", e)

    def execute(self):
        try:
            # Get tenant_id if provided
            if self.input_parameters.get("tenant_name") is not None:
                tenant_name = self.input_parameters.get("tenant_name")
                tenant_data = self.ob_connector.execute_sql("SELECT tenant_id FROM oceanbase.__all_tenant WHERE tenant_name = '{0}';".format(tenant_name))
                if len(tenant_data) == 0:
                    raise RCAInitException("Cannot find tenant id by tenant name: {0}".format(tenant_name))
                self.tenant_id = tenant_data[0][0]
                self.verbose("tenant_id is {0}".format(self.tenant_id))

            # If error_msg is provided, use log-based analysis
            if self.error_msg:
                self._execute_log_based_analysis()
                return

            # Use version-specific analysis
            if self.observer_version == "4.2.0.0" or StringUtils.compare_versions_greater(self.observer_version, "4.2.0.0"):
                self.__execute_4_2()
            elif StringUtils.compare_versions_greater("4.2.0.0", self.observer_version):
                self.__execute_old()
            else:
                raise Exception("observer version is {0}. Not support".format(self.observer_version))

        except RCANotNeedExecuteException:
            raise
        except Exception as e:
            raise RCAExecuteException("LockConflictScene execute error: {0}".format(e))

    def verbose(self, info):
        self.stdio.verbose("[lock_conflict] {0}".format(info))

    def _execute_log_based_analysis(self):
        """Log-based analysis for lock conflict (from transaction_wait_timeout)"""
        first_record = RCA_ResultRecord(self.stdio)

        if "Shared lock conflict" in self.error_msg:
            first_record.add_record("Error type: Shared lock conflict")
            self._analyze_shared_lock_conflict(first_record)
        elif "Lock wait timeout exceeded" in self.error_msg:
            first_record.add_record("Error type: Lock wait timeout exceeded")
            self._analyze_lock_wait_timeout(first_record)
        else:
            first_record.add_record("error_msg does not contain known lock error patterns")
            first_record.add_suggest("Please provide error_msg containing 'Shared lock conflict' or 'Lock wait timeout exceeded'. " "Or run without error_msg parameter for general lock conflict analysis.")

        self.Result.records.append(first_record)

    def _analyze_shared_lock_conflict(self, record):
        """Analyze Shared lock conflict by searching logs"""
        # Gather log about "lock_for_read need retry"
        work_path_lock = self.work_path + "/lock_for_read"
        self.gather_log.grep("lock_for_read need retry")
        logs_name = self.gather_log.execute(save_path=work_path_lock)

        if not logs_name or len(logs_name) == 0:
            record.add_record("No 'lock_for_read need retry' logs found")
            record.add_suggest("No lock_for_read logs found. Please check if syslog_level includes WDIAG.")
            return

        record.add_record("Found 'lock_for_read need retry' logs in {0}".format(work_path_lock))

        # Find data_trans_id in logs
        data_trans_id_value = None
        for log_name in logs_name:
            try:
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "data_trans_id" in line:
                            match = re.search(r"data_trans_id_:\{txid:(\d+)\}", line)
                            if match:
                                data_trans_id_value = match.group(1)
                                break
                if data_trans_id_value:
                    break
            except Exception as e:
                self.verbose("Error reading log: {0}".format(e))

        if data_trans_id_value:
            record.add_record("Found blocking transaction: tx_id={0}".format(data_trans_id_value))

            # Gather logs for the blocking transaction
            work_path_tx = self.work_path + "/data_trans_id_{0}".format(data_trans_id_value)
            self.gather_log.grep("{0}".format(data_trans_id_value))
            self.gather_log.execute(save_path=work_path_tx)

            record.add_suggest(
                "Shared lock conflict caused by transaction (tx_id:{0}) in commit phase. "
                "This transaction is holding a read lock. "
                "Use 'obdiag rca run --scene=transaction_not_ending --env tx_id={0}' for further analysis. "
                "Logs saved to: {1}".format(data_trans_id_value, work_path_tx)
            )
        else:
            record.add_record("Could not extract data_trans_id from logs")
            record.add_suggest("Please check logs in {0} for data_trans_id information".format(work_path_lock))

    def _analyze_lock_wait_timeout(self, record):
        """Analyze Lock wait timeout exceeded by searching logs"""
        # Gather log about "mvcc_write conflict"
        work_path_mvcc = self.work_path + "/mvcc_write_conflict"
        self.gather_log.grep("mvcc_write conflict")
        logs_name = self.gather_log.execute(save_path=work_path_mvcc)

        if not logs_name or len(logs_name) == 0:
            record.add_record("No 'mvcc_write conflict' logs found")
            record.add_suggest("No mvcc_write conflict logs found. Please check if syslog_level includes INFO.")
            return

        record.add_record("Found 'mvcc_write conflict' logs in {0}".format(work_path_mvcc))

        # Find conflict_tx_id in logs
        conflict_tx_id_value = None
        for log_name in logs_name:
            try:
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "conflict_tx_id" in line:
                            match = re.search(r"conflict_tx_id=\{txid:(\d+)\}", line)
                            if match:
                                conflict_tx_id_value = match.group(1)
                                break
                if conflict_tx_id_value:
                    break
            except Exception as e:
                self.verbose("Error reading log: {0}".format(e))

        if conflict_tx_id_value:
            record.add_record("Found blocking transaction: conflict_tx_id={0}".format(conflict_tx_id_value))
            record.add_suggest(
                "Lock wait timeout caused by transaction (tx_id:{0}) holding row lock. "
                "This transaction is not completing. "
                "Options: 1) Wait for the transaction to complete; "
                "2) Kill the blocking session; "
                "3) Use 'obdiag rca run --scene=transaction_not_ending --env tx_id={0}' for further analysis.".format(conflict_tx_id_value)
            )
        else:
            record.add_record("Could not extract conflict_tx_id from logs")
            record.add_suggest("Please check logs in {0} for conflict_tx_id information".format(work_path_mvcc))

    def __execute_4_2(self):
        """Analysis using GV$OB_LOCKS for OceanBase 4.2+"""
        first_record = RCA_ResultRecord(self.stdio)

        # Query blocked locks
        locks_sql = 'SELECT * FROM oceanbase.GV$OB_LOCKS WHERE BLOCK=1 AND TYPE="TX" '
        if self.tenant_id is not None:
            locks_sql = locks_sql + 'AND tenant_id={0}'.format(self.tenant_id)
        locks_sql = locks_sql + ' LIMIT 100;'

        self.verbose("locks_sql is {0}".format(locks_sql))
        cursor = self.ob_connector.execute_sql_return_cursor_dictionary(locks_sql)
        data = cursor.fetchall()

        if len(data) == 0:
            first_record.add_record("No blocked locks found in GV$OB_LOCKS")
            first_record.add_suggest("No block lock found. Not Need Execute")
            self.Result.records.append(first_record)
            raise RCANotNeedExecuteException("No block lock found.")

        first_record.add_record("Found {0} blocked locks".format(len(data)))

        for OB_LOCKS_data in data:
            trans_record = RCA_ResultRecord(self.stdio)
            first_record_records = first_record.records.copy()
            trans_record.records.extend(first_record_records)
            self.Result.records.append(trans_record)

            try:
                if OB_LOCKS_data.get("ID1") is None:
                    trans_record.add_record("Holding lock trans_id is null")
                    trans_record.add_suggest("Holding lock trans_id is null. Cannot proceed.")
                    continue

                trans_id = OB_LOCKS_data["ID1"]
                trans_record.add_record("Holding lock trans_id: {0}".format(trans_id))

                # Get holding lock session ID
                holding_lock_session_id = trans_id
                cursor_by_trans_id = self.ob_connector.execute_sql_return_cursor_dictionary('SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE TX_ID="{0}" AND SESSION_ID<>0;'.format(trans_id))
                holding_lock_session_id_datas = cursor_by_trans_id.fetchall()

                if len(holding_lock_session_id_datas) > 0:
                    holding_lock_session_id = holding_lock_session_id_datas[0].get("SESSION_ID")
                    trans_record.add_record("Holding lock session_id: {0}".format(holding_lock_session_id))
                else:
                    trans_record.add_record("Holding session not found, may be closed")
                    trans_record.add_suggest("Holding session not found. The session may have been closed.")
                    continue

                # Get waiting lock session ID
                wait_lock_trans_id = OB_LOCKS_data["TRANS_ID"]
                trans_record.add_record("Waiting lock trans_id: {0}".format(wait_lock_trans_id))

                cursor_by_trans_id = self.ob_connector.execute_sql_return_cursor_dictionary('SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE TX_ID="{0}" AND SESSION_ID<>0;'.format(wait_lock_trans_id))
                wait_lock_session_datas = cursor_by_trans_id.fetchall()

                wait_lock_session_id = "not found"
                if len(wait_lock_session_datas) == 0:
                    trans_record.add_record("Waiting session not found")
                    trans_record.add_suggest("Waiting session not found. You can kill holding_lock_session_id: {0}".format(holding_lock_session_id))
                    continue

                wait_lock_session_id = wait_lock_session_datas[0].get("SESSION_ID")
                trans_record.add_record("Waiting lock session_id: {0}".format(wait_lock_session_id))

                # Check SQL_AUDIT for holding lock SQL
                sql_info = "not available"
                cursor_check_switch = self.ob_connector.execute_sql_return_cursor_dictionary("SHOW PARAMETERS LIKE '%enable_sql_audit%';")
                audit_switch_value = cursor_check_switch.fetchone().get("value")

                if audit_switch_value.strip().upper() == "TRUE":
                    holding_lock_sql_info_cursor = self.ob_connector.execute_sql_return_cursor_dictionary('SELECT * FROM oceanbase.gv$OB_SQL_AUDIT WHERE tx_id!=0 AND SID="{0}";'.format(holding_lock_session_id))
                    trans_record.add_record('Executed: SELECT * FROM oceanbase.gv$OB_SQL_AUDIT WHERE SID="{0}"'.format(holding_lock_session_id))
                    holding_lock_sql_info = holding_lock_sql_info_cursor.fetchall()

                    if len(holding_lock_sql_info) == 0:
                        trans_record.add_record("No SQL info found in gv$OB_SQL_AUDIT for session {0}".format(holding_lock_session_id))
                    else:
                        holding_lock_sql_info_json_data = json.dumps(holding_lock_sql_info, cls=DateTimeEncoder)
                        file_name = "{0}/rca_holding_lock_sql_info_{1}.json".format(self.work_path, holding_lock_session_id)
                        with open(file_name, "w+") as f:
                            f.write(str(holding_lock_sql_info_json_data))
                        trans_record.add_record("Holding lock SQL info saved to {0}".format(file_name))
                        sql_info = "saved to {0}".format(file_name)
                else:
                    trans_record.add_record("SQL_AUDIT is disabled, cannot get SQL info")

                trans_record.add_suggest(
                    "Lock conflict detected. "
                    "Holding lock session: {0}; Waiting session: {1}; SQL info: {2}. "
                    "To resolve: kill one of the sessions or wait for the holding transaction to complete.".format(holding_lock_session_id, wait_lock_session_id, sql_info)
                )

            except Exception as e:
                trans_record.add_record("Error processing lock data: {0}".format(e))
                trans_record.add_suggest("Error occurred: {0}".format(e))

    def __execute_old(self):
        """Analysis using __all_virtual_lock_wait_stat for older versions"""
        first_record = RCA_ResultRecord(self.stdio)

        cursor = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * FROM oceanbase.__all_virtual_lock_wait_stat ORDER BY try_lock_times LIMIT 50;")
        virtual_lock_wait_stat_datas = cursor.fetchall()

        if len(virtual_lock_wait_stat_datas) == 0:
            first_record.add_record("No lock wait data found in __all_virtual_lock_wait_stat")
            first_record.add_suggest("No block lock found. Not Need Execute")
            self.Result.records.append(first_record)
            raise RCANotNeedExecuteException("No block lock found.")

        first_record.add_record("Found {0} lock wait entries".format(len(virtual_lock_wait_stat_datas)))

        for trans_lock_data in virtual_lock_wait_stat_datas:
            trans_id = trans_lock_data["block_session_id"]
            trans_record = RCA_ResultRecord(self.stdio)
            first_record_records = first_record.records.copy()
            trans_record.records.extend(first_record_records)
            self.Result.records.append(trans_record)

            trans_record.add_record("Block data: {0}".format(trans_lock_data))
            trans_record.add_record("Block session_id: {0}".format(trans_id))
            trans_record.add_suggest("Lock conflict session ID: {0}. " "To resolve: kill this session to rollback the blocking transaction. " "WARNING: This will rollback the transaction!".format(trans_lock_data.get("block_session_id")))

    def get_scene_info(self):
        return {
            "name": "lock_conflict",
            "info_en": "Root cause analysis for lock conflict. Supports GV$OB_LOCKS analysis (4.2+), log-based analysis for 'Shared lock conflict' and 'Lock wait timeout exceeded'.",
            "info_cn": "锁冲突的根因分析，支持GV$OB_LOCKS分析（4.2+）以及基于日志的'Shared lock conflict'和'Lock wait timeout exceeded'分析",
        }


lock_conflict = LockConflictScene()
