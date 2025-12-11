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
@file: transaction_not_ending_scene.py
@desc: Root cause analysis for transaction not ending issues.
       Reference: [4.0] 事务问题通用排查手册
"""
import os
import re

from src.handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
    RCANotNeedExecuteException,
)
from src.handler.rca.rca_handler import RcaScene
from src.common.tool import StringUtils


class TransactionNotEndingScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.phase = None
        self.tx_id = None
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
        # tx_id
        self.tx_id = self.input_parameters.get("tx_id")
        self.phase = self.input_parameters.get("phase")

    def verbose(self, info):
        self.stdio.verbose("[TransactionNotEndingScene] {0}".format(info))

    # The problem of incomplete transactions can be divided into three categories, depending on whether the transaction is in the commit phase.
    def execute(self):
        try:
            syslog_level_data = self.ob_connector.execute_sql_return_cursor_dictionary('SHOW PARAMETERS like "syslog_level"').fetchall()
            self.record.add_record("syslog_level data is {0}".format(syslog_level_data[0].get("value") or None))

            if self.tx_id is not None:
                self.record.add_record("tx_id is {0}".format(self.tx_id))
                # Query transaction participants info
                transaction_datas = self.ob_connector.execute_sql_return_cursor_dictionary(
                    "SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE TX_ID='{0}';".format(self.tx_id)
                ).fetchall()
                self.record.add_record("transaction_data count: {0}".format(len(transaction_datas)))
                if len(transaction_datas) > 0:
                    self.record.add_record("transaction_data: {0}".format(transaction_datas))

            if self.phase is None:
                # Auto detect phase based on transaction state
                if self.tx_id is not None:
                    self._auto_detect_and_analyze()
                else:
                    raise RCANotNeedExecuteException("phase and tx_id are both None. Please provide at least tx_id by --env tx_id=xxx")
            else:
                if self.phase.strip().upper() == "UNSUBMITTED":
                    self.execute_unsubmitted_phase()
                elif self.phase.strip().upper() == "COMMIT":
                    self.execute_commit_phase()
                elif self.phase.strip().upper() == "REPLAY":
                    self.execute_replay_phase()
                else:
                    raise RCANotNeedExecuteException("phase is {0}, not support. Just support 'UNSUBMITTED', 'COMMIT', 'REPLAY'. Please check --env".format(self.phase))

        except RCANotNeedExecuteException:
            raise
        except Exception as e:
            raise RCAExecuteException("TransactionNotEndingScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionNotEndingScene execute")

    def _auto_detect_and_analyze(self):
        """Auto detect transaction phase and analyze"""
        tx_id = self.tx_id
        # Query transaction state
        transaction_data = self.ob_connector.execute_sql_return_cursor_dictionary(
            "SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE TX_ID='{0}';".format(tx_id)
        ).fetchall()

        if len(transaction_data) == 0:
            self.record.add_record("Transaction {0} not found in GV$OB_TRANSACTION_PARTICIPANTS, maybe already ended".format(tx_id))
            self.record.add_suggest("Transaction may have already ended. Please check if the tx_id is correct.")
            return

        # Check state to determine phase
        for row in transaction_data:
            state = row.get("STATE") or row.get("state")
            action = row.get("ACTION") or row.get("action")
            ctx_create_time = row.get("CTX_CREATE_TIME") or row.get("ctx_create_time")
            tx_expired_time = row.get("TX_EXPIRED_TIME") or row.get("tx_expired_time")

            self.record.add_record("Transaction state: {0}, action: {1}".format(state, action))

            # Check if it's a replay context (ctx_create_time == tx_expired_time)
            if ctx_create_time == tx_expired_time:
                self.record.add_record("Detected replay phase transaction (ctx_create_time == tx_expired_time)")
                self.execute_replay_phase()
                return

            # ACTIVE state means not in commit phase
            if state == "ACTIVE":
                self.record.add_record("Transaction is in ACTIVE state (non-commit phase)")
                self.execute_unsubmitted_phase()
                return
            else:
                # Not ACTIVE means in commit phase
                self.record.add_record("Transaction is in {0} state (commit phase)".format(state))
                self.execute_commit_phase()
                return

    # Transactions in the non commit phase
    def execute_unsubmitted_phase(self):
        tx_id = self.tx_id
        if tx_id is None:
            raise RCANotNeedExecuteException("tx_id is None. Please check --env tx_id=xxx")

        # Query transactions in ACTIVE state (non-commit phase)
        transaction_data = self.ob_connector.execute_sql_return_cursor_dictionary(
            "SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE STATE='ACTIVE' AND TX_ID='{0}';".format(tx_id)
        ).fetchall()

        if len(transaction_data) > 0:
            self.record.add_record("Found {0} transaction participants in non-commit phase".format(len(transaction_data)))

            for row in transaction_data:
                action = row.get("ACTION") or row.get("action")
                session_id = row.get("SESSION_ID") or row.get("session_id")

                # Check ACTION field
                # action = 1 NULL, action = 2 START, action = 3 COMMIT, action = 4 ABORT, action = 5 DIED, action = 6 END
                if action == "NULL" or action == 1:
                    self.record.add_record("ACTION is NULL, may have long-running query")
                    self.record.add_suggest("There may be a long-running query. Check session {0}".format(session_id))
                elif action == "START" or action == 2:
                    self.record.add_record("ACTION is START, statement not continuing")
                    self.record.add_suggest(
                        "Statement not continuing. Possible causes: "
                        "1) Tenant queue backlog (check with dump tenant); "
                        "2) Client not sending next statement; "
                        "3) Deadlock between transactions. "
                        "Please check session {0}".format(session_id)
                    )

            # Check for deadlock
            self._check_deadlock()

            # Gather transaction logs
            self._gather_tx_logs(tx_id)
        else:
            self.record.add_record("No transaction in non-commit phase found for tx_id: {0}".format(tx_id))
            self.record.add_suggest("Transaction may have moved to commit phase or already ended")

    # Transactions in the commit phase
    def execute_commit_phase(self):
        tx_id = self.tx_id
        if tx_id is None:
            raise RCANotNeedExecuteException("tx_id is None. Please check --env tx_id=xxx")

        # Query transactions not in ACTIVE state (in commit phase)
        transaction_data = self.ob_connector.execute_sql_return_cursor_dictionary(
            "SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE STATE<>'ACTIVE' AND TX_ID='{0}';".format(tx_id)
        ).fetchall()

        if len(transaction_data) > 0:
            self.record.add_record("Found {0} transaction participants in commit phase".format(len(transaction_data)))

            # Find the participant with smallest state (lagging behind)
            min_state_row = None
            state_order = {"INIT": 10, "REDO_COMPLETE": 20, "PREPARE": 30, "PRE_COMMIT": 40, "COMMIT": 50, "ABORT": 60, "CLEAR": 70}

            for row in transaction_data:
                state = row.get("STATE") or row.get("state")
                if min_state_row is None:
                    min_state_row = row
                else:
                    current_min_state = min_state_row.get("STATE") or min_state_row.get("state")
                    if state_order.get(state, 100) < state_order.get(current_min_state, 100):
                        min_state_row = row

            if min_state_row:
                state = min_state_row.get("STATE") or min_state_row.get("state")
                ls_id = min_state_row.get("LS_ID") or min_state_row.get("ls_id")
                svr_ip = min_state_row.get("SVR_IP") or min_state_row.get("svr_ip")
                self.record.add_record("Lagging participant: state={0}, ls_id={1}, svr_ip={2}".format(state, ls_id, svr_ip))

            # Gather transaction logs to check sub_state and busy_cbs
            self._gather_and_analyze_commit_logs(tx_id)

            # Check log stream leader status
            self._check_log_stream_status()

            self.record.add_suggest(
                "Transaction in commit phase not ending. Possible causes: "
                "1) Waiting for GTS (check sub_state & 0x4); "
                "2) Clog callback stuck (check busy_cbs_.get_size()); "
                "3) Majority replicas have issues (disk full, network failure, OOM). "
                "Please use 'obdiag rca run --scene=log_error' for further analysis."
            )
        else:
            self.record.add_record("No transaction in commit phase found for tx_id: {0}".format(tx_id))
            self.record.add_suggest("Transaction may be in non-commit phase or already ended")

    # Transaction context in replay phase does not end
    def execute_replay_phase(self):
        # Replay context: ctx_create_time = expired_time
        transaction_data = self.ob_connector.execute_sql_return_cursor_dictionary(
            "SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE CTX_CREATE_TIME = TX_EXPIRED_TIME;"
        ).fetchall()

        if len(transaction_data) > 0:
            self.record.add_record("Found {0} replay phase transactions".format(len(transaction_data)))
            self.record.add_record("Replay transactions: {0}".format(transaction_data))

            # Check if follower is lagging
            for row in transaction_data:
                ls_id = row.get("LS_ID") or row.get("ls_id")
                tenant_id = row.get("TENANT_ID") or row.get("tenant_id")
                svr_ip = row.get("SVR_IP") or row.get("svr_ip")

                # Check log sync status
                log_stat = self.ob_connector.execute_sql_return_cursor_dictionary(
                    "SELECT * FROM oceanbase.__all_virtual_log_stat WHERE tenant_id={0} AND ls_id={1};".format(tenant_id, ls_id)
                ).fetchall()

                if len(log_stat) > 0:
                    self.record.add_record("Log stat for ls_id {0}: {1}".format(ls_id, log_stat))

            self.record.add_suggest(
                "Replay phase transaction not ending indicates follower replica is lagging. "
                "Check __all_virtual_log_stat to see if end_lsn is falling behind. "
                "This is often caused by slow replay or network issues."
            )
        else:
            self.record.add_record("No replay phase transaction found")
            self.record.add_suggest("No replay phase transaction found")

    def _check_deadlock(self):
        """Check for deadlock"""
        deadlock_sql = "SELECT * FROM oceanbase.GV$OB_DEADLOCK_EVENT_HISTORY ORDER BY EVENT_ID DESC LIMIT 10;"
        try:
            deadlock_data = self.ob_connector.execute_sql_return_cursor_dictionary(deadlock_sql).fetchall()
            if len(deadlock_data) > 0:
                self.record.add_record("Found recent deadlock events: {0}".format(len(deadlock_data)))
                self.record.add_suggest(
                    "Deadlock detected. Check GV$OB_DEADLOCK_EVENT_HISTORY for details. "
                    "Reference: https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000000639718"
                )
        except Exception as e:
            self.verbose("Failed to check deadlock: {0}".format(e))

    def _check_log_stream_status(self):
        """Check log stream leader status"""
        try:
            no_leader_sql = """
            SELECT ls_id, tenant_id FROM oceanbase.__all_virtual_log_stat 
            GROUP BY tenant_id, ls_id 
            HAVING COUNT(CASE WHEN role = 'LEADER' THEN 1 END) = 0;
            """
            no_leader_data = self.ob_connector.execute_sql_return_cursor_dictionary(no_leader_sql).fetchall()
            if len(no_leader_data) > 0:
                self.record.add_record("Found log streams without leader: {0}".format(no_leader_data))
                self.record.add_suggest("Log stream has no leader. This is a serious issue. Please contact OceanBase community.")

            # Check if ls_id=1 (GTS service) has leader
            gts_check_sql = """
            SELECT tenant_id, ls_id FROM oceanbase.__all_virtual_log_stat 
            WHERE ls_id = 1 
            GROUP BY tenant_id, ls_id 
            HAVING COUNT(CASE WHEN role = 'LEADER' THEN 1 END) = 0;
            """
            gts_no_leader = self.ob_connector.execute_sql_return_cursor_dictionary(gts_check_sql).fetchall()
            if len(gts_no_leader) > 0:
                self.record.add_record("LS_ID=1 (GTS service) has no leader for tenants: {0}".format(gts_no_leader))
                self.record.add_suggest("GTS service (ls_id=1) has no leader. This will cause transaction commit to hang waiting for GTS.")
        except Exception as e:
            self.verbose("Failed to check log stream status: {0}".format(e))

    def _gather_tx_logs(self, tx_id):
        """Gather transaction related logs"""
        try:
            work_path_tx = self.work_path + "/tx_{0}".format(tx_id)
            self.gather_log.grep("txid:{0}".format(tx_id))
            logs_name = self.gather_log.execute(save_path=work_path_tx)
            if logs_name and len(logs_name) > 0:
                self.record.add_record("Transaction logs gathered to: {0}".format(work_path_tx))
        except Exception as e:
            self.verbose("Failed to gather tx logs: {0}".format(e))

    def _gather_and_analyze_commit_logs(self, tx_id):
        """Gather and analyze commit phase logs"""
        try:
            work_path_tx = self.work_path + "/tx_{0}_commit".format(tx_id)
            self.gather_log.grep("txid:{0}".format(tx_id))
            logs_name = self.gather_log.execute(save_path=work_path_tx)

            if logs_name and len(logs_name) > 0:
                self.record.add_record("Transaction commit logs gathered to: {0}".format(work_path_tx))

                # Analyze logs for sub_state and busy_cbs
                for log_file in logs_name:
                    try:
                        with open(log_file, 'r', encoding='utf-8') as f:
                            content = f.read()

                            # Check sub_state (waiting for GTS if sub_state & 0x4 == 1)
                            sub_state_match = re.search(r'sub_state:\{flag:(\d+)\}', content)
                            if sub_state_match:
                                sub_state_flag = int(sub_state_match.group(1))
                                if sub_state_flag & 0x4:
                                    self.record.add_record("sub_state & 0x4 = 1, transaction is waiting for GTS")
                                    self.record.add_suggest("Transaction is waiting for GTS. Check if ls_id=1 has leader.")

                            # Check busy_cbs_.get_size() (clog callback stuck)
                            busy_cbs_match = re.search(r'busy_cbs_\.get_size\(\):(\d+)', content)
                            if busy_cbs_match:
                                busy_cbs_size = int(busy_cbs_match.group(1))
                                if busy_cbs_size > 0:
                                    self.record.add_record("busy_cbs_.get_size() = {0}, clog callback stuck".format(busy_cbs_size))
                                    self.record.add_suggest(
                                        "Clog callback is stuck. This indicates majority replicas may have issues "
                                        "(disk full, network failure, or OOM)."
                                    )

                            # Check for unresponded participant
                            if "unresponded participant" in content:
                                self.record.add_record("Found 'unresponded participant' in logs")
                                self.record.add_suggest(
                                    "Coordinator is waiting for participant response. "
                                    "Check participant status or network connectivity."
                                )

                            # Check for post trans errors
                            if "post trans" in content and ("fail" in content.lower() or "error" in content.lower()):
                                self.record.add_record("Found 'post trans' errors in logs")
                                self.record.add_suggest("RPC errors during transaction commit. Check network status.")

                    except Exception as e:
                        self.verbose("Failed to analyze log file {0}: {1}".format(log_file, e))

        except Exception as e:
            self.verbose("Failed to gather commit logs: {0}".format(e))

    def get_scene_info(self):
        return {
            "name": "transaction_not_ending",
            "info_en": "[Deprecated] Root cause analysis for transaction not ending. Analyzes non-commit phase, commit phase, and replay phase issues. Please use 'suspend_transaction' scene instead: obdiag rca run --scene=suspend_transaction --env tx_id=xxx or --env tenant_name=xxx",
            "info_cn": "[已废弃] 事务不结束场景的根因分析，包括非提交阶段、提交阶段和回放阶段的问题分析。请使用'suspend_transaction'场景: obdiag rca run --scene=suspend_transaction --env tx_id=xxx 或 --env tenant_name=xxx",
        }


transaction_not_ending = TransactionNotEndingScene()
