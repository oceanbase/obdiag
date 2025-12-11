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
@time: 2025/01/09
@file: suspend_transaction.py
@desc: Root cause analysis for suspended/not-ending transactions.
       Analyzes: no_leader, memstore full, disk full, clog disk full, replay delay,
       and commit phase issues (sub_state, busy_cbs).
       NOTE: This scene includes functionality from transaction_not_ending.
       Reference: [4.0] 事务问题通用排查手册
"""
import os
import re

from src.handler.rca.rca_exception import RCAInitException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils


class SuspendTransactionScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.work_path = self.store_dir
        self.tenant_name = None
        self.tx_id = None

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

        # Get parameters
        self.tenant_name = self.input_parameters.get("tenant_name")
        self.tx_id = self.input_parameters.get("tx_id")

        if not self.tenant_name and not self.tx_id:
            raise RCAInitException("Please provide tenant_name or tx_id. " "Example: --env tenant_name=test_tenant or --env tx_id=12345")

    def verbose(self, info):
        self.stdio.verbose("[SuspendTransactionScene] {0}".format(info))

    def execute(self):
        record = RCA_ResultRecord()

        # If tx_id is provided, analyze specific transaction
        if self.tx_id:
            self.record.add_record("Analyzing specific transaction: tx_id={0}".format(self.tx_id))
            self._analyze_specific_transaction()
            return

        # Otherwise, analyze by tenant
        if not self.tenant_name:
            raise RCANotNeedExecuteException("tenant_name is required for general suspend transaction analysis")

        record.add_record("tenant_name is {0}".format(self.tenant_name))
        self.record.add_record("tenant_name is {0}".format(self.tenant_name))

        # Step 1: Find suspended transactions
        sql = """
        SELECT count(1) as suspend_transaction_count 
        FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS t, oceanbase.__all_tenant a 
        WHERE t.tenant_id=a.tenant_id 
        AND t.LAST_REQUEST_TIME < date_sub(now(), INTERVAL 600 SECOND) 
        AND a.tenant_name='{0}' 
        AND t.ROLE = 'LEADER' 
        AND t.ACTION <> 'START';
        """.format(
            self.tenant_name
        )

        self.verbose("Checking for suspended transactions: {0}".format(sql))
        try:
            suspend_transaction_count_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("Result: {0}".format(suspend_transaction_count_data))
        except Exception as e:
            self.verbose("Error executing query: {0}".format(str(e)))
            return

        suspend_transaction_count = suspend_transaction_count_data[0]['suspend_transaction_count']
        self.verbose("Suspended transaction count: {0}".format(suspend_transaction_count))

        if suspend_transaction_count == 0:
            self.record.add_record("No suspended transactions found for tenant {0}".format(self.tenant_name))
            raise RCANotNeedExecuteException("No suspended transactions found.")

        self.record.add_record("Found {0} suspended transactions".format(suspend_transaction_count))

        # Step 2: Check for no_leader log stream
        if self._check_no_leader():
            return

        # Step 3: Check memstore usage
        if self._check_memstore_full():
            return

        # Step 4: Check cluster data disk
        if self._check_data_disk_full():
            return

        # Step 5: Check tenant clog disk
        if self._check_clog_disk_full():
            return

        # Step 6: Check clog replay delay
        if self._check_replay_delay():
            return

        # Step 7: Analyze transaction logs for commit phase issues
        self._analyze_commit_phase_logs()

        # If none of the above, provide general suggestion
        self.record.add_record("Could not determine specific cause for suspended transactions")
        self.record.add_suggest("Suspended transactions detected but no common cause found. " "Please contact OceanBase community for further analysis.")

    def _analyze_specific_transaction(self):
        """Analyze a specific transaction by tx_id"""
        # Query transaction info
        sql = "SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS WHERE TX_ID='{0}';".format(self.tx_id)
        try:
            tx_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
        except Exception as e:
            self.record.add_record("Error querying transaction: {0}".format(e))
            return

        if len(tx_data) == 0:
            self.record.add_record("Transaction {0} not found. It may have already ended.".format(self.tx_id))
            self.record.add_suggest("Transaction not found. Please verify the tx_id is correct.")
            return

        self.record.add_record("Found {0} participants for transaction {1}".format(len(tx_data), self.tx_id))

        for row in tx_data:
            state = row.get("STATE") or row.get("state")
            action = row.get("ACTION") or row.get("action")
            ls_id = row.get("LS_ID") or row.get("ls_id")
            svr_ip = row.get("SVR_IP") or row.get("svr_ip")

            self.record.add_record("Participant: state={0}, action={1}, ls_id={2}, svr_ip={3}".format(state, action, ls_id, svr_ip))

        # Gather and analyze transaction logs
        self._analyze_commit_phase_logs()

    def _check_no_leader(self):
        """Check for log stream without leader"""
        sql = """
        SELECT ls_id FROM oceanbase.GV$OB_LOG_STAT a, oceanbase.__all_tenant t 
        WHERE a.tenant_id=t.tenant_id AND t.tenant_name='{0}' 
        GROUP BY a.ls_id 
        HAVING count(CASE WHEN a.role = 'LEADER' THEN 1 END)=0;
        """.format(
            self.tenant_name
        )

        self.verbose("Checking for no_leader: {0}".format(sql))
        try:
            no_leader_ls = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("Result: {0}".format(no_leader_ls))
        except Exception as e:
            self.verbose("Error: {0}".format(str(e)))
            return False

        if len(no_leader_ls) > 0:
            ls_ids = [str(row['ls_id']) for row in no_leader_ls]
            self.record.add_record("Found log streams without leader: {0}".format(", ".join(ls_ids)))
            self.record.add_suggest("CRITICAL: Log stream(s) {0} have no leader! " "This is a serious issue. Please contact OceanBase community immediately.".format(", ".join(ls_ids)))
            return True

        self.verbose("All log streams have leaders")
        return False

    def _check_memstore_full(self):
        """Check for memstore full"""
        sql = """
        SELECT t.tenant_name, m.svr_ip, m.svr_port,
               round(m.active_span / 1024 / 1024 / 1024, 5) active_gb,
               round(m.freeze_trigger / 1024 / 1024 / 1024, 5) trigger_gb,
               round(m.memstore_used / 1024 / 1024 / 1024, 5) used_gb,
               round(m.memstore_limit / 1024 / 1024 / 1024, 5) limit_gb,
               round(m.memstore_used/m.memstore_limit, 5) as memstore_use_ratio,
               m.freeze_cnt freeze_count
        FROM oceanbase.__all_virtual_tenant_memstore_info m
        INNER JOIN oceanbase.__all_tenant t ON t.tenant_id = m.tenant_id
        WHERE t.tenant_name = '{0}';
        """.format(
            self.tenant_name
        )

        self.verbose("Checking memstore: {0}".format(sql))
        try:
            memstore_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("Result: {0}".format(memstore_data))
        except Exception as e:
            self.verbose("Error: {0}".format(str(e)))
            return False

        for row in memstore_data:
            svr_ip = row['svr_ip']
            memstore_use_ratio = row['memstore_use_ratio']

            if memstore_use_ratio >= 1:
                self.record.add_record("Memstore full on {0}: usage ratio = {1}".format(svr_ip, memstore_use_ratio))
                self.record.add_suggest("Memstore is full on {0} (usage: {1}). " "Try expanding tenant memory. " "If that doesn't work, consider restarting the observer.".format(svr_ip, memstore_use_ratio))
                return True

        return False

    def _check_data_disk_full(self):
        """Check for cluster data disk full"""
        sql = """
        SELECT svr_ip, round(DATA_DISK_IN_USE/DATA_DISK_CAPACITY, 2) as data_ratio 
        FROM oceanbase.gv$ob_servers 
        GROUP BY svr_ip;
        """

        self.verbose("Checking data disk: {0}".format(sql))
        try:
            disk_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("Result: {0}".format(disk_data))
        except Exception as e:
            self.verbose("Error: {0}".format(str(e)))
            return False

        for row in disk_data:
            svr_ip = row['svr_ip']
            data_ratio = row['data_ratio']

            if data_ratio >= 0.9:
                self.record.add_record("Data disk full on {0}: usage ratio = {1}".format(svr_ip, data_ratio))
                self.record.add_suggest("Data disk is nearly full on {0} (usage: {1}). " "Please expand data disk capacity.".format(svr_ip, data_ratio))
                return True

        return False

    def _check_clog_disk_full(self):
        """Check for tenant clog disk full"""
        sql = """
        SELECT t.tenant_name, a.svr_ip, round(a.LOG_DISK_IN_USE/a.LOG_DISK_SIZE, 2) as clog_disk_ratio 
        FROM oceanbase.gv$ob_units a, oceanbase.__all_tenant t 
        WHERE a.tenant_id=t.tenant_id AND t.tenant_name='{0}';
        """.format(
            self.tenant_name
        )

        self.verbose("Checking clog disk: {0}".format(sql))
        try:
            clog_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("Result: {0}".format(clog_data))
        except Exception as e:
            self.verbose("Error: {0}".format(str(e)))
            return False

        for row in clog_data:
            svr_ip = row['svr_ip']
            clog_disk_ratio = row['clog_disk_ratio']

            if clog_disk_ratio >= 0.85:
                self.record.add_record("Clog disk nearly full on {0}: usage ratio = {1}".format(svr_ip, clog_disk_ratio))
                self.record.add_suggest("Clog disk is nearly full on {0} (usage: {1}). " "Try expanding clog disk. " "If that doesn't work, run 'obdiag rca run --scene=clog_disk_full'.".format(svr_ip, clog_disk_ratio))
                return True

        return False

    def _check_replay_delay(self):
        """Check for clog replay delay"""
        sql = """
        SELECT a.tenant_name, b.tenant_id, b.svr_ip, b.ls_id, now(),
               b.unsubmitted_log_scn,
               DATE_FORMAT(scn_to_timestamp(b.unsubmitted_log_scn), '%Y-%m-%d %H:%i:%s') as formatted_timestamp,
               TIMESTAMPDIFF(MINUTE, scn_to_timestamp(b.unsubmitted_log_scn), now()) as replay_delay_minutes,
               b.pending_cnt
        FROM oceanbase.__all_virtual_replay_stat b, oceanbase.__all_tenant a
        WHERE b.tenant_id=a.tenant_id AND a.tenant_name='{0}' AND role='FOLLOWER'
        ORDER BY b.ls_id;
        """.format(
            self.tenant_name
        )

        self.verbose("Checking replay delay: {0}".format(sql))
        try:
            replay_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("Result: {0}".format(replay_data))
        except Exception as e:
            self.verbose("Error: {0}".format(str(e)))
            return False

        for row in replay_data:
            svr_ip = row['svr_ip']
            ls_id = row['ls_id']
            replay_delay_minutes = row['replay_delay_minutes']

            if replay_delay_minutes and replay_delay_minutes >= 3:
                self.record.add_record("Replay delay on {0} ls_id={1}: delay = {2} minutes".format(svr_ip, ls_id, replay_delay_minutes))
                self.record.add_suggest("Clog replay is delayed on {0} ls_id={1} by {2} minutes. " "Try expanding tenant specifications. " "If that doesn't work, contact OceanBase community.".format(svr_ip, ls_id, replay_delay_minutes))
                return True

        return False

    def _analyze_commit_phase_logs(self):
        """Analyze transaction logs for commit phase issues (sub_state, busy_cbs)"""
        if not self.tx_id:
            # Get a sample tx_id from suspended transactions
            sql = """
            SELECT t.TX_ID
            FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS t, oceanbase.__all_tenant a
            WHERE t.tenant_id=a.tenant_id
            AND t.LAST_REQUEST_TIME < date_sub(now(), INTERVAL 600 SECOND)
            AND a.tenant_name='{0}'
            AND t.ROLE = 'LEADER'
            AND t.STATE <> 'ACTIVE'
            LIMIT 1;
            """.format(
                self.tenant_name
            )

            try:
                tx_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if len(tx_data) > 0:
                    self.tx_id = tx_data[0]['TX_ID']
                    self.verbose("Using sample tx_id: {0}".format(self.tx_id))
            except Exception as e:
                self.verbose("Error getting sample tx_id: {0}".format(e))
                return

        if not self.tx_id:
            return

        # Gather transaction logs
        work_path_tx = self.work_path + "/tx_{0}".format(self.tx_id)
        self.gather_log.grep("txid:{0}".format(self.tx_id))
        logs_name = self.gather_log.execute(save_path=work_path_tx)

        if not logs_name or len(logs_name) == 0:
            self.verbose("No logs found for tx_id: {0}".format(self.tx_id))
            return

        self.record.add_record("Transaction logs gathered to: {0}".format(work_path_tx))

        # Analyze logs
        for log_file in logs_name:
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    content = f.read()

                    # Check sub_state (waiting for GTS if sub_state & 0x4 == 1)
                    sub_state_match = re.search(r'sub_state:\{flag:(\d+)\}', content)
                    if sub_state_match:
                        sub_state_flag = int(sub_state_match.group(1))
                        if sub_state_flag & 0x4:
                            self.record.add_record("Transaction is waiting for GTS (sub_state & 0x4 = 1)")
                            self.record.add_suggest("Transaction is waiting for GTS. " "Check if ls_id=1 has a leader (GTS service requires ls_id=1 leader).")

                    # Check busy_cbs_.get_size() (clog callback stuck)
                    busy_cbs_match = re.search(r'busy_cbs_\.get_size\(\):(\d+)', content)
                    if busy_cbs_match:
                        busy_cbs_size = int(busy_cbs_match.group(1))
                        if busy_cbs_size > 0:
                            self.record.add_record("Clog callback stuck (busy_cbs_.get_size() = {0})".format(busy_cbs_size))
                            self.record.add_suggest("Clog callback is stuck. This indicates majority replicas may have issues " "(disk full, network failure, or OOM). Check replica status.")

                    # Check for unresponded participant
                    if "unresponded participant" in content:
                        self.record.add_record("Found 'unresponded participant' - coordinator waiting for participant response")
                        self.record.add_suggest("Coordinator is waiting for participant response. " "Check participant status and network connectivity.")

                    # Check for post trans errors
                    if "post trans" in content.lower() and ("fail" in content.lower() or "error" in content.lower()):
                        self.record.add_record("Found RPC errors during transaction")
                        self.record.add_suggest("RPC errors detected during transaction. Check network status.")

            except Exception as e:
                self.verbose("Error analyzing log file {0}: {1}".format(log_file, e))

    def get_scene_info(self):
        return {
            "name": "suspend_transaction",
            "info_en": "Root cause analysis for suspended/not-ending transactions. Analyzes: no_leader, memstore full, disk full, clog replay delay, and commit phase issues (GTS waiting, clog callback stuck).",
            "info_cn": "悬挂/不结束事务的根因分析，包括：无主、memstore满、磁盘满、clog回放延迟、以及提交阶段问题（等待GTS、clog回调卡住）",
        }


suspend_transaction = SuspendTransactionScene()
