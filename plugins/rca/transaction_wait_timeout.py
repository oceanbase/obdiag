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
@file: transaction_wait_timeout_scene.py
@desc: Root cause analysis for lock wait timeout.
       NOTE: This scene is now integrated into lock_conflict scene.
       Supports: "Shared lock conflict" (-6004) and "Lock wait timeout exceeded" (-6003)
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


class TransactionWaitTimeoutScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.conflict_tx_id_value = None
        self.data_trans_id_value = None
        self.error_msg_type = None
        self.error_msg = None
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

        self.error_msg = self.input_parameters.get("error_msg")
        if not self.error_msg:
            raise RCANotNeedExecuteException('error_msg is empty. Please provide error_msg by --env error_msg="Shared lock conflict" or --env error_msg="Lock wait timeout exceeded"')

        if "Shared lock conflict" in self.error_msg:
            self.record.add_record("Error type: Shared lock conflict (-6004)")
            self.error_msg_type = "Shared lock conflict"
        elif "Lock wait timeout exceeded" in self.error_msg:
            self.record.add_record("Error type: Lock wait timeout exceeded (-6003)")
            self.error_msg_type = "Lock wait timeout exceeded"
        else:
            raise RCANotNeedExecuteException('error_msg should contain "Shared lock conflict" or "Lock wait timeout exceeded"')

        # Suggest using lock_conflict scene
        self.stdio.warn("[TransactionWaitTimeoutScene] NOTE: This scene is integrated into 'lock_conflict'. " "You can also use: obdiag rca run --scene=lock_conflict --env error_msg='{0}'".format(self.error_msg))

    def verbose(self, info):
        self.stdio.verbose("[TransactionWaitTimeoutScene] {0}".format(info))

    def execute(self):
        try:
            syslog_level_data = self.ob_connector.execute_sql_return_cursor_dictionary('SHOW PARAMETERS like "syslog_level"').fetchall()
            self.record.add_record("syslog_level data is {0}".format(syslog_level_data[0].get("value") or None))

            if self.error_msg_type == "Shared lock conflict":
                self._analyze_shared_lock_conflict()
            elif self.error_msg_type == "Lock wait timeout exceeded":
                self._analyze_lock_wait_timeout()

        except RCANotNeedExecuteException:
            raise
        except Exception as e:
            raise RCAExecuteException("TransactionWaitTimeoutScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionWaitTimeoutScene execute")

    def _analyze_shared_lock_conflict(self):
        """Analyze Shared lock conflict by searching logs"""
        # Gather log about "lock_for_read need retry"
        work_path_lock = self.work_path + "/lock_for_read"
        self.gather_log.grep("lock_for_read need retry")
        logs_name = self.gather_log.execute(save_path=work_path_lock)

        if not logs_name or len(logs_name) == 0:
            self.record.add_record("No 'lock_for_read need retry' logs found")
            self.record.add_suggest("No lock_for_read logs found. Please check if syslog_level includes WDIAG.")
            return

        self.record.add_record("Found 'lock_for_read need retry' logs in {0}".format(work_path_lock))

        # Find data_trans_id in logs
        data_trans_id_line = None
        for log_name in logs_name:
            try:
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "data_trans_id" in line:
                            data_trans_id_line = line
                            match = re.search(r"data_trans_id_:\{txid:(\d+)\}", line)
                            if match:
                                self.data_trans_id_value = match.group(1)
                                break
                if self.data_trans_id_value:
                    break
            except Exception as e:
                self.verbose("Error reading log: {0}".format(e))

        if self.data_trans_id_value:
            self.record.add_record("Found blocking transaction: tx_id={0}".format(self.data_trans_id_value))

            # Gather logs for the blocking transaction
            work_path_tx = self.work_path + "/data_trans_id_{0}".format(self.data_trans_id_value)
            self.gather_log.grep("{0}".format(self.data_trans_id_value))
            self.gather_log.execute(save_path=work_path_tx)

            self.record.add_suggest(
                "Shared lock conflict caused by transaction (tx_id:{0}) in commit phase. "
                "The read request is waiting for this transaction to complete its commit. "
                "Use 'obdiag rca run --scene=transaction_not_ending --env tx_id={0}' for further analysis. "
                "Logs saved to: {1}".format(self.data_trans_id_value, work_path_tx)
            )
        else:
            self.record.add_record("Could not extract data_trans_id from logs")
            if data_trans_id_line:
                self.record.add_record("Log line: {0}".format(data_trans_id_line[:500]))
            self.record.add_suggest("Please check logs in {0} for data_trans_id information".format(work_path_lock))

    def _analyze_lock_wait_timeout(self):
        """Analyze Lock wait timeout exceeded by searching logs"""
        # Gather log about "mvcc_write conflict"
        work_path_mvcc = self.work_path + "/mvcc_write_conflict"
        self.gather_log.grep("mvcc_write conflict")
        logs_name = self.gather_log.execute(save_path=work_path_mvcc)

        if not logs_name or len(logs_name) == 0:
            self.record.add_record("No 'mvcc_write conflict' logs found")
            self.record.add_suggest("No mvcc_write conflict logs found. Please check if syslog_level includes INFO.")
            return

        self.record.add_record("Found 'mvcc_write conflict' logs in {0}".format(work_path_mvcc))

        # Find conflict_tx_id in logs
        conflict_tx_id_line = None
        for log_name in logs_name:
            try:
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "conflict_tx_id" in line:
                            conflict_tx_id_line = line
                            match = re.search(r"conflict_tx_id=\{txid:(\d+)\}", line)
                            if match:
                                self.conflict_tx_id_value = match.group(1)
                                break
                if self.conflict_tx_id_value:
                    break
            except Exception as e:
                self.verbose("Error reading log: {0}".format(e))

        if self.conflict_tx_id_value:
            self.record.add_record("Found blocking transaction: conflict_tx_id={0}".format(self.conflict_tx_id_value))
            self.record.add_suggest(
                "Lock wait timeout caused by transaction (tx_id:{0}) holding row lock and not completing. "
                "To resolve: "
                "1) Wait for the transaction to complete; "
                "2) Kill the blocking session; "
                "3) Use 'obdiag rca run --scene=transaction_not_ending --env tx_id={0}' for further analysis.".format(self.conflict_tx_id_value)
            )
        else:
            self.record.add_record("Could not extract conflict_tx_id from logs")
            if conflict_tx_id_line:
                self.record.add_record("Log line: {0}".format(conflict_tx_id_line[:500]))
            self.record.add_suggest("Please check logs in {0} for conflict_tx_id information".format(work_path_mvcc))

    def get_scene_info(self):
        return {
            "name": "transaction_wait_timeout",
            "info_en": "[Deprecated] Root cause analysis for lock wait timeout. Supports 'Shared lock conflict' (-6004) and 'Lock wait timeout exceeded' (-6003). Please use 'lock_conflict' scene instead: obdiag rca run --scene=lock_conflict --env error_msg=\"...\"",
            "info_cn": "[已废弃] 锁等待超时的根因分析，支持'Shared lock conflict'(-6004)和'Lock wait timeout exceeded'(-6003)。请使用'lock_conflict'场景: obdiag rca run --scene=lock_conflict --env error_msg=\"...\"",
        }


transaction_wait_timeout = TransactionWaitTimeoutScene()
