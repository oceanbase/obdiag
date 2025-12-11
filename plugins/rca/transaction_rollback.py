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
@file: transaction_rollback_scene.py
@desc: Root cause analysis for transaction rollback errors.
       Error codes: 6002 (internal: -6224, -6223, -6211, -6213)
       - -6224: transaction need rollback
       - -6223: Transaction exiting
       - -6211: Transaction is killed
       - -6213: Transaction context does not exist
       Reference: [4.0] 事务问题通用排查手册
"""
import datetime
import os
import re

from src.handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
)
from src.handler.rca.rca_handler import RcaScene
from src.common.tool import StringUtils


class TransactionRollbackScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.work_path = self.store_dir
        self.trans_is_killed_log = None
        self.rollback_reason = None

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

    def verbose(self, info):
        self.stdio.verbose("[TransactionRollbackScene] {0}".format(info))

    def execute(self):
        try:
            # get the syslog_level
            syslog_level_data = self.ob_connector.execute_sql_return_cursor_dictionary(' SHOW PARAMETERS like "syslog_level"').fetchall()
            self.record.add_record("syslog_level data is {0}".format(syslog_level_data[0].get("value") or None))

            # Step 1: gather log about "trans is killed"
            self.verbose("Step 1: Searching for 'trans is killed' logs")
            work_path_trans_is_killed = self.work_path + "/trans_is_killed"
            self.gather_log.grep("trans is killed")
            logs_name = self.gather_log.execute(save_path=work_path_trans_is_killed)

            if logs_name is None or len(logs_name) <= 0:
                self.record.add_record("No log found about 'trans is killed'")
                self.record.add_suggest(
                    "No 'trans is killed' log found. The transaction rollback may be caused by other reasons. "
                    "Please check 'sending error packet' logs for more details."
                )
                return

            self.record.add_record("Found 'trans is killed' logs in {0}".format(work_path_trans_is_killed))

            # Extract trans_id from logs
            trans_id = None
            for log_name in logs_name:
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "trans is killed" in line:
                            self.trans_is_killed_log = line
                            # Try to extract trans_id
                            match = re.search(r'trans_id[=:]\{?txid:(\d+)\}?', line)
                            if match:
                                trans_id = match.group(1)
                            break
                if self.trans_is_killed_log:
                    break

            if self.trans_is_killed_log:
                self.record.add_record("Found trans is killed log: {0}".format(self.trans_is_killed_log[:500]))
                if trans_id:
                    self.record.add_record("Extracted trans_id: {0}".format(trans_id))

            # Step 2: Check for leader switch (switch to follower forcedly)
            self.verbose("Step 2: Checking for leader switch")
            leader_switch_found = self._check_leader_switch()

            # Step 3: Check for transaction timeout
            self.verbose("Step 3: Checking for transaction timeout")
            timeout_found = self._check_transaction_timeout()

            # Step 4: Check election errors if leader switch was found
            if leader_switch_found:
                self.verbose("Step 4: Checking election logs for errors")
                self._check_election_errors()

            # Provide final suggestion based on findings
            if not leader_switch_found and not timeout_found:
                self.record.add_suggest(
                    "Could not determine the exact reason for transaction rollback. "
                    "Please check the transaction logs and contact OceanBase community for further analysis."
                )

        except Exception as e:
            raise RCAExecuteException("TransactionRollbackScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionRollbackScene execute")

    def _check_leader_switch(self):
        """Check for leader switch logs"""
        work_path_switch = self.work_path + "/switch_to_follower"
        self.gather_log.grep("switch to follower forcedly success")
        logs_name = self.gather_log.execute(save_path=work_path_switch)

        if logs_name and len(logs_name) > 0:
            self.record.add_record("Found 'switch to follower forcedly success' logs in {0}".format(work_path_switch))
            self.rollback_reason = "leader_switch"
            self.record.add_suggest(
                "Transaction was killed due to LEADER SWITCH (leader revoke). "
                "The transaction was active when the leader changed to follower. "
                "This is expected behavior during leader changes. "
                "Please check if there were planned or unexpected leader switches."
            )
            return True
        else:
            self.record.add_record("No 'switch to follower forcedly success' logs found")
            return False

    def _check_transaction_timeout(self):
        """Check for transaction timeout"""
        work_path_expired = self.work_path + "/trans_expired_time"
        self.gather_log.grep("trans_expired_time")
        logs_name = self.gather_log.execute(save_path=work_path_expired)

        if logs_name is None or len(logs_name) <= 0:
            self.record.add_record("No 'trans_expired_time' logs found")
            return False

        self.record.add_record("Found 'trans_expired_time' logs in {0}".format(work_path_expired))

        for log in logs_name:
            try:
                with open(log, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "trans_expired_time" in line:
                            # Extract trans_expired_time value
                            match = re.search(r'trans_expired_time[=:](\d+)', line)
                            if match:
                                expired_time_us = int(match.group(1))
                                # Convert to datetime (microseconds to seconds)
                                expired_time_s = expired_time_us / 1e6
                                expired_datetime = datetime.datetime.utcfromtimestamp(expired_time_s)

                                # Extract log timestamp
                                log_time_match = re.search(r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]', line)
                                if log_time_match:
                                    log_time_str = log_time_match.group(1)
                                    log_datetime = datetime.datetime.strptime(log_time_str[:26], '%Y-%m-%d %H:%M:%S.%f')

                                    if expired_datetime <= log_datetime:
                                        self.record.add_record(
                                            "trans_expired_time ({0}) <= log_time ({1})".format(
                                                expired_datetime, log_datetime
                                            )
                                        )
                                        self.rollback_reason = "timeout"
                                        self.record.add_suggest(
                                            "Transaction was killed due to TIMEOUT. "
                                            "The transaction expired before completion. "
                                            "Please check and adjust transaction timeout settings: "
                                            "ob_trx_timeout, ob_query_timeout, ob_trx_idle_timeout."
                                        )
                                        return True
            except Exception as e:
                self.verbose("Error parsing log file {0}: {1}".format(log, e))

        return False

    def _check_election_errors(self):
        """Check election logs for errors that may have caused leader switch"""
        work_path_election = self.work_path + "/election_errors"
        # Set scope to "election" to gather election logs
        self.gather_log.set_parameters("scope", "election")
        self.gather_log.grep("election")
        logs_name = self.gather_log.execute(save_path=work_path_election)

        if logs_name and len(logs_name) > 0:
            error_found = False
            for log_name in logs_name:
                try:
                    with open(log_name, "r") as f:
                        content = f.read()
                        if "ERROR" in content or "error" in content.lower():
                            error_found = True
                            break
                except Exception:
                    pass

            if error_found:
                self.record.add_record("Found ERROR in election logs")
                self.record.add_suggest(
                    "Election errors detected. The leader switch may have been caused by "
                    "abnormal conditions (network issues, disk problems, etc.). "
                    "Please check election logs in {0} for details.".format(work_path_election)
                )
            else:
                self.record.add_record("No ERROR found in election logs")
                self.record.add_suggest(
                    "No election errors found. The leader switch may have been triggered by "
                    "RS scheduling (auto leader rebalancing). "
                    "Please check if auto_leader_switch is enabled."
                )

    def get_scene_info(self):
        return {
            "name": "transaction_rollback",
            "info_en": "Root cause analysis for transaction rollback errors. Analyzes whether rollback was caused by leader switch or timeout. Error code: 6002 (internal: -6224, -6223, -6211, -6213)",
            "info_cn": "事务回滚报错的根因分析，分析回滚是由切主还是超时导致。对应错误码6002（内部错误码-6224/-6223/-6211/-6213）",
        }


transaction_rollback = TransactionRollbackScene()
