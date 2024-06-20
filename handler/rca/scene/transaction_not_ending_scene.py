#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
@desc:
"""
import datetime
import os
import re

from handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
    RCANotNeedExecuteException,
)
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import StringUtils


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
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        self.stdio.warn("[TransactionNotEndingScene] is a beta scene. It may not work well.")
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
            transaction = None
            if self.tx_id is not None:
                self.record.add_record("tx_id is {0}".format(self.tx_id))
                transaction_datas = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.__all_virtual_trans_stat where tx_id!='{0}';".format(self.tx_id)).fetchall()
                pass
            if self.phase is None:
                raise RCANotNeedExecuteException("phase is None. Please check --input_parameters")
            else:
                if self.phase.strip().upper() == "UNSUBMITTED":
                    self.execute_unsubmitted_phase()
                elif self.phase.strip().upper() == "COMMIT":
                    self.execute_commit_phase()
                elif self.phase.strip().upper() == "REPLAY":
                    self.execute_replay_phase()
                else:
                    raise RCANotNeedExecuteException("phase is {0}, not support. Just support 'UNSUBMITTED', 'COMMIT', 'REPLAY'. Please check --input_parameters".format(self.phase))

        except Exception as e:
            raise RCAExecuteException("TransactionNotEndingScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionNotEndingScene execute")

    # Transactions in the non commit phase do not have a corresponding transaction ID
    def execute_unsubmitted_phase(self):
        tx_id = self.input_parameters.get("tx_id")
        if tx_id is None:
            raise RCANotNeedExecuteException("tx_id is None. Please check --input_parameters")
        # found the
        transaction_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS where state='ACTION' and tx_id={0};".format(tx_id)).fetchall()
        if len(transaction_data) > 0:
            self.record.add_record("The following transactions are in the non-commit phase: transaction_nu: {0}".format(len(transaction_data)))
        else:
            self.record.add_record("There is no transaction in the non-commit phase.")
            self.record.add_suggest("Please check the transaction is retry ")

    # Transactions in the commit phase
    def execute_commit_phase(self):
        tx_id = self.input_parameters.get("tx_id")
        if tx_id is None:
            raise RCANotNeedExecuteException("tx_id is None. Please check --input_parameters")
        transaction_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS where state<>'ACTION'  and tx_id={0};".format(tx_id)).fetchall()
        if len(transaction_data) > 0:
            self.record.add_record("the data in GV$OB_TRANSACTION_PARTICIPANTS by tx_id: {0}".format(transaction_data))
            # to log_no_leader
            self.record.add_suggest("please check the clog_leader by ' obdiag rca run --scene=log_error ' ")
        else:
            self.record.add_record("There is on transaction in the commit phase.")
            self.record.add_suggest("not get data in GV$OB_TRANSACTION_PARTICIPANTS by tx_id: {0}".format(tx_id))
        pass

    # Transaction context in replay phase does not end
    def execute_replay_phase(self):
        transaction_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * FROM oceanbase.GV$OB_TRANSACTION_PARTICIPANTS where ctx_create_time==expired_time;").fetchall()
        if len(transaction_data) > 0:
            self.record.add_record("the data in GV$OB_TRANSACTION_PARTICIPANTS replay: {0}".format(transaction_data))
            # to log_no_leader
            self.record.add_suggest("Transaction not ending due to asynchronous backup machine. ")
        else:
            self.record.add_record("There is no transaction in the replay phase.")
            self.record.add_suggest("not get data in GV$OB_TRANSACTION_PARTICIPANTS replay")
            return
        pass

    def get_scene_info(self):
        return {
            "name": "transaction_not_ending",
            "info_en": "transaction wait timeout error (beta), error_code like -4012",
            "info_cn": "事务不结束场景（测试板），目前使用较为复杂",
        }


transaction_not_ending = TransactionNotEndingScene()
