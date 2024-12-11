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
@file: transaction_execute_timeout_scene.py
@desc:
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
        self.conflict_tx_id = None
        self.data_trans_id_value = None
        self.error_msg_type = None
        self.error_msg = None
        self.data_trans_id = None
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
            raise RCANotNeedExecuteException('error_msg is empty. Please input error_msg by add --input_parameters="{"err_msg":"xxx"}"')
        if "Shared lock conflict" in self.error_msg:
            self.record.add_record("err_msg type is Shared lock conflict.")
            self.error_msg_type = "Shared lock conflict"
            return True
        elif "Lock wait timeout exceeded" in self.error_msg:
            self.record.add_record("err_msg type is Lock wait timeout exceeded.")
            self.error_msg_type = "Lock wait timeout exceeded"
            return True
        else:
            raise RCANotNeedExecuteException('error_msg is not find useful info. Please check "Shared lock conflict" or "Lock wait timeout exceeded" in error_msg')

    def verbose(self, info):
        self.stdio.verbose("[TransactionWaitTimeoutScene] {0}".format(info))

    def execute(self):
        try:
            syslog_level_data = self.ob_connector.execute_sql_return_cursor_dictionary('SHOW PARAMETERS like "syslog_level"').fetchall()
            self.record.add_record("syslog_level data is {0}".format(syslog_level_data[0].get("value") or None))
            if self.error_msg_type == "Shared lock conflict":
                # gather log about "lock_for_read need retry".
                work_path_trans_is_killed = self.work_path + "/lock_for_read"
                self.gather_log.grep("lock_for_read need retry")
                logs_name = self.gather_log.execute(save_path=work_path_trans_is_killed)
                if logs_name is None or len(logs_name) <= 0:
                    self.record.add_record("no log about 'trans is killed'")
                    return False
                else:
                    self.record.add_record("find 'lock_for_read need retry' in {0}".format(work_path_trans_is_killed))
                # find tx_id 'by data_trans_id' on log
                for log_name in logs_name:
                    with open(log_name, "r") as f:
                        lines = f.readlines()
                        for line in lines:
                            if "data_trans_id" in line:
                                self.data_trans_id = line
                                break
                if self.data_trans_id is None:
                    self.record.add_record("can not find 'data_trans_id' in log.")
                    self.record.add_suggest("can not find 'data_trans_id' in log.")
                    return False
                else:
                    self.record.add_record("find data_trans_id in {0}".format(self.data_trans_id))
                    match = re.search(r"data_trans_id_:\{txid:(\d+)}", self.data_trans_id)
                    if match:
                        self.data_trans_id_value = match.group(1)
                if self.data_trans_id_value is None:
                    self.record.add_record("can not find data_trans_id value in log.")
                    self.record.add_suggest("can not find data_trans_id value in log.")
                    return False
                else:
                    self.record.add_record("find data_trans_id value in {0}".format(self.data_trans_id_value))
                    # gather log about data_trans_id
                    work_path_data_trans_id = self.work_path + "/data_trans_id_{0}".format(self.data_trans_id_value)
                    self.gather_log.grep("{0}".format(self.data_trans_id_value))
                    self.gather_log.execute(save_path=work_path_data_trans_id)
                    self.record.add_suggest(
                        "the reason is transaction not ending (trans_id:{0}). You can get more info by 'obdiag rca run --scene=transaction_not_ending --input_parameters=\"{tx_id={0}}\"' or found {0} in the result.".format(self.data_trans_id_value)
                    )
            elif self.error_msg_type == "Lock wait timeout exceeded":
                # gather log about "mvcc_write conflict"
                work_path_mvcc_write_conflict = self.work_path + "/mvcc_write_conflict"
                self.gather_log.grep("mvcc_write conflict")
                logs_name = self.gather_log.execute(save_path=work_path_mvcc_write_conflict)
                if logs_name is None or len(logs_name) <= 0:
                    self.record.add_record("no log about 'mvcc_write conflict'")
                    self.record.add_suggest("no log about 'mvcc_write conflict'")
                    return
                else:
                    self.record.add_record("find 'mvcc_write conflict' success in {0}".format(work_path_mvcc_write_conflict))
                # find conflict_tx_id in log
                for log_name in logs_name:
                    with open(log_name, "r") as f:
                        lines = f.readlines()
                        for line in lines:
                            if "conflict_tx_id" in line:
                                self.conflict_tx_id = line
                                break
                if self.conflict_tx_id is None:
                    self.record.add_record("can not find 'conflict_tx_id' in log.")
                    self.record.add_suggest("can not find 'conflict_tx_id' in log.")
                    return False
                else:
                    self.record.add_record("find conflict_tx_id in {0}".format(self.conflict_tx_id))
                    match = re.search(r"conflict_tx_id=\{txid:(\d+)}", self.conflict_tx_id)
                    if match:
                        self.conflict_tx_id_value = match.group(1)
                    if self.conflict_tx_id_value is None:
                        self.record.add_record("can not find conflict_tx_id value in log.")
                        self.record.add_suggest("can not find conflict_tx_id value in log.")
                        return False
                    else:
                        self.record.add_record("find conflict_tx_id in {0}".format(self.conflict_tx_id_value))
                        self.record.add_suggest("A transaction that holds a row lock and does not end: (tx_id:{0}). Please check it.".format(self.conflict_tx_id_value))

        except Exception as e:
            raise RCAExecuteException("TransactionWaitTimeoutScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionWaitTimeoutScene execute")

    def get_scene_info(self):
        return {
            "name": "transaction_wait_timeout",
            "info_en": "transaction wait timeout error, error_msg like 'Shared lock conflict' or 'Lock wait timeout exceeded'",
            "info_cn": "事务等待超时报错",
        }


transaction_wait_timeout = TransactionWaitTimeoutScene()
