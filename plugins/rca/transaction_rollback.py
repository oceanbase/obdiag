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
@desc:
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
            # gather log about "trans is killed".
            work_path_trans_is_killed = self.work_path + "/trans_is_killed"
            self.gather_log.grep("trans is killed")
            logs_name = self.gather_log.execute(save_path=work_path_trans_is_killed)
            if logs_name is None or len(logs_name) <= 0:
                self.record.add_record("no log about 'trans is killed'")
                return False
            else:
                self.record.add_record("find trans is killed in {0}".format(work_path_trans_is_killed))
            for log_name in logs_name:
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "trans is killed" in line:
                            self.trans_is_killed_log = line
                            break
            if self.trans_is_killed_log is not None:
                self.record.add_record("find trans is killed in {0}".format(self.trans_is_killed_log))
                self.record.add_suggest("The transaction was killed by the leader change ")
                return False
            else:
                self.record.add_record("can not find trans is killed in {0}".format(logs_name))
                self.record.add_suggest("can not find trans is killed. ".format(logs_name))
                self.record.add_record("The transaction was killed by timeout")
            # gather log about switch to follower forcedly success, about leader revoke timeout
            work_path_switch_to_follower_forcedly_success = self.work_path + "/switch_to_follower_forcedly_success"
            self.gather_log.grep("switch to follower forcedly success")
            logs_name = self.gather_log.execute(save_path=work_path_switch_to_follower_forcedly_success)
            if logs_name is None or len(logs_name) <= 0:
                self.record.add_record("no log about 'switch to follower forcedly success'")
            else:
                self.record.add_record("find switch to follower forcedly success in {0}".format(work_path_switch_to_follower_forcedly_success))
                self.record.add_suggest("The transaction was killed due to the leader change. ")
                return
            # gather log about trans_expired_time, about timeout
            work_path_trans_expired_time = self.work_path + "/trans_expired_time"
            self.gather_log.grep("trans_expired_time")
            logs_name = self.gather_log.execute(save_path=work_path_trans_expired_time)
            if logs_name is None or len(logs_name) <= 0:
                self.record.add_record("no log about 'trans expired time'")
                return False
            else:
                self.record.add_record("find trans expired time in {0}".format(work_path_trans_expired_time))
            for log in logs_name:
                with open(log, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "trans_expired_time" in line:
                            match = re.search(r'trans_expired_time:(\d+)', line)
                            if match:
                                date_str = match.group(1)
                                date_int = int(date_str) / 1e9
                                date_obj = datetime.datetime.utcfromtimestamp(date_int)
                                now = datetime.datetime.now()
                                if date_obj < now:
                                    self.record.add_record("trans_expired_time< The time point at which the transaction was killed.")
                                    self.record.add_suggest("Transaction killed due to transaction timeout. Please check the configuration of the transaction timeout.")

            if self.record.suggest_is_empty():
                self.record.add_suggest("not found the reason.Please check the transaction_rollback error is exist or contact the Oceanbase community.")
        except Exception as e:
            raise RCAExecuteException("TransactionRollbackScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionRollbackScene execute")

    def get_scene_info(self):
        return {
            "name": "transaction_rollback",
            "info_en": "transaction rollback error. error_code like -6002",
            "info_cn": "事务回滚报错",
        }


transaction_rollback = TransactionRollbackScene()
