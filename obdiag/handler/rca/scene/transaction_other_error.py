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
@file: transaction_other_error_scene.py
@desc:
"""
import os

from obdiag.handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
    RCANotNeedExecuteException,
)
from obdiag.handler.rca.rca_handler import RcaScene
from obdiag.common.tool import StringUtils


class TransactionOtherErrorScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.error_code = None
        self.error_msg_type = None
        self.error_msg = None
        self.trans_is_killed_log = None
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
        self.error_code = self.input_parameters.get("error_code")
        if not self.error_code:
            raise RCANotNeedExecuteException("error_code is empty")
        if self.error_code != "-4030" and self.error_code != "-4121" and self.error_code != "-4122" and self.error_code != "-4124" and self.error_code != "-4019":
            raise RCANotNeedExecuteException("error_code is not -4030，-4121，-4122，-4124，-4019")
        if self.error_code == "-4030":
            self.error_msg_type = "RPC send error"
        elif self.error_code == "-4121":
            self.error_msg_type = "RPC post error"
        elif self.error_code == "-4122":
            self.error_msg_type = "Connect error"
        elif self.error_code == "-4124":
            self.error_msg_type = "Connect error"

    def verbose(self, info):
        self.stdio.verbose("[TransactionOtherErrorScene] {0}".format(info))

    def execute(self):
        try:
            syslog_level_data = self.ob_connector.execute_sql_return_cursor_dictionary('SHOW PARAMETERS like "syslog_level"').fetchall()
            self.record.add_record("syslog_level data is {0}".format(syslog_level_data[0].get("value") or None))
            # The three types of error codes, RPC send error, RPC post error, and Connect error, are most likely caused by RPC issues
            if self.error_code == "-4021" or self.error_code == "-4122" or self.error_code == "-4124":
                self.record.add_record("error_code is {0}, error_msg_type is {1}")
                self.record.add_suggest("It is highly likely that there is an issue with the RPC and we need to use 'tsar' to check the network conditions in the environment at that time. And send it to Oceanbase Community")
                return
            if self.error_code == "-4030":
                self.record.add_record("error_code is {0}, error_msg_type is {1}".format(self.error_code, self.error_msg_type))
                self.record.add_suggest("Tenant out of memory, please expand")
                return
            # gather log about EASY SLOW
            work_path_EASY_SLOW = self.work_path + "/EASY_SLOW"
            self.gather_log.grep("EASY SLOW")
            logs_name = self.gather_log.execute(save_path=work_path_EASY_SLOW)
            if logs_name is None or len(logs_name) <= 0:
                self.record.add_record("no EASY SLOW log found")
                self.record.add_suggest("no EASY SLOW log found")
                return
            EASY_SLOW_nu = 0
            for log_name in logs_name:
                log_content = open(os.path.join(work_path_EASY_SLOW, log_name), "r").readlines()
                EASY_SLOW_nu += len(log_content)
                if EASY_SLOW_nu >= 1000:
                    self.record.add_record("EASY SLOW log number over 1000".format(EASY_SLOW_nu))
                    self.record.add_suggest("EASY SLOW log number over 1000, There is an issue with network latency")
                    break
            self.record.add_record("not found the reason. Please  send it to Oceanbase Community")

        except Exception as e:
            raise RCAExecuteException("TransactionOtherErrorScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionOtherErrorScene execute")

    def get_scene_info(self):
        return {
            "name": "transaction_other_error",
            "info_en": "transaction other error, error_code like -4030，-4121，-4122，-4124，-4019",
            "info_cn": "事务其他错误，除了目前已经列出的错误，比如错误码为：-4030，-4121，-4122，-4124，-4019",
        }


transaction_other_error = TransactionOtherErrorScene()
