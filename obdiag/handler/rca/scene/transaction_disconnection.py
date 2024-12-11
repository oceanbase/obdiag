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
@time: 2024/05/28
@file: transaction_disconnection_scene.py
@desc:
"""
import os
import re

from obdiag.handler.rca.rca_exception import RCAInitException
from obdiag.handler.rca.rca_handler import RcaScene
from obdiag.common.tool import StringUtils


class TransactionDisconnectionScene(RcaScene):
    def __init__(self):
        super().__init__()
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

    def execute(self):
        # get the syslog_level
        syslog_level_data = self.ob_connector.execute_sql_return_cursor_dictionary(' SHOW PARAMETERS like "syslog_level"').fetchall()
        self.record.add_record("syslog_level data is {0}".format(syslog_level_data[0].get("value") or None))
        # gather log about "session is kill"
        work_path_session_killed_log = self.work_path + "/session_killed_log"
        self.gather_log.grep("session is kill")
        if self.input_parameters.get("since") is not None:
            since = self.input_parameters.get("since")
            self.gather_log.set_parameters("since", since)
        logs_name = self.gather_log.execute(save_path=work_path_session_killed_log)
        # get the session id on logfile
        if logs_name is None or len(logs_name) <= 0:
            self.record.add_record("no log about 'session is kill'")
            self.record.add_suggest("no log about 'session is kill'. please check the log file on {0}".format(work_path_session_killed_log))
            return False
        check_nu = 10
        sessid_list = []
        for log_name in logs_name:
            if check_nu == 0:
                break
            with open(log_name, "r") as f:
                lines = f.readlines()
                for line in lines:
                    if "session is kill" in line and "sessid_=" in line:
                        # get the session id on line
                        match = re.search(r'sessid_=(\d+)', line)
                        if match:
                            sessid = match.group(1)
                            check_nu = check_nu - 1
                            sessid_list.append(sessid)
                            if check_nu == 0:
                                break
        # gather log by sessid_list
        if len(sessid_list) == 0:
            self.record.add_record("no log about 'session is kill' to get session_id.")
            self.record.add_suggest("no log about 'session is kill' to get session_id. please check the log file on {0}".format(work_path_session_killed_log))
            return False
        else:
            self.record.add_record("the session id list is {0}".format(str(sessid_list)))
            for sessid in sessid_list:
                work_path_session_id = self.work_path + "/session_killed_log_{0}".format(sessid)
                self.gather_log.grep(sessid)
                if self.input_parameters.get("since") is not None:
                    since = self.input_parameters.get("since")
                    self.gather_log.set_parameters("since", since)
                self.gather_log.execute(save_path=work_path_session_id)
                self.record.add_record("the session id {0} has been gathered. the log save on {1}.".format(sessid, work_path_session_id))
            self.record.add_suggest("please check the log file on {0}. And send it to the oceanbase community to get more support.".format(work_path_session_killed_log))

    def get_scene_info(self):

        return {
            "name": "transaction_disconnection",
            "info_en": "root cause analysis of transaction disconnection",
            "info_cn": "针对事务断连场景的根因分析",
        }


transaction_disconnection = TransactionDisconnectionScene()
