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
@file: replay_hold.py
@desc:
"""
import os

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils


class ReplayHoldScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.logs_name = None
        self.work_path = None
        self.index_table_id = None
        self.estimated_size = None
        self.estimated_data_size = None

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
        self.work_path = context.get_variable('store_dir')
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        self.record.add_record("start check the replay_hold scene")

    def verbose(self, info):
        self.stdio.verbose("[ReplayHoldScene] {0}".format(info))

    def execute(self):
        try:
            self.record.add_record("start check the replay_hold scene")
            replay_hold_data_sql = "select a.svr_ip, a.svr_port, a.tenant_id, a.ls_id, b.end_scn, a.unsubmitted_log_scn, a.pending_cnt from oceanbase.__all_virtual_replay_stat a join oceanbase.__all_virtual_log_stat b on a.svr_ip=b.svr_ip and a.svr_port=b.svr_port and a.tenant_id=b.tenant_id and a.ls_id = b.ls_id and a.role='FOLLOWER'"
            self.record.add_record("check the replay_hold. by sql: " + replay_hold_data_sql)
            replay_hold_data_start = self.__execute_sql_with_save(
                replay_hold_data_sql,
                "replay_hold_data_start",
            )
            need_check = False
            if replay_hold_data_start is None:
                self.record.add_record("replay_hold_data_start is empty. Need not check.")
                self.stdio.print("replay_hold_data_sql result is empty. Need not check.")
                return

            for row in replay_hold_data_start:
                if row["unsubmitted_log_scn"] < row["end_scn"]:
                    self.record.add_record("find unsubmitted_log_scn < end_scn. svr_ip: {0}, svr_port: {1}, tenant_id: {2}, ls_id: {3}".format(row["svr_ip"], row["svr_port"], row["tenant_id"], row["ls_id"]))
                    need_check = True
            if not need_check:
                self.record.add_record("no unsubmitted_log_scn < end_scn found.")
                self.record.add_suggest("no unsubmitted_log_scn < end_scn found.")
                return
            self.record.add_record("replay_hold_data_start save: {0}".format("replay_hold_data_start.txt"))
            # gather log
            self.gather_log.set_parameters("scope", "observer")
            log_path = os.path.join(self.work_path, "all_log")
            self.gather_log.execute()
            self.logs_name = self.gather_log.execute(save_path=log_path)
            self.record.add_record("gather log save: {0}".format(self.logs_name))
            # check "fatal error" in log
            if self.__check_start_port_in_log():
                self.record.add_suggest("find Unretrievable error. Please send {0} to the Oceanbase community.".format(self.work_path))
                return
            self.record.add_suggest("No Unretrievable error found. Please send {0} to the Oceanbase community.".format(self.work_path))
        except Exception as e:

            raise RCAExecuteException("ReplayHoldScene execute error: {0}".format(e))
        finally:
            self.record.add_suggest("Please send {0} to the Oceanbase community.".format(self.work_path))
            self.stdio.verbose("end ReplayHoldScene execute")

    def __check_start_port_in_log(self):
        if self.logs_name is None:
            return False
        for log in self.logs_name:
            with open(log, 'r') as f:
                for line in f:
                    if "fatal error" in line:
                        self.record.add_record("find 'fatal error' in log: {0}".format(log))
                        return True
        return False

    def __execute_sql_with_save(self, sql: str, save_file_name: str):
        try:
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            data = cursor.fetchall()
            self.verbose("the sql:{1} .data is {0}".format(data, sql))
            if len(data) <= 0:
                self.record.add_record("sql: {0} execute result is empty.".format(sql))
                return []
            columns = [desc[0] for desc in cursor.description]
            data_save_path = os.path.join(self.work_path, "{}.txt".format(save_file_name))
            with open(data_save_path, 'w') as f:
                f.write('\t'.join(columns) + '\n')
                for row in data:
                    line = ""
                    for item in row:
                        line += "{}\t".format(row[item])
                    f.write(line + '\n')
            return data
        except Exception as e:
            raise RCAExecuteException("ReplayHoldScene __execute_with_save execute error: {0}".format(e))

    def get_scene_info(self):
        return {"name": "replay_hold", "info_en": "[beta] replay hold", "info_cn": '[beta] 回放卡问题排查', "example": "obdiag rca run --scene=replay_hold"}


replay_hold = ReplayHoldScene()
