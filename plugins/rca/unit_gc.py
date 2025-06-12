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
@file: unit_gc.py
@desc:
"""
import datetime
import os
import re

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils


class UnitGCScene(RcaScene):
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

    def verbose(self, info):
        self.stdio.verbose("[UnitGCScene] {0}".format(info))

    def execute(self):
        try:
            # get gv$ob_units save on work_path
            sql = "select * from oceanbase.gv$ob_units;"
            self.verbose("get gv$ob_units execute_sql is {0}".format(sql))
            cursor_gv_ob_units = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            gv_ob_units_data = cursor_gv_ob_units.fetchall()
            self.verbose("the gv_ob_units_data is {0}".format(gv_ob_units_data))
            if len(gv_ob_units_data) <= 0:
                self.record.add_record("Not find tenant_ids about unit_gc.")
                raise RCANotNeedExecuteException("Not find tenant_ids about unit_gc.")
            self.record.add_record("find tenant_ids about unit_gc. sql: {0}".format(sql))
            # save gv_ob_units_data > #{work_path}/gv_ob_units_data.txt
            columns = [desc[0] for desc in cursor_gv_ob_units.description]
            gv_ob_units_data_save_path = os.path.join(self.work_path, "gv_ob_units_data.txt")
            with open(gv_ob_units_data_save_path, 'w') as f:
                f.write('\t'.join(columns) + '\n')
                for row in gv_ob_units_data:
                    line = ""
                    for item in row:
                        line += "{}\t".format(row[item])
                    f.write(line + '\n')
            self.record.add_record("save gv_ob_units_data to {0}".format(gv_ob_units_data_save_path))
            # analyze gv_ob_units_data
            self.record.add_record("start analyze gv_ob_units_data")
            task_list = []
            for row in gv_ob_units_data:
                tenant_id = row['TENANT_ID']
                status = row['STATUS']
                if status == "NORMAL":
                    continue
                elif status == "WAIT GC":
                    task_list.append(tenant_id)
                else:
                    self.record.add_record("find tenant_id {0} about unit_gc. but the status is {1}".format(tenant_id, status))
            self.record.add_record("find tenant_ids about unit_gc. task_list: {0}".format(task_list))
            # gather log
            self.record.add_record("start gather log about unit_gc")
            if len(task_list) > 0:
                self.gather_log.set_parameters("scope", "observer")
                log_path = os.path.join(self.work_path, "all_log")
                if not os.path.exists(log_path):
                    os.makedirs(log_path)
                self.logs_name = self.gather_log.execute(save_path=log_path)
                if self.logs_name is None or len(self.logs_name) <= 0:
                    self.record.add_record("Not find log_disk_full about unit_gc.")
                    raise RCANotNeedExecuteException("Not find log_disk_full about unit_gc.")
                else:
                    self.record.add_record("gather log save in {0}".format(log_path))
            else:
                self.record.add_record("Not find tenant_ids about unit_gc.")
                return self.stdio.print("[RCANotNeedExecute] Not find tenant_ids about unit_gc.")
            # check "this ls is not safe to destroy" exit in log
            self.record.add_record("start check \"this ls is not safe to destroy\" exit in log")
            not_safe_to_destroy_log = self.__check_str_in_log("this ls is not safe to destroy")
            if not_safe_to_destroy_log:
                self.record.add_record("find \"this ls is not safe to destroy\" exit in log: {0}".format(not_safe_to_destroy_log))
                self.record.add_record("the type of problem is \"safe destroy\"")
                # 开始引用计数问题 todo
                # self.record.add_record("start analyze safe destroy problem")
                return
            self.record.add_record("the type of problem is \"gc mod problem\"")
            self.record.add_record("start transfer")
            # transfer_log: check "The ls is dependent and is not allowed to be GC" exit in log
            transfer_log = self.__check_str_in_log("The ls is dependent and is not allowed to be GC")
            if transfer_log:
                self.record.add_record("find \"The ls is dependent and is not allowed to be GC\" exit in log: {0}".format(transfer_log))
                # 存在transfer依赖
                self.record.add_record("the type of problem is \"transfer dependency exists\"")
                return
            else:
                self.record.add_record("Not find \"The ls is dependent and is not allowed to be GC\" exit in log")
            # only_read_log: check "need wait before readonly tx been cleaned up" in log
            only_read_log = self.__check_str_in_log("need wait before readonly tx been cleaned up")
            if only_read_log:
                self.record.add_record("find \"need wait before readonly tx been cleaned up\" exit in log: {0}".format(only_read_log))
                self.record.add_record("the type of problem is \"only read\"")
                return
            else:
                self.record.add_record("Not find \"need wait before readonly tx been cleaned up\" exit in log")
            # ls_waiting_compaction_log: check "need wait before readonly tx been cleaned up" in log
            ls_waiting_compaction_log = self.__check_str_in_log("need wait before readonly tx been cleaned up")
            if ls_waiting_compaction_log:
                self.record.add_record("find \"need wait before readonly tx been cleaned up\" exit in log: {0}".format(ls_waiting_log))
                self.record.add_record("the type of problem is \"ls waiting for comparison\"")
                return
            else:
                self.record.add_record("Not find \"The ls is waiting for comparison\" exit in log")
            # ls_waiting_transactions_log : check "offline tx service failed" in log
            ls_waiting_transactions_log = self.__check_str_in_log("offline tx service failed")
            if ls_waiting_transactions_log:
                self.record.add_record("find \"offline tx service failed\" exit in log: {0}".format(ls_waiting_transactions_log))
                self.record.add_record("the type of problem is \"ls waiting for transactions\"")
                return
            else:
                self.record.add_record("Not find \"offline tx service failed\" exit in log")
            # other_mod_log: check "this ls is not safe to destroy" exit in log
            other_mod_log = self.__check_str_in_log("ls offline failed")
            if other_mod_log:
                self.record.add_record("find \"ls offline failed\" exit in log: {0}".format(other_mod_log))
                self.record.add_record("the type of problem is \"other mod\"")
                return
            else:
                self.record.add_record("Not find \"ls offline failed\" exit in log")
            # ls_wait: check "ls is waiting for" exit in log
            ls_wait_log = self.__check_str_in_log("ls wait not finished.")
            if ls_wait_log:
                self.record.add_record("find \"ls wait not finished.\" exit in log: {0}".format(ls_wait_log))
                self.record.add_record("the type of problem is \"ls waiting for\"")
                return
            else:
                self.record.add_record("Not find \"ls wait not finished.\" exit in log")
            # resource_release: check "check_all_meta_mem_released" exit in log
            resource_release_log = self.__check_str_in_log("check_all_meta_mem_released")
            if resource_release_log:
                self.record.add_record("find \"check_all_meta_mem_released\" exit in log: {0}".format(resource_release_log))
                self.record.add_record("the type of problem is \"resource release\"")
                return
            else:
                self.record.add_record("Not find \"check_all_meta_mem_released\" exit in log")
            self.record.add_record("Not find problem type about unit_gc.")

        except Exception as e:
            raise RCAExecuteException("UnitGCScene execute error: {0}".format(e))
        finally:
            self.record.add_suggest("Please send {0} to the Oceanbase community.".format(self.work_path))
            self.stdio.verbose("end UnitGCScene execute")

    def __check_str_in_log(self, str):
        for log_name in self.logs_name:
            with open(log_name, "r", encoding="utf-8", errors="ignore") as f:
                file_data = f.read()
                if str in file_data:
                    return log_name
        return None

    def get_scene_info(self):
        return {"name": "unit_gc", "info_en": "[beta] unit gc 问题排查.", "info_cn": '[beta] clog日志磁盘空间满的问题', "example": "obdiag rca run --scene=unit_gc"}


unit_gc = UnitGCScene()
