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
@file: memory_full.py
@desc:
"""
import os

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils


class MemoryFullScene(RcaScene):
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
        self.record.add_record("start check the memory_full scene")

    def verbose(self, info):
        self.stdio.verbose("[MemoryFullScene] {0}".format(info))

    def execute(self):
        try:
            # gather log
            self.gather_log.set_parameters("scope", "observer")
            log_path = os.path.join(self.work_path, "all_log")
            if not os.path.exists(log_path):
                os.makedirs(log_path)
            if self.ob_connector:
                # save desc __all_virtual_memory_info;
                self.record.add_record("ob_connector is exist, use sql to save __all_virtual_memory_info.")
                self.__execute_sql_with_save("desc oceanbase.__all_virtual_memory_info;", "virtual_memory_info")
            self.logs_name = self.gather_log.execute(save_path=log_path)
            if self.logs_name is None or len(self.logs_name) <= 0:
                self.record.add_record("Not find log.")
                raise RCANotNeedExecuteException("Not find log.")
            else:
                self.record.add_record("gather log save in {0}".format(log_path))
            if self.__check_start_port_in_log():
                self.record.add_record("find error log in log.")
            else:
                self.record.add_record("Not find error log in log.")
                return False
            if self.ob_connector:
                # When connection is available, use virtual tables for troubleshooting
                self.record.add_record("ob_connector is exist, use virtual table to check memory_full scene.")
                minor_freeze_info_data = self.__execute_sql_with_save("select * from oceanbase.__all_virtual_minor_freeze_info;", "minor_freeze_info")
                if minor_freeze_info_data is None or len(minor_freeze_info_data) <= 0:
                    self.record.add_record("Not find memstore stat in virtual table.")
                else:
                    for row in minor_freeze_info_data:
                        state = row["state"]
                        svr_ip = row['svr_ip']
                        svr_port = row['svr_port']
                        tenant_id = row['tenant_id']
                        # If freeze state is WAIT_READY_FOR_FLUSH or FINISH, need to confirm memtable state
                        if state == "WAIT_READY_FOR_FLUSH" or state == "FINISH":
                            self.record.add_record("the state is {0}, need check memtable state.".format(state))
                            self.record.add_record("the svr_ip is {0}, svr_port is {1}, tenant_id is {2}. state is {3}".format(svr_ip, svr_port, tenant_id, state))
                            memstore_info_datas = self.__execute_sql_with_save(
                                'select * from oceanbase.__all_virtual_memstore_info where svr_ip ={0} and svr_port ={1} and tenant_id ={2} and is_active = "NO" order by start_scn limit 3;'.format(svr_ip, svr_ip, tenant_id),
                                "all_virtual_memstore_info_{0}_{1}_{2}".format(svr_ip, svr_port, tenant_id),
                            )
                            for memstore_info_row in memstore_info_datas:
                                # Confirm freeze_state status
                                # NOT_READY_FOR_FLUSH: means dump conditions are not met
                                # READY_FOR_FLUSH: means dump conditions are met but dump not started or not completed
                                # RELEASED: means dump succeeded and memtable released from memtable_mgr, but reference count not cleared so memtable cannot be destroyed
                                freeze_state = memstore_info_row["freeze_state"]
                                if freeze_state == "NOT_READY_FOR_FLUSH":
                                    self.record.add_record("the freeze_state is {0}, memtable not ready for flush.".format(freeze_state))
                                    unsubmitted_count = int(memstore_info_row["unsubmitted_count"])
                                    unsynced_count = int(memstore_info_row["unsynced_count"])
                                    write_ref_count = int(memstore_info_row["write_ref_count"])
                                    self.record.add_record("unsubmitted_count is {0}, unsynced_count is {1}, write_ref_count is {2}".format(unsubmitted_count, unsynced_count, write_ref_count))
                                    if unsubmitted_count == unsynced_count and unsubmitted_count != 0:
                                        self.record.add_record("unsubmitted_count == unsynced_count, but not 0.")
                                        # Likely related to multi-source data
                                        self.record.add_suggest("may be multi source data, please check the table is multi source or not.")
                                        return
                                    elif unsubmitted_count == 0 and write_ref_count != 0 and unsynced_count != 0:
                                        self.record.add_record("unsubmitted_count == 0, but write_ref_count != 0 and unsynced_count != 0.")
                                        # There are logs without callbacks
                                        self.record.add_suggest("there are logs without callbacks.")
                                        return
                                    elif unsubmitted_count == 0 and unsynced_count == 0 and write_ref_count == 0:
                                        self.record.add_record("unsubmitted_count == 0, unsynced_count == 0.")
                                        # Continuous max replay/callback position has not pushed over the memtable right boundary
                                        self.record.add_suggest("continuous max replay/callback position has not pushed over the memtable right boundary. The issue about 'ReplayService")
                                        return
                                elif freeze_state == "READY_FOR_FLUSH":
                                    self.record.add_record("the freeze_state is {0}, memtable meets flush conditions but flush not started or not completed.".format(freeze_state))
                                    self.record.add_record("need check compaction and dag info.")
                                    # Need to confirm dump status
                                    self.__execute_sql_with_save("select * from oceanbase.__all_virtual_compaction_diagnose_info;", "all_virtual_compaction_diagnose_info")
                                    # Confirm dump queue backlog
                                    self.__execute_sql_with_save("select * from oceanbase.__all_virtual_dag;", "all_virtual_dag")
                                elif freeze_state == "RELEASED":
                                    self.record.add_record(
                                        "the freeze_state is {0}, Represents successful dump and release of memtable from memtable_magr, but the reference count is not clear yet, resulting in memtable not being destroyed.".format(freeze_state)
                                    )
            else:
                # TODO When connection is not available, use logs for troubleshooting
                self.record.add_record("ob_connector is not exist, use log to check memory_full scene.")
                self.record.add_record("Not support this scene yet. we will support it soon.")

        except Exception as e:
            raise RCAExecuteException("MemoryFullScene execute error: {0}".format(e))
        finally:
            self.record.add_suggest("Please send {0} to the Oceanbase community.".format(self.work_path))
            self.stdio.verbose("end MemoryFullScene execute")

    def __check_start_port_in_log(self):
        check_flag = False
        for log_name in self.logs_name:
            tag_map = {"failed to create memtable": False, "cannot create more memtable": False, "ret=-4263": False, "ret=-4013": False, "memtable not ready for flush for long time": False}
            with open(log_name, "r", encoding="utf-8", errors="ignore") as f:
                file_data = f.read()
                for tag in tag_map:
                    if tag in file_data:
                        tag_map[tag] = True
            for tag in tag_map:
                if tag_map[tag]:
                    self.record.add_record("find {0} in log.".format(tag))
                    check_flag = True
                    break
        if check_flag:
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
            raise RCAExecuteException("MemoryFullScene __execute_with_save execute error: {0}".format(e))

    def get_scene_info(self):
        return {"name": "memory_full", "info_en": "[beta] memory full. e.g. error_code_4013 .", "info_cn": '[beta] 内存爆问题排查', "example": "obdiag rca run --scene=memory_full"}


memory_full = MemoryFullScene()
