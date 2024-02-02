# !/usr/bin/env python
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
@time: 2023/12/29
@file: lock_conflict_scene.py
@desc:
"""
from common.command import get_observer_version
from common.logger import logger
from common.ob_connector import OBConnector
from handler.rca.rca_exception import RCAInitException, RCANotNeedExecuteException
from handler.rca.rca_scene.scene_base import scene_base, Result, RCA_ResultRecord
from utils.shell_utils import SshHelper
from utils.version_utils import compare_versions_greater


class LockConflictScene(scene_base):
    def __init__(self):
        super().__init__()
        self.ob_connector = None
        self.observer_nodes = None
        self.ob_cluster = None
        self.observer_version = None
        self.default_node = None

    def init(self, cluster, nodes, obproxy_nodes, env, result_path):
        try:
            super().init(cluster, nodes, obproxy_nodes, env, result_path)
            self.default_node = self.observer_nodes[0]

            ssh = SshHelper(True, self.default_node.get("ip"),
                            self.default_node.get("user"),
                            self.default_node.get("password"),
                            self.default_node.get("port"),
                            self.default_node.get("private_key"),
                            self.default_node)
            self.observer_version = get_observer_version(True, ssh, self.default_node["home_path"])

            self.ob_connector = OBConnector(ip=self.ob_cluster.get("db_host"),
                                            port=self.ob_cluster.get("db_port"),
                                            username=self.ob_cluster.get("tenant_sys").get("user"),
                                            password=self.ob_cluster.get("tenant_sys").get("password"),
                                            timeout=10000)

        except Exception as e:
            raise RCAInitException("LockConflictScene RCAInitException: ", e)

    def execute(self):
        if self.observer_version is None or len(self.observer_version) == 0:
            raise Exception("observer version is None. Please check the NODES conf.")
        if self.observer_version == "4.2.0.0" or compare_versions_greater(self.observer_version, "4.2.0.0"):
            self.__execute_4_2()
        elif compare_versions_greater("4.2.2.0", self.observer_version):
            self.__execute_old()
        else:
            raise Exception("observer version is {0}. Not support".format(self.observer_version))

    def __execute_4_2(self):
        first_record = RCA_ResultRecord()
        # get trans_id
        cursor = self.ob_connector.execute_sql_return_cursor_dictionary(
            'select * from oceanbase.GV$OB_LOCKS where BLOCK=1 and TYPE="TX" limit 50;')
        data = cursor.fetchall()
        if len(data) == 0:
            first_record.add_record("on GV$OB_LOCKS result is null")
            first_record.add_suggest("No block lock found. Not Need Execute")
            self.Result.records.append(first_record)
            raise RCANotNeedExecuteException("No block lock found.")
        first_record.add_record("by select * from oceanbase.GV$OB_LOCKS where BLOCK=1; the len is {0}".format(len(data)))
        for OB_LOCKS_data in data:
            trans_record = RCA_ResultRecord()
            first_record_records = first_record.records.copy()
            trans_record.records.extend(first_record_records)
            self.Result.records.append(trans_record)
            try:
                if OB_LOCKS_data.get('TRANS_ID') is None:
                    trans_record.add_record("trans_id is null")
                    trans_record.add_suggest("trans_id is null. can not do next")
                    continue
                else:
                    trans_id = OB_LOCKS_data['TRANS_ID']
                    trans_record.add_record("trans_id is {0}".format(trans_id))
                    cursor_by_trans_id = self.ob_connector.execute_sql_return_cursor_dictionary(
                        'select * from oceanbase.V$OB_TRANSACTION_PARTICIPANTS where TX_ID="{0}";'.format(trans_id))
                    session_datas = cursor_by_trans_id.fetchall()
                    trans_record.add_record(
                        "get SESSION_ID by trans_id:{0}. get data:{0}".format(trans_id, session_datas))
                    if len(session_datas) != 1:
                        trans_record.add_suggest(
                            "get SESSION_ID by trans_id:{0}. Maybe the lock is not exist".format(trans_id))
                        continue
                    if session_datas[0].get("SESSION_ID") is not None:
                        trans_record.add_record("get SESSION_ID:{0}".format(session_datas[0].get("SESSION_ID")))
                        trans_record.add_suggest("Sessions corresponding to lock transactions. The ID is {0}, "
                                                 "which may be a lock conflict issue.You can be accessed through kill "
                                                 "session_ Roll back the corresponding transaction with ID. Please "
                                                 "note that this will result in corresponding transaction regression! "
                                                 "".format(session_datas[0].get("SESSION_ID")))

            except Exception as e:
                trans_record.add_record("get SESSION_ID panic. OB_LOCKS_data:{0} error: {1}".format(OB_LOCKS_data, e))
                trans_record.add_suggest("get SESSION_ID panic. OB_LOCKS_data:{0} error: {1}".format(OB_LOCKS_data, e))

        return

    def __execute_old(self):
        first_record = RCA_ResultRecord()
        cursor = self.ob_connector.execute_sql_return_cursor_dictionary(
            "select * from oceanbase.__all_virtual_lock_wait_stat order by try_lock_times limit 50;")
        virtual_lock_wait_stat_datas = cursor.fetchall()
        if len(virtual_lock_wait_stat_datas) == 0:
            first_record.add_record("on __all_virtual_trans_stat result is null")
            first_record.add_suggest("No block lock found. Not Need Execute")
            self.Result.records.append(first_record)
            raise RCANotNeedExecuteException("No block lock found.")
        first_record.add_record(
            "by select * from oceanbase.__all_virtual_lock_wait_stat order by try_lock_times limit 50; the len is {0}".format(
                len(virtual_lock_wait_stat_datas)))

        for trans_lock_data in virtual_lock_wait_stat_datas:

            trans_id = trans_lock_data["block_session_id"]
            trans_record = RCA_ResultRecord()
            first_record_records = first_record.records.copy()
            trans_record.records.extend(first_record_records)
            self.Result.records.append(trans_record)
            trans_record.add_record("block_data is {0}".format(trans_lock_data))
            trans_record.add_record("block_session_id is {0}".format(trans_id))
            trans_record.add_suggest("Sessions corresponding to lock transactions. The ID is {0}, "
                                     "which may be a lock conflict issue.You can be accessed through kill "
                                     "session_Roll back the corresponding transaction with ID. Please "
                                     "note that this will result in corresponding transaction regression! "
                                     "".format(trans_lock_data.get("block_session_id")))

        return

    def export_result(self):
        return self.Result.export()