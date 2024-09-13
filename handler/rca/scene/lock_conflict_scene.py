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
@time: 2024/07/29
@file: lock_conflict_scene.py
@desc:
"""
import json
from handler.rca.rca_exception import RCAInitException, RCANotNeedExecuteException
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import StringUtils, DateTimeEncoder


class LockConflictScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.tenant_id = None

    def init(self, context):
        try:
            super().init(context)
            self.local_path = context.get_variable("store_dir")
            if self.observer_version is None or len(self.observer_version.strip()) == 0 or self.observer_version == "":
                raise Exception("observer version is None. Please check the NODES conf.")
        except Exception as e:
            raise RCAInitException("LockConflictScene RCAInitException: ", e)

    def execute(self):
        if self.input_parameters is not None:
            tenant_name = self.input_parameters.get("tenant_name")
            tenant_data = self.ob_connector.execute_sql("select tenant_id from oceanbase.__all_tenant where tenant_name = '{0}';".format(tenant_name))
            if len(tenant_data) == 0:
                raise RCAInitException("can not find tenant id by tenant name: {0}. Please check the tenant name.".format(tenant_name))
            self.tenant_id = tenant_data[0][0]
            self.verbose("tenant_id is {0}".format(self.tenant_id))
        if self.observer_version == "4.2.0.0" or StringUtils.compare_versions_greater(self.observer_version, "4.2.0.0"):
            self.__execute_4_2()
        elif StringUtils.compare_versions_greater("4.2.0.0", self.observer_version):
            self.__execute_old()
        else:
            raise Exception("observer version is {0}. Not support".format(self.observer_version))

    def verbose(self, info):
        self.stdio.verbose("[lock_conflict] {0}".format(info))

    def __execute_4_2(self):
        first_record = RCA_ResultRecord()
        # get trans_id
        locks_sql = 'select * from oceanbase.GV$OB_LOCKS where BLOCK=1 and TYPE="TX" '
        if self.tenant_id is not None:
            locks_sql = locks_sql + 'and tenant_id={0}'.format(self.tenant_id)
        locks_sql = locks_sql + '' + ' limit 100;'
        self.verbose("locks_sql is{0}".format(locks_sql))
        cursor = self.ob_connector.execute_sql_return_cursor_dictionary(locks_sql)
        data = cursor.fetchall()
        if len(data) == 0:
            first_record.add_record("on GV$OB_LOCKS result is null")
            first_record.add_suggest("No block lock found. Not Need Execute")
            self.Result.records.append(first_record)
            raise RCANotNeedExecuteException("No block lock found.")
        # first_record.add_record("by select * from oceanbase.GV$OB_LOCKS where BLOCK=1; the len is {0}".format(len(data)))
        first_record.add_record("by locks_sql; the len is {0}".format(len(data)))
        for OB_LOCKS_data in data:
            trans_record = RCA_ResultRecord()
            first_record_records = first_record.records.copy()
            trans_record.records.extend(first_record_records)
            self.Result.records.append(trans_record)
            try:
                if OB_LOCKS_data.get("ID1") is None:  # Holding lock session id
                    trans_record.add_record("Holding lock trans_id is null")
                    trans_record.add_suggest("Holding lock trans_id is null. can not do next")
                    continue
                else:
                    trans_id = OB_LOCKS_data["ID1"]
                    trans_record.add_record("get holding_lock trans_id:{0}".format(trans_id))
                    holding_lock_session_id = trans_id
                    self.stdio.verbose("get holding lock SESSION_ID by trans_id:{0}".format(trans_id))
                    cursor_by_trans_id = self.ob_connector.execute_sql_return_cursor_dictionary('select * from oceanbase.GV$OB_TRANSACTION_PARTICIPANTS where TX_ID="{0}";'.format(holding_lock_session_id))
                    holding_lock_session_id_datas = cursor_by_trans_id.fetchall()
                    holding_lock_session_id = "not get"
                    self.stdio.verbose("get sql_info by holding_lock_session_id:{0}".format(holding_lock_session_id_datas))
                    if len(holding_lock_session_id_datas) > 0:
                        holding_lock_session_id = holding_lock_session_id_datas[0].get("SESSION_ID")
                    else:
                        trans_record.add_record("holding_lock_session_id is {0}".format(holding_lock_session_id_datas))
                        trans_record.add_suggest("holding_lock_session_id is null. maybe the session is closed")
                        continue
                    trans_record.add_record("get holding_lock_session_id:{0}".format(holding_lock_session_id))

                    wait_lock_trans_id = OB_LOCKS_data["TRANS_ID"]
                    trans_record.add_record("wait_lock_trans_id is {0}".format(wait_lock_trans_id))
                    cursor_by_trans_id = self.ob_connector.execute_sql_return_cursor_dictionary('select * from oceanbase.GV$OB_TRANSACTION_PARTICIPANTS where TX_ID="{0}";'.format(wait_lock_trans_id))

                    wait_lock_session_datas = cursor_by_trans_id.fetchall()
                    self.stdio.verbose("get sql_info by holding_lock_session_id:{0}".format(holding_lock_session_id))
                    wait_lock_session_id = "not get"
                    if len(wait_lock_session_datas) == 0:
                        trans_record.add_record("wait_lock_session_id is null")
                        trans_record.add_suggest("wait_lock_session_id is null. maybe the session is closed, you can kill holding_lock_session_id: {0}".format(holding_lock_session_id))
                        continue

                    wait_lock_session_id = wait_lock_session_datas[0].get("SESSION_ID")
                    trans_record.add_record("get wait_lock_session_id:{0}".format(wait_lock_session_datas[0].get("SESSION_ID")))
                    self.stdio.verbose("get sql_info by holding_lock_session_id:{0}".format(holding_lock_session_id))
                    # check SQL_AUDIT switch
                    sql_info = "not find"

                    cursor_check_switch = self.ob_connector.execute_sql_return_cursor_dictionary("SHOW PARAMETERS LIKE '%enable_sql_audit%';")
                    audit_switch_value = cursor_check_switch.fetchone().get("value")
                    if audit_switch_value.strip().upper() == "TRUE":
                        holding_lock_sql_info_cursor = self.ob_connector.execute_sql_return_cursor_dictionary('SELECT * FROM oceanbase.gv$OB_SQL_AUDIT where tx_id!=0 and SID="{0}";'.format(holding_lock_session_id))
                        trans_record.add_record('exec sql: SELECT * FROM oceanbase.gv$OB_SQL_AUDIT where SID="{0}"; to get holding_lock_sql_info.'.format(holding_lock_session_id))
                        holding_lock_sql_info = holding_lock_sql_info_cursor.fetchall()
                        if len(holding_lock_sql_info) == 0:
                            trans_record.add_record("holding_lock_session_id: {0}; not find sql_info on gv$OB_SQL_AUDIT".format(holding_lock_session_id))
                        else:
                            holding_lock_sql_info_json_data = json.dumps(holding_lock_sql_info, cls=DateTimeEncoder)
                            file_name = "{0}/rca_holding_lock_sql_info_{1}.json".format(self.local_path, holding_lock_session_id)
                            with open(file_name, "w+") as f:
                                f.write(str(holding_lock_sql_info_json_data))
                            trans_record.add_record("holding_lock_session_id: {0}. holding_lock_sql_info save on {1}".format(holding_lock_session_id, file_name))
                            sql_info = "save on {0}".format(file_name)
                    else:
                        self.stdio.verbose("SQL_AUDIT switch is False")
                        trans_record.add_record("SQL_AUDIT switch is False. can't get sql_info")
                    trans_record.add_suggest(
                        "holding_lock_session_id: {0}; wait_lock_session_id : {1}, sql_info: {2}. Lock conflicts can be ended by killing holding_lock_session_id or wait_lock_session_id".format(holding_lock_session_id, wait_lock_session_id, sql_info)
                    )
            except Exception as e:
                trans_record.add_record("get SESSION_ID panic. OB_LOCKS_data:{0} error: {1}".format(OB_LOCKS_data, e))
                trans_record.add_suggest("get SESSION_ID panic. OB_LOCKS_data:{0} error: {1}".format(OB_LOCKS_data, e))

        return

    def __execute_old(self):
        first_record = RCA_ResultRecord()
        cursor = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.__all_virtual_lock_wait_stat order by try_lock_times limit 50;")
        virtual_lock_wait_stat_datas = cursor.fetchall()
        if len(virtual_lock_wait_stat_datas) == 0:
            first_record.add_record("on __all_virtual_trans_stat result is null")
            first_record.add_suggest("No block lock found. Not Need Execute")
            self.Result.records.append(first_record)
            raise RCANotNeedExecuteException("No block lock found.")
        first_record.add_record("by select * from oceanbase.__all_virtual_lock_wait_stat order by try_lock_times limit 50; the len is {0}".format(len(virtual_lock_wait_stat_datas)))

        for trans_lock_data in virtual_lock_wait_stat_datas:
            trans_id = trans_lock_data["block_session_id"]
            trans_record = RCA_ResultRecord()
            first_record_records = first_record.records.copy()
            trans_record.records.extend(first_record_records)
            self.Result.records.append(trans_record)
            trans_record.add_record("block_data is {0}".format(trans_lock_data))
            trans_record.add_record("block_session_id is {0}".format(trans_id))
            trans_record.add_suggest(
                "Sessions corresponding to lock transactions. The ID is {0}, "
                "which may be a lock conflict issue.You can be accessed through kill "
                "session_Roll back the corresponding transaction with ID. Please "
                "note that this will result in corresponding transaction regression! "
                "".format(trans_lock_data.get("block_session_id"))
            )

        return

    def get_scene_info(self):
        return {
            "name": "lock_conflict",
            "info_en": "root cause analysis of lock conflict",
            "info_cn": "针对锁冲突的根因分析",
        }


lock_conflict = LockConflictScene()
