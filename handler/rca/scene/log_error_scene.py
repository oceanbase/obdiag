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
@time: 2024/04/16
@file: log_error_scene.py
@desc:
"""
import os
import re

from handler.rca.rca_exception import RCAInitException, RCAExecuteException
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import StringUtils


class LogErrorScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.all_tenant_election_leader_info = None
        self.work_path = None
        self.all_tenant_ids = None

    def init(self, context):
        super().init(context)
        ## observer version≥4.0.0.0
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            self.stdio.error("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
            raise RCAInitException("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
        self.verbose("observer version is {0}.".format(observer_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")

    def verbose(self, info):
        self.stdio.verbose("[NoLeaderScene] {0}".format(info))

    def execute(self):
        try:
            if self.observer_version >= '4.2.1.0':
                self.execute_421()
                return
            # check Election leader
            # get_all_tenant_id
            self.verbose("start to get all tenant id...")
            sql = "select tenant_id from oceanbase.__all_tenant;"
            tenant_ids = self.ob_connector.execute_sql(sql)
            if len(tenant_ids) <= 0:
                raise RCAExecuteException("can not find any tenant id")
            self.all_tenant_election_leader_info = {}
            for tenant_id_data in tenant_ids:
                record = RCA_ResultRecord()
                try:
                    tenant_id_data = tenant_id_data[0]

                    record.add_record("tenant_id:{0}.".format(tenant_id_data))
                    self.execute_by_tenant_id(tenant_id_data, record)
                except Exception as e:
                    self.verbose("check election leader error,tenant_id:{0},error:{1}".format(tenant_id_data, e))
                    continue
                finally:
                    if len(record.suggest) == 13:
                        record.add_suggest("no suggest")
                    self.Result.records.append(record)
        except Exception as e:
            self.stdio.error("NoLeaderScene execute Exception:{0}".format(e))

    def execute_by_tenant_id(self, tenant_id, record):
        try:
            record.add_record("start step1")
            election_leader_info = self.check_election_leader_by_tenant_id(tenant_id)
            self.verbose("election_leader_info:{0}".format(election_leader_info))
            record.add_record("election_leader_info:{0}".format(election_leader_info))
            if election_leader_info == "":
                self.verbose("can not find any election leader,tenant_id:{0}".format(tenant_id))
                record.add_record("election_leader_info is null")
                record.add_suggest("can not find any election leader,tenant_id:{0}. Please check it.".format(tenant_id))
                return
            record.add_record("start step2")
            step_next_tag = True
            ls_ids = self.ob_connector.execute_sql("select distinct (ls_id) from oceanbase.__all_virtual_log_stat where tenant_id={0};".format(tenant_id))
            if ls_ids is None or len(ls_ids) <= 0:
                self.stdio.warn("not found log about election_leader. tenant_id: {0}".format(tenant_id))
                record.add_suggest("not found log on oceanbase.__all_virtual_log_stat. tenant_id: {0}".format(tenant_id))
                return

            for ls_id in ls_ids:
                ls_id = ls_id[0]
                leader_ls_id_bool = self.ob_connector.execute_sql('select count(0) from oceanbase.__all_virtual_log_stat where role="LEADER" and tenant_id={0} and ls_id="{1}";'.format(tenant_id, ls_id))
                leader_ls_id_bool = leader_ls_id_bool[0]
                if leader_ls_id_bool <= 0:
                    record.add_record("tenant_id: {0}, ls_id: {1} on oceanbase.__all_virtual_log_stat no LEADER".format(tenant_id, ls_id))
                    record.add_suggest("tenant_id: {0}, ls_id: {1} on oceanbase.__all_virtual_log_stat no LEADER".format(tenant_id, ls_id))
                    self.stdio.warn("tenant_id: {0}, ls_id: {1} on oceanbase.__all_virtual_log_stat no LEADER".format(tenant_id, ls_id))
                    step_next_tag = False

            if step_next_tag is False:
                self.verbose("step_next_tag is false")
                return
            return

        except Exception as e:
            self.stdio.warn("execute_by_tenant_id:{0} Exception:{1}".format(tenant_id, e))

    def execute_421(self):
        try:
            self.stdio.print("start execute_421")
            if self.ob_connector is None:
                self.stdio.error("ob_connector is None. please check conf")
                return
            # get data from __all_virtual_ha_diagnose
            sql = "select * from oceanbase.__all_virtual_ha_diagnose;"
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            diagnose_data = cursor.fetchall()
            if diagnose_data is None or len(diagnose_data) <= 0:
                self.stdio.warn("not found data on oceanbase.__all_virtual_ha_diagnose")
                return
            # get all tenant_id
            tenant_ids = []
            for data in diagnose_data:
                tenant_ids.append(data["tenant_id"])
            self.verbose("tenant_ids:{0}".format(tenant_ids))
            # step1
            ### tenant_diagnose_data:   [tenant_id] diagnose_data
            tenant_diagnose_data = {}
            for data in diagnose_data:
                if tenant_diagnose_data.get(data["tenant_id"]) is None:
                    tenant_diagnose_data[data["tenant_id"]] = [data]
                else:
                    tenant_data = tenant_diagnose_data.get(data["tenant_id"])
                    tenant_data.append(data)
                    tenant_diagnose_data[data["tenant_id"]] = tenant_data
            self.verbose("tenant_diagnose_data:{0}".format(tenant_diagnose_data))
            self.stdio.start_loading("no_leader scene start analyzing...")
            for tenant_id in tenant_diagnose_data:
                record_one_tenant = self.execute_421_no_leader_by_tenant_id(tenant_id, tenant_diagnose_data[tenant_id])
                self.Result.records.append(record_one_tenant)
            self.stdio.stop_loading('no_leader scene end')
            return

        except Exception as e:
            raise RCAExecuteException("execute_421 execute error: {0}".format(e))

    def execute_421_no_leader_by_tenant_id(self, tenant_id, diagnose_data):
        record = RCA_ResultRecord()
        try:
            self.stdio.verbose("start execute_421_no_leader_by_tenant_id")
            record.add_record("tenant_id: {0}.".format(tenant_id))
            leader_nu = {}
            record.add_record("start step1")
            for diagnose_data_by_tenant_id in diagnose_data:
                if diagnose_data_by_tenant_id["election_role"].upper() == "LEADER":
                    leader_nu[diagnose_data_by_tenant_id["ls_id"]] = leader_nu.get(diagnose_data_by_tenant_id["ls_id"], 0) + 1
                else:
                    leader_nu[diagnose_data_by_tenant_id["ls_id"]] = leader_nu.get(diagnose_data_by_tenant_id["ls_id"], 0)
            record.add_record("all ls_id:{0}".format(list(leader_nu.keys())))
            self.verbose("all ls_id:{0}".format(list(leader_nu.keys())))
            scene_1_tag = True
            for ls_id in leader_nu:
                record.add_record("on ls_id: {1} ".format(tenant_id, ls_id))
                self.verbose("on tenant_id: {0}, ls_id: {1} ".format(tenant_id, ls_id))
                if leader_nu[ls_id] > 1:
                    self.stdio.warn("the leader number > 1")
                    record.add_record("the ls_id's leader number > 1")
                    record.add_suggest("tenant_id: {0}, ls_id: {1} .the ls_id's leader number > 1".format(tenant_id, ls_id))
                    scene_1_tag = False
                    continue
                elif leader_nu[ls_id] == 0:
                    self.stdio.warn(
                        "the leader number = 0,The election layer is unable to select a new owner, and a common problem in this scenario is that the message delay is too large. You can continue to troubleshoot the problem of message delay or backlog in the log"
                    )
                    record.add_suggest(
                        "tenant_id: {0}, ls_id: {1} .the leader number = 0. The election layer is unable to select a new owner, and a common problem in this scenario is that the message delay is too large. You can continue to troubleshoot the problem of message delay or backlog in the log".format(
                            tenant_id, ls_id
                        )
                    )
                    scene_1_tag = False
                    continue
                else:
                    ## Normal
                    self.verbose("Normal. The ls_id's leader number = 1")
                    record.add_record("Normal. The ls_id's leader number = 1")

            if scene_1_tag is False:
                self.verbose("scene_1 is check")
                return record

            ## scene 2
            record.add_record("start step2")
            scene_2_tag = True
            for tenant_diagnose_data_by_tenant_id in diagnose_data:
                ls_id = tenant_diagnose_data_by_tenant_id["ls_id"]
                record.add_record("on ls_id: {1} ".format(tenant_id, ls_id))
                if tenant_diagnose_data_by_tenant_id["election_role"].upper() == "LEADER" and tenant_diagnose_data_by_tenant_id["palf_role"].upper() != "LEADER" and tenant_diagnose_data_by_tenant_id["palf_state"].upper() != "ACTIVE":
                    self.stdio.warn(
                        "tenant_id: {0}, ls_id: {1} on oceanbase.__all_virtual_ha_diagnose election_role is LEADER but palf_role is {2} and palf_state is {3}".format(
                            tenant_id, ls_id, tenant_diagnose_data_by_tenant_id["palf_role"], tenant_diagnose_data_by_tenant_id["palf_state"]
                        )
                    )
                    record.add_record(
                        "tenant_id: {0}, ls_id: {1} on oceanbase.__all_virtual_ha_diagnose election_role is LEADER but palf_role is {2} and palf_state is {3}".format(
                            tenant_id, ls_id, tenant_diagnose_data_by_tenant_id["palf_role"], tenant_diagnose_data_by_tenant_id["palf_state"]
                        )
                    )
                    record.add_suggest(
                        "tenant_id: {0}, ls_id: {1} on oceanbase.__all_virtual_ha_diagnose election_role is LEADER but palf_role is {2} and palf_state is {3}. The newly elected leader failed to take office in the palf layer, and the palf_state can be used to determine at which stage the palf failed to take office.".format(
                            tenant_id, ls_id, tenant_diagnose_data_by_tenant_id["palf_role"], tenant_diagnose_data_by_tenant_id["palf_state"]
                        )
                    )
                    scene_2_tag = False
                else:
                    self.verbose(
                        "tenant_id: {0}, ls_id: {1} on oceanbase.__all_virtual_ha_diagnose election_role is LEADER , palf_role is {2} and palf_state is {3}".format(
                            tenant_id, ls_id, tenant_diagnose_data_by_tenant_id["palf_role"], tenant_diagnose_data_by_tenant_id["palf_state"]
                        )
                    )
                    record.add_record("Normal. Unable to find a replica where both election_role and palf_role are leaders, but log_handler_role is follower")
                    continue
            if scene_2_tag is False:
                self.verbose("scene_2 is check")
                return
            ## scene 3
            record.add_record("start step3")

            for tenant_diagnose_data_by_tenant_id in diagnose_data:
                record.add_record("tenant_id: {0}, ls_id: {1} ".format(tenant_diagnose_data_by_tenant_id["tenant_id"], tenant_diagnose_data_by_tenant_id["ls_id"]))
                if tenant_diagnose_data_by_tenant_id["election_role"].upper() == "LEADER" and tenant_diagnose_data_by_tenant_id["palf_role"].upper() == "LEADER" and tenant_diagnose_data_by_tenant_id["log_handler_role"].upper() == "follower":
                    record.add_record("election_role:LEADER , palf_role: LEADER, log_handler_role: follower")
                    log_handler_takeover_state = tenant_diagnose_data_by_tenant_id["log_handler_takeover_state"].lower()
                    record.add_record("log_handler_takeover_state: {0}".format(log_handler_takeover_state))
                    if log_handler_takeover_state == "wait_replay_done":
                        record.add_suggest("Previous stuck waiting for replay steps. Please check the issue about replay")
                    elif log_handler_takeover_state == "unknown":
                        record.add_suggest("Please check observe whether the remaining log streams of this tenant also have the issue of log handler failure in taking over")
                    elif log_handler_takeover_state == "wait_rc_handler_done":
                        log_handler_takeover_log_type = tenant_diagnose_data_by_tenant_id["log_handler_takeover_log_type"]
                        record.add_record("log_handler_takeover_log_type: {0}".format(log_handler_takeover_log_type))
                        record.add_suggest("log_handler_takeover_log_type is {0}. Please report oceanbase's community".format(log_handler_takeover_log_type))
                else:
                    record.add_record("Normal.Unable to find a replica where the selection_role is a leader, but the palf_role and palf_state are not leaders or active, respectively")

            if record.suggest_is_empty():
                record.add_suggest("Normal. Not find the reason of the log handler failure in taking over.")
        except Exception as e:
            raise RCAExecuteException("tenant_id: {0}. execute_421_no_leader_by_tenant_id execute error: {1}".format(tenant_id, e))
        finally:

            return record

    def check_election_leader_by_tenant_id(self, tenant_id):
        try:
            self.stdio.verbose("start check_election_leader_by_tenant_id")
            self.gather_log.set_parameters("scope", "election")
            self.gather_log.grep("T{0}_.*dump proposer info".format(tenant_id))
            self.work_path = self.store_dir
            logs_name = self.gather_log.execute()
            if len(logs_name) == 0:
                self.stdio.warn("check_election_leader_by_tenant_id not found log about election_leader. tenant_id: {0}".format(tenant_id))
                return ""
            self.stdio.verbose("check_election_leader_by_tenant_id tenant_id: {0}, logs_name:{1}".format(tenant_id, logs_name))
            for name in logs_name:
                self.stdio.verbose("read the log file: {0}".format(name))
                with open(name, 'rb') as file:
                    file.seek(0, os.SEEK_END)
                    file_length = file.tell()
                    file.seek(max(file_length - 1024, 0), 0)
                    lines = file.readlines()
                    last_line = lines[-1].decode().strip()
                    pattern = r'addr:"([^"]+)"'
                    match = re.search(pattern, last_line)
                    if match:
                        return match.group(1)
                    else:
                        return ""
        except Exception as e:
            raise RCAExecuteException("check_election_leader_by_tenant_id: {1}. execute error: {0}".format(e, tenant_id))

    def export_result(self):
        super().export_result()

    def get_scene_info(self):

        return {
            "name": "log_error",
            "info_en": "Troubleshooting log related issues. Currently supported scenes: no_leader.",
            "info_cn": '日志相关问题排查。目前支持：无主场景。',
        }


log_error = LogErrorScene()
