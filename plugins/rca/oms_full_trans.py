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
@time: 2024/1/2
@file: oms_full_trans.py
@desc:
"""
import os.path

from src.handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
)
from src.handler.rca.rca_handler import RcaScene


class OMSFullTransScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.component_id = None

    def init(self, context):
        super().init(context)
        # check component id
        self.component_id = self.input_parameters.get("component_id")
        self.stdio.verbose("oms component_id is {0}".format(self.component_id))
        if self.component_id is None:
            raise RCAInitException("component_id is not exist.")

    def execute(self):
        try:
            self.record.add_record("oms component_id is {0}".format(self.component_id))
            # 1. check oms woker path
            for server in self.oms_nodes:
                ssh_client = server["ssher"]
                try:
                    self.stdio.verbose("node:{0} check oms woker path: {1}/{2}".format(ssh_client.get_ip(), server.get("run_path"), self.component_id))
                    ssh_client.exec_cmd("cd {0}/{1}".format(server.get("run_path"), self.component_id))
                except Exception as e:
                    self.record.add_record("node:{0} run_path: {1}/{2} is not exist.".format(ssh_client.get_ip(), server.get("run_path"), self.component_id))
                    self.record.add_suggest("the component's work path is not exist. May be the oms'resource is not enough.")
                    return
            self.record.add_record("check oms woker path is exist.")
            # 2. download oms error.details
            error_details_paths = []
            for server in self.oms_nodes:
                ssh_client = server["ssher"]
                try:
                    self.stdio.verbose("node:{0} download oms error.details".format(ssh_client.get_ip()))
                    # check error.details is exist
                    if not os.path.exists("{0}/error.details".format(self.store_dir)):
                        os.makedirs("{0}/error.details".format(self.store_dir))
                    local_error_details_path = "{0}/error.details_{1}".format(self.store_dir, ssh_client.get_name())
                    self.stdio.verbose("local_error_details_path:{0}".format(local_error_details_path))
                    ssh_client.download("{0}/{1}/error.details".format(server.get("run_path"), self.component_id), local_error_details_path)
                    self.record.add_record("download oms error.details is success.")
                    error_details_paths.append(local_error_details_path)
                except Exception as e:
                    if "No such file" in "{0}".format(e):
                        self.record.add_record("node:{0} not find error.details.".format(ssh_client.get_ip()))
                    else:
                        self.stdio.error("node:{0} download oms error.details error: {1}".format(ssh_client.get_ip(), e))
                        self.record.add_record("node:{0} download oms error.details error: {1}".format(ssh_client.get_ip(), e))
            if len(error_details_paths) == 0:
                self.record.add_record("not find oms error.details")
            self.record.add_record("downland oms error.details is end.")
            # 3. gather the oms log
            oms_logs_name = []
            try:
                self.stdio.verbose("gather oms log")
                self.gather_log.set_parameters("target", "oms")
                self.gather_log.set_parameters("store_dir", "oms")
                self.gather_log.set_parameters("oms_component_id", self.component_id)
                self.record.add_record("get oms log about connector by component_id: {0}".format(self.component_id))
                if self.input_parameters.get("since") is not None:
                    since = self.input_parameters.get("since")
                    self.gather_log.set_parameters("since", since)
                self.work_path = self.store_dir
                oms_logs_name = self.gather_log.execute()
                self.stdio.verbose("oms_logs_name:{0}".format(oms_logs_name))
                self.record.add_record("oms_logs save on {0}".format(self.gather_log.work_path))
            except Exception as e:
                self.record.add_record("gather the oms log error: {0}".format(e))
            self.record.add_record("gather the oms log is end.")
            # get sinkType and sourceType on conf/coordinator.json
            self.record.add_record("get sinkType and sourceType on conf/coordinator.json")
            for server in self.oms_nodes:
                ssh_client = server["ssher"]
                try:
                    sinkType_data = ssh_client.exec_cmd('cat {0}/{1}/conf/coordinator.json|grep "sinkType"'.format(server.get("run_path"), self.component_id))
                    sourceType_data = ssh_client.exec_cmd('cat {0}/{1}/conf/coordinator.json|grep "sourceType"'.format(server.get("run_path"), self.component_id))
                    self.record.add_record("on node {0}, sinkType: {1}, sourceType: {2}".format(ssh_client.get_name(), sinkType_data, sourceType_data))
                except Exception as e:
                    self.record.add_record("get {1} sinkType and sourceType on conf/coordinator.json error: {0}".format(e, ssh_client.get_ip()))
                    continue

            # 4. check the oms full trans
            if len(error_details_paths) > 0:
                self.record.add_record("check log error.details is start.")
                # error.details SINK_TABLE_NOT_FOUND
                for error_details_path in error_details_paths:
                    try:
                        self.stdio.verbose("check {0} is start.".format(error_details_path))
                        with open(error_details_path, 'r', encoding='utf-8', errors='ignore') as f:
                            # TDDO find something on error.details
                            SINK_TABLE_NOT_FOUND_tag = False
                            SINK_TABLE_IS_NOT_EMPTY_tag = False
                            for line in f.readlines():
                                if "SINK_TABLE_NOT_FOUND" in line and SINK_TABLE_NOT_FOUND_tag is False:
                                    self.record.add_record("error.details SINK_TABLE_NOT_FOUND is exist.")
                                    self.record.add_suggest("the component_id is {0}, the sink table is not found.".format(self.component_id))
                                    SINK_TABLE_NOT_FOUND_tag = True
                                    continue
                                elif "SINK_TABLE_IS_NOT_EMPTY" in line and SINK_TABLE_IS_NOT_EMPTY_tag is False:
                                    self.record.add_record("error.details SINK_TABLE_IS_NOT_EMPTY is exist.")
                                    self.record.add_suggest("the component_id is {0}, the sink table is not empty.".format(self.component_id))
                                    SINK_TABLE_IS_NOT_EMPTY_tag = True
                                    continue
                            if SINK_TABLE_NOT_FOUND_tag is False and SINK_TABLE_IS_NOT_EMPTY_tag is False:
                                self.record.add_record("error.details SINK_TABLE_NOT_FOUND and SINK_TABLE_IS_NOT_EMPTY is not exist.")
                                self.record.add_suggest("the component_id is {0}, the sink table is empty.".format(self.component_id))
                    except Exception as e:
                        raise RCAExecuteException("error.details SINK_TABLE_NOT_FOUND error: {0}".format(e))
                else:
                    pass

        except Exception as e:
            self.record.add_record("execute oms full trans error: {0}".format(e))
            raise RCAExecuteException(e)
        finally:
            self.record.add_suggest("if you want to know more about the result, please contact with oms team with {0}".format(self.store_dir))
            return self.record

    def get_scene_info(self):
        return {
            "name": "oms_full_trans",
            "info_en": "OMS full connector error",
            "info_cn": "oms全量迁移报错",
        }


oms_full_trans = OMSFullTransScene()
