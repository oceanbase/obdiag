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
@time: 2023/12/22
@file: rca_handler.py
@desc:
"""
import datetime
import json
import os
from textwrap import fill
from common.command import get_obproxy_version, get_observer_version_by_sql, get_observer_version
from prettytable import PrettyTable
from common.ob_connector import OBConnector
from handler.rca.plugins.gather import Gather_log
from handler.rca.rca_exception import RCANotNeedExecuteException
from handler.rca.rca_list import RcaScenesListHandler
from common.ssh import SshHelper
from common.tool import Util
from common.tool import StringUtils
from colorama import Fore, Style


class RCAHandler:
    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.ob_cluster = self.context.cluster_config
        self.options = self.context.options
        observer_nodes = self.context.cluster_config.get("servers")

        # build observer_nodes ,add ssher
        context_observer_nodes = []
        if observer_nodes is not None:
            for node in observer_nodes:
                ssh = SshHelper(True, node.get("ip"), node.get("ssh_username"), node.get("ssh_password"), node.get("ssh_port"), node.get("ssh_key_file"), node)
                node["ssher"] = ssh
                context_observer_nodes.append(node)
            self.context.set_variable("observer_nodes", context_observer_nodes)
        obproxy_nodes = self.context.obproxy_config.get("servers")
        # build obproxy_nodes
        context_obproxy_nodes = []
        if obproxy_nodes is not None:
            for node in obproxy_nodes:
                ssh = SshHelper(True, node.get("ip"), node.get("ssh_username"), node.get("ssh_password"), node.get("ssh_port"), node.get("ssh_key_file"), node)
                node["ssher"] = ssh
                context_obproxy_nodes.append(node)
            self.context.set_variable("obproxy_nodes", context_obproxy_nodes)

        # build ob_connector
        try:
            if self.ob_cluster is not None:
                ob_connector = OBConnector(
                    ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.ob_cluster.get("tenant_sys").get("user"), password=self.ob_cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=10000
                )
                self.context.set_variable("ob_connector", ob_connector)
        except Exception as e:
            self.stdio.warn("RCAHandler init ob_connector failed: {0}. If the scene need it, please check the conf.yaml".format(str(e)))
        # build report
        store_dir = Util.get_option(self.options, 'store_dir')
        if store_dir is None:
            store_dir = "./rca/"
        self.stdio.verbose("RCAHandler.init store dir: {0}".format(store_dir))
        report = Result(self.context)
        report.set_save_path(store_dir)
        self.context.set_variable("report", report)

        # build observer_version by sql or ssher. If using SSHer, the observer_version is set to node[0].
        observer_version = ""
        try:
            observer_version = get_observer_version_by_sql(self.ob_cluster, self.stdio)
        except Exception as e:
            if len(context_observer_nodes) > 0:
                observer_version = get_observer_version(True, context_observer_nodes[0]["ssher"], context_observer_nodes[0]["home_path"], self.stdio)
            else:
                self.stdio.warn("RCAHandler Failed to get observer version:{0}".format(e))
        self.stdio.verbose("RCAHandler.init get observer version: {0}".format(observer_version))

        if observer_version != "":
            self.stdio.verbose("RCAHandler.init get observer version: {0}".format(observer_version))
            self.context.set_variable("observer_version", observer_version)
        else:
            self.stdio.warn("RCAHandler.init Failed to get observer version.")

        # build obproxy_version. just by ssh
        if self.context.get_variable("obproxy_version", default="") == "":
            if len(obproxy_nodes) > 0:
                obproxy_version = ""
                try:
                    if len(context_obproxy_nodes) > 0:
                        obproxy_version = get_obproxy_version(True, context_obproxy_nodes[0]["ssher"], context_obproxy_nodes[0]["home_path"], self.stdio)
                except Exception as e:
                    self.stdio.warn("RCAHandler.init Failed to get obproxy version. Error:{0}".format(e))
                if obproxy_version != "":
                    self.stdio.verbose("RCAHandler.init get obproxy version: {0}".format(obproxy_version))
                else:
                    self.stdio.warn("RCAHandler.init Failed to get obproxy version.")
                self.stdio.verbose("RCAHandler.init get obproxy version: {0}".format(obproxy_version))
                self.context.set_variable("obproxy_version", obproxy_version)

        self.context.set_variable("ob_cluster", self.ob_cluster)

        # set rca_deep_limit
        rca_list = RcaScenesListHandler(self.context)
        all_scenes_info, all_scenes_item = rca_list.get_all_scenes()
        self.context.set_variable("rca_deep_limit", len(all_scenes_info))
        self.all_scenes = all_scenes_item

        self.rca_scene_parameters = None
        self.rca_scene = None
        self.cluster = self.context.get_variable("ob_cluster")
        self.nodes = self.context.get_variable("observer_nodes")
        self.obproxy_nodes = self.context.get_variable("obproxy_nodes")
        self.store_dir = store_dir

        # init input parameters
        self.report = None
        self.tasks = None
        rca_scene_parameters = Util.get_option(self.options, 'input_parameters', "")
        if rca_scene_parameters != "":
            try:
                rca_scene_parameters = json.loads(rca_scene_parameters)
            except Exception as e:
                raise Exception("Failed to parse input_parameters. Please check the option is json:{0}".format(rca_scene_parameters))
        else:
            rca_scene_parameters = {}
        self.context.set_variable("input_parameters", rca_scene_parameters)
        self.store_dir = Util.get_option(self.options, 'store_dir', "./rca/")
        self.context.set_variable("store_dir", self.store_dir)
        self.stdio.verbose(
            "RCAHandler init.cluster:{0}, init.nodes:{1}, init.obproxy_nodes:{2}, init.store_dir:{3}".format(
                self.cluster.get("ob_cluster_name") or self.cluster.get("obproxy_cluster_name"), StringUtils.node_cut_passwd_for_log(self.nodes), StringUtils.node_cut_passwd_for_log(self.obproxy_nodes), self.store_dir
            )
        )

    def get_result_path(self):
        return self.store_dir

    def handle(self):

        scene_name = Util.get_option(self.options, 'scene', None)
        if scene_name:
            scene_name = scene_name.strip()
            if scene_name in self.all_scenes:
                self.rca_scene = self.all_scenes[scene_name]
            if self.rca_scene is None:
                raise Exception("rca_scene :{0} is not exist".format(scene_name))

            self.store_dir = os.path.expanduser("{0}/{1}_{2}".format(self.store_dir, scene_name, datetime.datetime.now().strftime('%Y%m%d%H%M%S')))
            if not os.path.exists(self.store_dir):
                os.mkdir(self.store_dir)

            self.context.set_variable("store_dir", self.store_dir)
            self.stdio.verbose("{1} store_dir:{0}".format(self.store_dir, scene_name))
            # build gather_log
            self.context.set_variable("gather_log", Gather_log(self.context))
            try:
                if self.rca_scene.init(self.context) is False:
                    return
            except Exception as e:
                raise Exception("rca_scene.init err: {0}".format(e))
            self.stdio.verbose("{0} init success".format(scene_name))
        else:
            raise Exception("rca_scene :{0} is not exist or not input".format(scene_name))

    # get all tasks
    def execute(self):
        try:
            self.rca_scene.execute()
        except RCANotNeedExecuteException as e:
            self.stdio.warn("rca_scene.execute not need execute: {0}".format(e))
            pass
        except Exception as e:
            raise Exception("rca_scene.execute err: {0}".format(e))
        try:
            self.rca_scene.export_result()
        except Exception as e:
            raise Exception("rca_scene.export_result err: {0}".format(e))
        self.stdio.print("rca finished. For more details, the result on '" + Fore.YELLOW + self.get_result_path() + Style.RESET_ALL + "' \nYou can get the suggest by '" + Fore.YELLOW + "cat " + self.get_result_path() + "/record" + Style.RESET_ALL + "'")


class RcaScene:
    def __init__(self):
        self.gather_log = None
        self.stdio = None
        self.input_parameters = None
        self.ob_cluster = None
        self.ob_connector = None
        self.store_dir = None
        self.obproxy_version = None
        self.observer_version = None
        self.report = None
        self.obproxy_nodes = None
        self.observer_nodes = None
        self.context = None
        self.name = type(self).__name__
        self.Result = None

    def init(self, context):
        self.context = context
        self.stdio = context.stdio
        self.Result = Result(self.context)
        self.observer_nodes = context.get_variable('observer_nodes')
        self.obproxy_nodes = context.get_variable('obproxy_nodes')
        self.report = context.get_variable('report')
        self.obproxy_version = context.get_variable('obproxy_version', default="")
        self.observer_version = context.get_variable('observer_version', default="")
        self.ob_connector = context.get_variable('ob_connector', default=None)
        self.store_dir = context.get_variable('store_dir')
        self.ob_cluster = context.get_variable('ob_cluster')
        self.input_parameters = context.get_variable('input_parameters') or {}
        self.gather_log = context.get_variable('gather_log')

    def execute(self):
        # 获取获取根因分析结果(包括运维建议)，返回RCA_ResultRecord格式
        raise Exception("rca ({0}) scene.execute() undefined".format(type(self).__name__))

    def get_result(self):
        # 设定场景分析的返回场景使用说明，需要的参数等等
        raise Exception("rca ({0}) scene.get_result() undefined".format(type(self).__name__))

    def get_scene_info(self):
        raise Exception("rca ({0})  scene.get_scene_info() undefined".format(type(self).__name__))

    def export_result(self):
        return self.Result.export()

    def get_all_tenants_id(self):
        try:
            if self.ob_connector is None:
                raise Exception("ob_connector is None")
            all_tenant_id_data = self.ob_connector.execute_sql("select tenant_id from oceanbase.__all_tenant;")[0]
            return all_tenant_id_data
        except Exception as e:
            raise Exception("run rca's get_all_tenants_id. Exception: {0}".format(e))


class Result:

    def __init__(self, context):
        # self.suggest = ""
        self.records = []
        self.context = context
        self.stdio = context.stdio
        self.save_path = self.context.get_variable('store_dir')

    def set_save_path(self, save_path):
        self.save_path = os.path.expanduser(save_path)
        if os.path.exists(save_path):
            self.save_path = save_path
        else:
            os.makedirs(save_path)
            self.save_path = save_path
        self.stdio.verbose("rca result save_path is :{0}".format(self.save_path))

    def export(self):
        record_file_name = os.path.expanduser("{0}/{1}".format(self.save_path, "record"))
        self.stdio.verbose("save record to {0}".format(record_file_name))
        with open(record_file_name, 'w') as f:
            for record in self.records:
                record_data = record.export_record()
                f.write(record_data.get_string())
                f.write("\n")
                f.write(record.export_suggest())
                f.write("\n")


class RCA_ResultRecord:
    def __init__(self):
        self.records = []
        self.suggest = "The suggest: "

    def add_record(self, record):
        self.records.append(record)

    def add_suggest(self, suggest):
        self.suggest += suggest

    def suggest_is_empty(self):
        return self.suggest == "The suggest: "

    def export_suggest(self):
        return self.suggest

    def export_record(self):
        record_tb = PrettyTable(["step", "info"])
        record_tb.align["info"] = "l"
        record_tb.title = "record"
        i = 0
        while i < len(self.records):
            record_tb.add_row([i + 1, fill(self.records[i], width=100)])
            i += 1
        return record_tb
