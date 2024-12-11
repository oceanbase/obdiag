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
import xmltodict
import yaml
from textwrap import fill
from obdiag.common.command import (
    get_obproxy_version,
    get_observer_version,
)
import traceback
from prettytable import PrettyTable
from obdiag.common.ob_connector import OBConnector
from obdiag.common.ssh_client.ssh import SshClient
from obdiag.handler.rca.plugins.gather import Gather_log
from obdiag.handler.rca.rca_exception import RCANotNeedExecuteException, RCAReportException
from obdiag.handler.rca.rca_list import RcaScenesListHandler
from obdiag.common.tool import Util
from obdiag.common.tool import StringUtils
from colorama import Fore, Style
from jinja2 import Template
from obdiag.common.result_type import ObdiagResult
from obdiag.common.version import OBDIAG_VERSION
from obdiag.common.scene import get_version_by_type


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
                ssh = SshClient(context, node)
                node["ssher"] = ssh
                context_observer_nodes.append(node)
            self.context.set_variable("observer_nodes", context_observer_nodes)
        obproxy_nodes = self.context.obproxy_config.get("servers")
        # build obproxy_nodes
        context_obproxy_nodes = []
        if obproxy_nodes is not None:
            for node in obproxy_nodes:
                ssh = SshClient(context, node)
                node["ssher"] = ssh
                context_obproxy_nodes.append(node)
            self.context.set_variable("obproxy_nodes", context_obproxy_nodes)
        # build oms_nodes
        oms_nodes = self.context.oms_config.get("servers")
        context_oms_nodes = []
        if oms_nodes is not None:
            for node in oms_nodes:
                ssh = SshClient(context, node)
                node["ssher"] = ssh
                context_oms_nodes.append(node)
            self.context.set_variable("oms_nodes", context_oms_nodes)

        # build ob_connector
        try:
            if self.ob_cluster.get("db_host") is not None:
                ob_connector = OBConnector(
                    context=self.context,
                    ip=self.ob_cluster.get("db_host"),
                    port=self.ob_cluster.get("db_port"),
                    username=self.ob_cluster.get("tenant_sys").get("user"),
                    password=self.ob_cluster.get("tenant_sys").get("password"),
                    timeout=10000,
                )
                self.context.set_variable("ob_connector", ob_connector)
        except Exception as e:
            self.stdio.warn("RCAHandler init ob_connector failed: {0}. If the scene need it, please check the conf".format(str(e)))
        # build report
        store_dir = Util.get_option(self.options, "store_dir")
        if store_dir is None:
            store_dir = "./obdiag_rca/"
        self.stdio.verbose("RCAHandler.init store dir: {0}".format(store_dir))
        report = Result(self.context)
        report.set_save_path(store_dir)
        self.context.set_variable("report", report)
        # build observer_version by sql or ssher. If using SSHer, the observer_version is set to node[0].
        observer_version = ""
        try:
            observer_version = get_observer_version(self.context)
        except Exception as e:
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
                        obproxy_version = get_obproxy_version(context)
                except Exception as e:
                    self.stdio.warn("RCAHandler.init Failed to get obproxy version. Error:{0}".format(e))
                if obproxy_version != "":
                    self.stdio.verbose("RCAHandler.init get obproxy version: {0}".format(obproxy_version))
                else:
                    self.stdio.warn("RCAHandler.init Failed to get obproxy version.")
                self.context.set_variable("obproxy_version", obproxy_version)
        self.context.set_variable("ob_cluster", self.ob_cluster)
        # set rca_deep_limit
        rca_list = RcaScenesListHandler(self.context)
        all_scenes_info, all_scenes_item = rca_list.get_all_scenes()
        self.context.set_variable("rca_deep_limit", len(all_scenes_info))
        self.all_scenes = all_scenes_item
        self.rca_scene = None
        self.cluster = self.context.get_variable("ob_cluster")
        self.nodes = self.context.get_variable("observer_nodes")
        self.obproxy_nodes = self.context.get_variable("obproxy_nodes")
        self.store_dir = store_dir
        # init input parameters
        self.report = None
        self.tasks = None
        self.context.set_variable("input_parameters", Util.get_option(self.options, "input_parameters"))
        self.context.set_variable("env", Util.get_option(self.options, "input_parameters"))
        self.store_dir = Util.get_option(self.options, "store_dir", "./obdiag_rca/")
        self.context.set_variable("store_dir", self.store_dir)
        self.stdio.verbose(
            "RCAHandler init.cluster:{0}, init.nodes:{1}, init.obproxy_nodes:{2}, init.store_dir:{3}".format(
                self.cluster.get("ob_cluster_name") or self.cluster.get("obproxy_cluster_name"),
                StringUtils.node_cut_passwd_for_log(self.nodes),
                StringUtils.node_cut_passwd_for_log(self.obproxy_nodes),
                self.store_dir,
            )
        )

    def get_result_path(self):
        return self.store_dir

    def handle(self):
        scene_name = Util.get_option(self.options, "scene", None)
        if scene_name:
            scene_name = scene_name.strip()
            if scene_name in self.all_scenes:
                self.rca_scene = self.all_scenes[scene_name]
            if self.rca_scene is None:
                raise Exception("rca_scene :{0} is not exist".format(scene_name))

            self.store_dir = os.path.expanduser(os.path.join(self.store_dir, "obdiag_{0}_{1}".format(scene_name, datetime.datetime.now().strftime("%Y%m%d%H%M%S"))))
            if not os.path.exists(self.store_dir):
                os.makedirs(self.store_dir)
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
            return self.__execute()
        else:
            self.stdio.error("rca_scene :{0} is not exist or not input".format(scene_name))
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="rca_scene :{0} is not exist or not input".format(scene_name))

    # get all tasks
    def __execute(self):
        try:
            self.rca_scene.execute()
        except RCANotNeedExecuteException as e:
            self.stdio.warn("rca_scene.execute not need execute: {0}".format(e))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, data={"result": "rca_scene.execute not need execute"})
        except Exception as e:
            self.stdio.verbose(traceback.format_exc())
            self.stdio.error("rca_scene.execute err: {0}".format(e))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="rca_scene.execute err: {0}".format(e))
        try:
            self.rca_scene.export_result()
        except Exception as e:
            self.stdio.verbose(traceback.format_exc())
            self.stdio.error("rca_scene.export_result err: {0}".format(e))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="rca_scene.export_result err: {0}".format(e))
        self.stdio.print(
            "rca finished. For more details, the result on '"
            + Fore.YELLOW
            + self.get_result_path()
            + Style.RESET_ALL
            + "' \nYou can get the suggest by '"
            + Fore.YELLOW
            + "cat "
            + self.get_result_path()
            + "/record"
            + "."
            + self.rca_scene.Result.rca_report_type
            + Style.RESET_ALL
            + "'"
        )
        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.get_result_path(), "record": self.rca_scene.Result.records_data()})


class RcaScene:
    def __init__(self):
        self.work_path = None
        self.record = None
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
        self.oms_nodes = None
        self.context = None
        self.name = type(self).__name__
        self.Result = None

    def init(self, context):
        self.context = context
        self.stdio = context.stdio
        self.record = RCA_ResultRecord(self.stdio)
        self.Result = Result(self.context)
        self.Result.records.append(self.record)
        self.observer_nodes = context.get_variable("observer_nodes")
        self.obproxy_nodes = context.get_variable("obproxy_nodes")
        self.oms_nodes = context.get_variable("oms_nodes")
        self.report = context.get_variable("report")
        self.obproxy_version = context.get_variable("obproxy_version", default="")
        self.observer_version = context.get_variable("observer_version", default="")
        self.ob_connector = context.get_variable("ob_connector", default=None)
        self.store_dir = context.get_variable("store_dir")
        self.ob_cluster = context.get_variable("ob_cluster")
        self.input_parameters = context.get_variable("input_parameters") or {}
        self.gather_log = context.get_variable("gather_log")
        self.work_path = self.store_dir

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
        self.record_file_name = ""
        self.records = []
        self.context = context
        self.stdio = context.stdio
        self.save_path = self.context.get_variable("store_dir")
        self.rca_report_type = Util.get_option(self.context.options, 'report_type')
        self.scene = Util.get_option(self.context.options, "scene")
        self.version = "unknown"
        try:
            if self.context.get_variable("ob_cluster").get("db_host") is not None or len(self.context.cluster_config.get("servers")) > 0:
                self.version = get_version_by_type(self.context, "observer")
        except Exception as e:
            self.stdio.verbose("rca get obcluster version fail. Maybe the scene need not it, skip it. Exception: {0}".format(e))
            self.stdio.warn("rca get obcluster version fail. if the scene need not it, skip it")

    def set_save_path(self, save_path):
        self.save_path = os.path.expanduser(save_path)
        if os.path.exists(save_path):
            self.save_path = save_path
        else:
            os.makedirs(save_path)
            self.save_path = save_path
        self.stdio.verbose("rca result save_path is :{0}".format(self.save_path))

    def export(self):
        try:
            self.record_file_name = os.path.expanduser("{0}/{1}".format(self.save_path, "record" + "." + self.rca_report_type))
            self.stdio.verbose("save record to {0}".format(self.record_file_name))
            if self.rca_report_type == "table":
                self.export_report_table()
            elif self.rca_report_type == "json":
                self.export_report_json()
            elif self.rca_report_type == "xml":
                self.export_report_xml()
            elif self.rca_report_type == "yaml":
                self.export_report_yaml()
            elif self.rca_report_type == "html":
                self.export_report_html()
            else:
                raise RCAReportException("export_report_type: {0} is not support".format(self.rca_report_type))
        except Exception as e:
            self.stdio.error("export_report Exception : {0}".format(e))
            raise RCAReportException(e)

    def export_report_table(self):
        with open(self.record_file_name, "w") as f:
            for record in self.records:
                if record.records is None or len(record.records) == 0:
                    continue
                record_data = record.export_record_table()
                f.write(record_data.get_string())
                f.write("\n")
                f.write(record.export_suggest())
                f.write("\n")

    def export_report_json(self):
        with open(self.record_file_name, "w", encoding='utf-8') as f:
            json.dump(self.records_data(), f, ensure_ascii=False)

    def export_report_xml(self):
        with open(self.record_file_name, 'w', encoding='utf-8') as f:
            allreport = {"report": self.records_data()}
            json_str = json.dumps(allreport)
            xml_str = xmltodict.unparse(json.loads(json_str))
            f.write(xml_str)
            f.close()

    def export_report_yaml(self):
        with open(self.record_file_name, 'w', encoding='utf-8') as f:
            yaml.dump(self.records_data(), f)

    def export_report_html(self):
        try:
            html_template_head = """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>{{ report_title }}</title>
                    <style>
                        body {
                            padding-top: 60px;
                            font: 16px/1.8 -apple-system, blinkmacsystemfont, "Helvetica Neue", helvetica, segoe ui, arial, roboto, "PingFang SC", "miui", "Hiragino Sans GB", "Microsoft Yahei", sans-serif;
                            background: #f4f6fa linear-gradient(180deg, #006aff 0%, #006aff00 100%) no-repeat;
                            background-size: auto 120px;
                        }

                        section {
                            background: #fff;
                            padding: 2em;
                            margin: 0 auto 2em;
                            max-width: 1280px;
                        }

                        header {
                            padding: 1em;
                            margin: -60px auto 0;
                            max-width: 1280px;
                        }

                        header>svg {
                            margin-left: -2em;
                        }

                        .line{ border-bottom:1px solid;}

                        .titleClass {
                            display: block;
                            white-space: nowrap;
                            margin-bottom: 1em;
                            font-weight: 500;
                            font-size: 1.25em;
                            text-align: left;
                            background: transparent;
                        }

                        table {
                            border-collapse: collapse;
                        }

                        th,
                        td {
                            border: 1px solid #f0f0f0;
                            padding: 8px;
                            text-align: left;
                        }

                        td+td {
                            font-family: 'Courier New', 'Consolas';
                        }

                        th {
                            background-color: #fafafa;
                        }
                    </style>
                </head>
                <body>
                    <header>
                        <svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="160"
                            viewBox="0 0 122 16">
                            <defs>
                                <path id="a" d="M0 0h12.019v4.626H0z" />
                            </defs>
                            <g fill="none" fill-rule="evenodd">
                                <path fill="#FFF"
                                    d="M64.285 9.499 66.2 5.746l.633 3.753zm.935-7.086-6.08 11.273h3.027l.985-1.96h4.054l.317 1.96h3.025l-2.23-11.273zM37.52 9.29a3.912 3.912 0 0 1-1.937 1.636 3.334 3.334 0 0 1-1.22.233 2.75 2.75 0 0 1-1.14-.233 2.363 2.363 0 0 1-.866-.65 2.511 2.511 0 0 1-.497-.986 2.91 2.91 0 0 1-.035-1.242c.077-.447.23-.861.458-1.24a4 4 0 0 1 .835-.987c.327-.279.69-.495 1.087-.65a3.355 3.355 0 0 1 1.22-.231c.414 0 .795.077 1.14.23.346.156.636.372.874.651.238.28.405.608.504.988.1.378.11.792.035 1.24-.077.448-.23.862-.458 1.24m2.446-5.471a4.538 4.538 0 0 0-1.701-1.264c-.692-.303-1.477-.455-2.355-.455-.888 0-1.727.152-2.517.455a7.173 7.173 0 0 0-2.12 1.264 7.525 7.525 0 0 0-1.568 1.891 6.882 6.882 0 0 0-.847 2.34c-.143.837-.127 1.616.05 2.34a4.72 4.72 0 0 0 .92 1.891c.44.538 1.003.959 1.69 1.263.685.304 1.473.455 2.36.455a6.96 6.96 0 0 0 2.51-.455 7.215 7.215 0 0 0 2.134-1.263 7.384 7.384 0 0 0 1.575-1.891 6.893 6.893 0 0 0 .85-2.34c.141-.837.125-1.617-.05-2.34a4.667 4.667 0 0 0-.93-1.891M59.105 11.203h-5.744l.332-1.943h5.556l.424-2.481h-5.556l.322-1.884h5.744l.424-2.482h-8.583L50.1 13.686l8.586-.002zM78.872 9.176l-3.315-6.764H72.72l-1.925 11.273h2.839l1.176-6.763 3.314 6.763h2.824l1.925-11.273H80.05zM88.09 11.129c-.342.19-.869.284-1.584.284h-.997l.409-2.392h.997c.714 0 1.21.095 1.485.284.278.19.38.493.308.912-.071.418-.277.723-.618.912m-1.426-6.474h.895c.888 0 1.27.365 1.145 1.092-.125.727-.63 1.091-1.518 1.091h-.895zm4.877 5.757c.058-.34.074-.659.048-.957a1.935 1.935 0 0 0-.246-.807 1.752 1.752 0 0 0-.59-.607 2.881 2.881 0 0 0-.974-.365c.45-.26.806-.576 1.068-.95.261-.374.44-.845.537-1.413.16-.936.022-1.654-.414-2.153-.435-.498-1.156-.748-2.16-.748h-4.602l-1.923 11.273h4.934c.579 0 1.112-.07 1.6-.21a3.9 3.9 0 0 0 1.286-.627c.371-.28.68-.623.929-1.032.248-.409.418-.876.507-1.404M108.454 6.808c-.218-.08-.44-.154-.664-.224a3.082 3.082 0 0 1-.595-.247 1.178 1.178 0 0 1-.4-.336c-.092-.13-.121-.293-.088-.494.049-.288.206-.523.467-.702.263-.18.576-.27.944-.27.29 0 .577.063.865.188.289.124.565.316.829.574l1.52-2.286a6.084 6.084 0 0 0-1.577-.68 6.259 6.259 0 0 0-1.656-.231c-.578 0-1.117.088-1.615.268-.5.179-.939.431-1.317.755a4.32 4.32 0 0 0-.952 1.166 4.66 4.66 0 0 0-.527 1.518c-.095.558-.089 1.018.017 1.382.107.364.278.665.512.904.234.24.515.431.842.576.328.145.666.278 1.012.396.29.11.535.21.738.3.202.089.361.187.478.29a.75.75 0 0 1 .23.344.972.972 0 0 1 .013.442c-.048.28-.205.527-.469.748-.264.219-.628.328-1.09.328-.406 0-.8-.095-1.182-.284-.383-.189-.754-.478-1.113-.867l-1.618 2.363c1.033.847 2.24 1.27 3.619 1.27.666 0 1.277-.092 1.834-.276a4.687 4.687 0 0 0 1.466-.778c.42-.333.762-.735 1.03-1.203.268-.47.453-.991.55-1.57.147-.858.051-1.552-.287-2.086-.339-.533-.944-.958-1.816-1.278M48.175 2.099c-.763 0-1.516.147-2.262.44a7.259 7.259 0 0 0-2.04 1.227 7.56 7.56 0 0 0-1.578 1.868 6.757 6.757 0 0 0-.878 2.385c-.147.867-.125 1.666.068 2.4.194.732.507 1.365.942 1.899.436.532.973.946 1.613 1.24a4.93 4.93 0 0 0 2.09.44c.366 0 .743-.037 1.133-.111a9.502 9.502 0 0 0 1.276-.35l.308-.107.592-3.467c-.86.798-1.744 1.196-2.651 1.196-.415 0-.788-.08-1.118-.24a2.27 2.27 0 0 1-.821-.658 2.452 2.452 0 0 1-.454-.986 3.183 3.183 0 0 1-.012-1.241c.074-.438.219-.847.434-1.227.213-.378.474-.704.782-.978a3.61 3.61 0 0 1 1.044-.65c.39-.16.795-.24 1.222-.24.965 0 1.704.415 2.22 1.24l.596-3.497a6.472 6.472 0 0 0-1.249-.441 5.513 5.513 0 0 0-1.257-.142M101.474 11.32c-.46-.092-1.36-.142-2.892.223l.349 2.185h3.025z" />
                                <path fill="#FFF"
                                    d="m101.35 10.66-.492-2.483c-.731.012-1.647.123-2.784.41-.124.032-.252.065-.382.101-.63.173-1.215.29-1.744.368l1.66-3.267.356 2.186c1.137-.287 2.053-.398 2.785-.411l-1.023-5.108h-3.097l-6.08 11.272h3.025l1.091-2.146c.91-.064 2.014-.223 3.269-.567.182-.05.355-.093.523-.133 1.533-.365 2.432-.314 2.892-.223M116.537 6.871c-2.251.59-3.965.534-4.713.463l-.4 2.423c.213.017.461.03.746.036 1.11.021 2.738-.08 4.701-.595 1.863-.487 2.96-.457 3.524-.364l.402-2.433c-.945-.1-2.322-.037-4.26.47M117.12 2.51c-2.152.562-3.812.537-4.607.472l-.601 3.72c.527.048 1.526.09 2.852-.094l.212-1.297c.743-.09 1.575-.239 2.478-.475 1.986-.519 3.1-.45 3.628-.344l.403-2.44c-.947-.116-2.353-.07-4.366.457M115.994 11.076c-.711.186-1.369.308-1.96.385l.195-1.19a16.13 16.13 0 0 1-2.116.107 13.007 13.007 0 0 1-.733-.035l-.604 3.61c.235.02.519.038.85.045 1.11.02 2.74-.08 4.703-.595 1.756-.46 2.831-.458 3.42-.378l.401-2.428c-.94-.085-2.287-.011-4.156.479M13.32 16a21.931 21.931 0 0 1 2.705-.943 22.178 22.178 0 0 1 8.428-.686v-2.878a25.035 25.035 0 0 0-9.87 1.006c-.246.075-.49.154-.734.24-.48.163-.952.34-1.415.53z" />
                                <path fill="#FFF"
                                    d="M24.453 2.157v8.618a25.783 25.783 0 0 0-10.837 1.286A34.304 34.304 0 0 1 0 13.842V5.225a25.741 25.741 0 0 0 10.835-1.285 34.33 34.33 0 0 1 13.617-1.781" />
                                <g>
                                    <mask id="b" fill="#fff">
                                        <use xlink:href="#a" />
                                    </mask>
                                    <path fill="#FFF"
                                        d="M11.132 0a21.931 21.931 0 0 1-2.704.942A22.178 22.178 0 0 1 0 1.628v2.878A25.035 25.035 0 0 0 9.87 3.5a24.633 24.633 0 0 0 2.15-.77z"
                                        mask="url(#b)" />
                                </g>
                            </g>
                        </svg>
                    </header>
            """
            html_template_tail = """
                </body>
                </html>
            """
            html_template_report_info_table = """
                    <section>
                        <table>
                            <div style="font-weight: bold;font-size: 24px;">{{ report_title }}</div>
                            <p class="line"></p>
                            <tr>
                                <th>Report Time</th>
                                <th>obdiag Version</th>
                                <th>OB Cluster Ip</th>
                                <th>OB Version</th>
                            </tr>
                            <tr>
                                <td>{{ report_time }}</td>
                                <td>{{ obdiag_version }}</td>
                                <td>{{ ob_cluster_ip }}</td>
                                <td>{{ ob_version }}</td>
                            </tr>
                        </table>
                    </section>
            """
            html_template_data_table = """
                    <section>
                        <table>
                            <caption class="titleClass">{{ record_name }}</caption>
                            <tr>
                                <th>Step</th>
                                <th>Info</th>
                            </tr>
                            {% for info in infos %}
                            <tr id="row-{{ loop.index0 }}">
                                <td>{{ loop.index0 }}</td>
                                <td>{{ info }}</td>
                            </tr>
                            {% endfor %}
                        </table>
                    </section>
            """
            self.stdio.verbose("export report start")
            report_record_html = []
            report_suggest_html = []
            save_path_time = self.save_path.split("/")[-1].split("_")[-1]
            for data in self.records_data():
                for record in data["record"]:
                    report_record_html.append(record)
                report_suggest_html.append(data["suggest"])
            report_title_str = "obdiag RCA Report"
            if self.scene != "":
                report_title_str = self.scene + " scene report"

            fp = open(self.record_file_name, 'a+', encoding='utf-8')
            template_head = Template(html_template_head)
            template_table = Template(html_template_data_table)
            fp.write(template_head.render(report_title=report_title_str) + "\n")
            template_report_info_table = Template(html_template_report_info_table)
            cluster_ips = ""
            for server in self.context.cluster_config["servers"]:
                cluster_ips += server["ip"]
                cluster_ips += ";"
            fp.write(template_report_info_table.render(report_title=report_title_str, report_time=save_path_time, obdiag_version=OBDIAG_VERSION, ob_cluster_ip=cluster_ips, ob_version=self.version) + "\n")

            if len(report_record_html) != 0:
                rendered_report_all_html = template_table.render(record_name="RCA Rcord", infos=report_record_html)
                fp.write(rendered_report_all_html + "\n")
            if len(report_suggest_html) != 0:
                report_suggest_html = template_table.render(record_name="RCA Suggest", tasks=report_suggest_html)
                fp.write(report_suggest_html + "\n")

            template_tail = Template(html_template_tail)
            fp.write(template_tail.render())
            fp.close()
            self.stdio.verbose("export report end")
        except Exception as e:
            raise RCAReportException("export report {0}".format(e))

    def records_data(self):
        records_data = []
        for record in self.records:
            if record.records is None or len(record.records) == 0:
                continue
            records_data.append({"record": record.records, "suggest": record.suggest})
        return records_data


class RCA_ResultRecord:
    def __init__(self, stdio=None):
        self.records = []
        self.suggest = "The suggest: "
        self.stdio = stdio

    def add_record(self, record):
        self.records.append(record)
        if self.stdio is not None:
            self.stdio.verbose("add record: {0}".format(fill(record, width=100)))

    def add_suggest(self, suggest):
        self.suggest += suggest + "\n"
        if self.stdio is not None:
            self.stdio.verbose("add suggest: {0}".format(suggest))

    def suggest_is_empty(self):
        return self.suggest == "The suggest: "

    def export_suggest(self):
        return self.suggest

    def export_record_table(self):
        record_tb = PrettyTable(["step", "info"])
        record_tb.align["info"] = "l"
        record_tb.title = "record"
        i = 0
        while i < len(self.records):
            record_tb.add_row([i + 1, fill(self.records[i], width=100)])
            i += 1
        return record_tb
