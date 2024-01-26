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
@file: obdiag_client.py
@desc:
"""
from common.command import get_obdiag_display
from common.constant import const
from handler.analyzer.analyze_flt_trace import AnalyzeFltTraceHandler
from handler.analyzer.analyze_log import AnalyzeLogHandler
from handler.checker.check_handler import CheckHandler
from handler.gather.gather_log import GatherLogHandler
from handler.gather.gather_awr import GatherAwrHandler
from handler.gather.gather_obproxy_log import GatherObProxyLogHandler
from handler.gather.gather_sysstat import GatherOsInfoHandler
from handler.gather.gather_obadmin import GatherObAdminHandler
from handler.gather.gather_perf import GatherPerfHandler
from handler.gather.gather_plan_monitor import GatherPlanMonitorHandler
from common.config_helper import ConfigHelper
import base64
import os
import sys
from common.logger import logger
from telemetry.telemetry import telemetry
from utils.time_utils import get_current_us_timestamp
from utils.yaml_utils import read_yaml_data
from utils.version_utils import print_obdiag_version
from colorama import Fore, Style

if getattr(sys, 'frozen', False):
    absPath = os.path.dirname(os.path.abspath(sys.executable))
else:
    absPath = os.path.dirname(os.path.abspath(__file__))
INNER_CONFIG_FILE = os.path.join(absPath, "conf/inner_config.yml")

DEFAULT_CONFIG_FILE = os.path.join(os.path.expanduser('~'), ".obdiag/config.yml")


class OBDIAGClient(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(OBDIAGClient, cls).__new__(cls, *args, **kwargs)
            cls._inited = False
        return cls._inst

    def __init__(self):
        if not self._inited:
            self.config_file = const.DEFAULT_CONFIG_PATH
            self.inner_config_file = INNER_CONFIG_FILE
            self.gather_timestamp = get_current_us_timestamp()
            self.config = None
            self.inner_config = None
            self.observer_nodes = []
            self.obproxy_nodes = []
            self.other_nodes = []
            self.ocp = None
            self._inited = True
            self.ocp_url = None
            self.ocp_user = None
            self.ocp_password = None
            self.ocp_is_exits = None
            # ocp metadb
            self.ocp_metadb_user = None
            self.ocp_metadb_password = None
            self.ocp_metadb_ip = None
            self.ocp_metadb_port = None
            self.ocp_metadb_name = None
            # gather handler
            self.gather_awr_handler = None
            self.gather_log_handler = None
            self.gather_sysstat_handler = None
            self.gather_obstack_handler = None
            self.gather_perf_handler = None
            self.gather_clog_handler = None
            self.gather_slog_handler = None
            self.gather_plan_monitor_handler = None
            self.gather_obproxy_log_handler = None
            # analyze handler
            self.analyze_log_handler = None
            self.analyze_flt_trace_handler = None
            # params
            self.default_collect_pack_dir = ""
            # obdiag basic config
            self.basic_config = None
            # obdiag check
            self.check_handler = None
            self.obproxy_cluster = None
            self.ob_cluster = None
            self.obdiag_log_file = os.path.join(
                os.path.expanduser(const.OBDIAG_BASE_DEFAULT_CONFIG["obdiag"]["logger"]["log_dir"]),
                const.OBDIAG_BASE_DEFAULT_CONFIG["obdiag"]["logger"]["log_filename"])

    def init(self, args):
        if "c" in args and (getattr(args, "c") is not None):
            self.config_file = os.path.abspath(getattr(args, "c"))
        self.read_config(self.config_file)
        self.read_inner_config(self.inner_config_file)
        self.init_basic_config()
        if self.inner_config.get("obdiag") is not None and self.inner_config.get("obdiag").get(
                "basic") is not None and self.inner_config.get("obdiag").get("basic").get(
            "telemetry") is not None and self.inner_config.get("obdiag").get("basic").get("telemetry") is False:
            telemetry.work_tag = False
        if ("gather_log" in args) or ("gather_obstack" in args) or ("gather_perf" in args) or (
                "gather_clog" in args) or ("gather_slog" in args) or ("analyze_log" in args):
            self.init_obcluster_config()
            return self.init_observer_node_config()
        elif "gather_awr" in args:
            return self.init_ocp_config()
        elif "gather_obproxy_log" in args:
            self.init_obproxy_config()
            return self.init_obproxy_node_config()
        elif ("analyze_flt_trace" in args) or ("gather_sysstat" in args):
            self.init_obcluster_config()
            sucess_1 = self.init_observer_node_config()
            self.init_obproxy_config()
            sucess_2 = self.init_obproxy_node_config()
            return sucess_1 or sucess_2
        elif "check" in args:
            self.init_obcluster_config()
            sucess_1 = self.init_observer_node_config()
            self.init_obproxy_config()
            sucess_2 = self.init_obproxy_node_config()
            sucess_3 = self.init_checker_config()
            return sucess_3 and (sucess_1 or sucess_2)
        elif "gather_plan_monitor" in args:
            return self.init_obcluster_config()

    def init_observer_node_config(self):
        try:
            observer_nodes = []
            if self.ob_cluster is not None:
                ob_cluster = self.ob_cluster
                cluster_name = ob_cluster.get("ob_cluster_name")
                db_host = ob_cluster.get("db_host")

                db_port = get_conf_data_str(ob_cluster.get("db_port"),2881)

                ob_servers = ob_cluster.get("servers")
                global_values = ob_servers.get("global")

                global_ssh_user_name = get_conf_data_str(global_values.get("ssh_username"), "root")

                global_ssh_password = get_conf_data_str(global_values.get("ssh_password"), "")

                global_ssh_port = get_conf_data_str(global_values.get("ssh_port"), 22)

                global_ssh_type = get_conf_data_str(global_values.get("ssh_type"), "remote")

                global_container_name = get_conf_data_str(global_values.get("container_name"), "")

                global_home_path = get_conf_data_str(global_values.get("home_path"), const.OB_INSTALL_DIR_DEFAULT)

                global_ssh_key_file = get_conf_data_str(global_values.get("ssh_key_file"), "")

                global_data_dir = get_conf_data_str(global_values.get("data_dir"),
                                                    os.path.join(global_home_path, "store").strip())

                global_redo_dir = get_conf_data_str(global_values.get("redo_dir"), global_data_dir)

                global_node_ip = global_values.get("ip")
                nodes = ob_servers.get("nodes")
                for node in nodes:
                    node_config = {}
                    node_config["cluster_name"] = cluster_name
                    node_config["host_type"] = "OBSERVER"
                    node_config["db_host"] = db_host
                    node_config["db_port"] = db_port
                    node_config["ip"] = get_conf_data_str(node.get("ip"), global_node_ip)
                    node_config["port"] = get_conf_data_str(node.get("port"), global_ssh_port)
                    node_config["home_path"] = get_conf_data_str(node.get("home_path"), global_home_path)
                    node_config["user"] = get_conf_data_str(node.get("ssh_username"), global_ssh_user_name)
                    node_config["password"] = get_conf_data_str(node.get("ssh_password"), global_ssh_password)
                    node_config["private_key"] = get_conf_data_str(node.get("ssh_key_file"), global_ssh_key_file)
                    node_config["data_dir"] = get_conf_data_str(node.get("data_dir"), global_data_dir)
                    node_config["redo_dir"] = get_conf_data_str(node.get("redo_dir"), global_redo_dir)
                    node_config["ssh_type"] = get_conf_data_str(node.get("ssh_type"), global_ssh_type)
                    node_config["container_name"] = get_conf_data_str(node.get("container_name"), global_container_name)
                    observer_nodes.append(node_config)
            else:
                return False
            self.observer_nodes = observer_nodes
            return True
        except:
            logger.error("observer node config init Failed")
            return False

    def init_obproxy_node_config(self):
        try:
            obproxy_nodes = []
            if self.obproxy_cluster is not None:
                obproxy_cluster = self.obproxy_cluster
                cluster_name = obproxy_cluster.get("obproxy_cluster_name")
                obproxy_servers = obproxy_cluster.get("servers")
                global_values = obproxy_servers.get("global")
                global_ssh_user_name = get_conf_data_str(global_values.get("ssh_username"), "root")
                global_ssh_password = get_conf_data_str(global_values.get("ssh_password"), "")
                global_ssh_port = get_conf_data_str(global_values.get("ssh_port"), 22)
                global_ssh_type = get_conf_data_str(global_values.get("ssh_type"), "remote")
                global_container_name = get_conf_data_str(global_values.get("container_name"), "")
                global_home_path = get_conf_data_str(global_values.get("home_path"), const.OBPROXY_INSTALL_DIR_DEFAULT)
                global_ssh_key_file = get_conf_data_str(global_values.get("ssh_key_file"), "")
                global_data_dir = get_conf_data_str(global_values.get("data_dir"),
                                                    os.path.join(global_home_path, "store").strip())
                global_redo_dir = get_conf_data_str(global_values.get("redo_dir"), global_data_dir)
                global_node_ip = global_values.get("ip")


                nodes = obproxy_servers.get("nodes")
                for node in nodes:
                    node_config = {}
                    node_ip = node.get("ip")
                    if node_ip:
                        node_config["host_type"] = "OBPROXY"
                        node_config["cluster_name"] = cluster_name
                        node_config["ip"] = get_conf_data_str(node.get("ip"), global_node_ip)
                        node_config["port"] = get_conf_data_str(node.get("port"), global_ssh_port)
                        node_config["home_path"] = get_conf_data_str(node.get("home_path"), global_home_path)
                        node_config["user"] = get_conf_data_str(node.get("ssh_username"), global_ssh_user_name)
                        node_config["password"] = get_conf_data_str(node.get("ssh_password"), global_ssh_password)
                        node_config["private_key"] = get_conf_data_str(node.get("ssh_key_file"), global_ssh_key_file)
                        node_config["data_dir"] = get_conf_data_str(node.get("data_dir"), global_data_dir)
                        node_config["redo_dir"] = get_conf_data_str(node.get("redo_dir"), global_redo_dir)
                        node_config["ssh_type"] = get_conf_data_str(node.get("ssh_type"), global_ssh_type)
                        node_config["container_name"] = get_conf_data_str(node.get("container_name"),
                                                                          global_container_name)
                        obproxy_nodes.append(node_config)
                self.obproxy_nodes = obproxy_nodes
                return True
            else:
                return False
        except:
            logger.error("obproxy node config init Failed")
            return False

    def init_basic_config(self):
        try:
            if self.inner_config.get("obdiag"):
                self.basic_config = self.inner_config.get("obdiag").get("basic")
                self.obdiag_log_file = os.path.join(os.path.expanduser(self.inner_config.get("obdiag").get("logger").get("log_dir")),
                                                    self.inner_config.get("obdiag").get("logger").get("log_filename"))
                self.config_file = os.path.expanduser(self.basic_config.get("config_path"))
        except:
            self.basic_config = const.OBDIAG_BASE_DEFAULT_CONFIG["obdiag"]["basic"]
            self.obdiag_log_file = os.path.join(os.path.expanduser(const.OBDIAG_BASE_DEFAULT_CONFIG["obdiag"]["logger"]["log_dir"]),
                                                const.OBDIAG_BASE_DEFAULT_CONFIG["obdiag"]["logger"]["log_filename"])

    def init_ocp_config(self):
        try:
            ocp = self.config.get("ocp")
            if ocp is not None:
                self.ocp = ocp
                self.ocp_url = self.ocp.get("login").get("url")
                self.ocp_user = self.ocp.get("login").get("user")
                self.ocp_password = self.ocp.get("login").get("password")
                return True
            else:
                return False
        except:
            logger.warning("ocp config init Failed")
            return False

    def init_checker_config(self):
        try:
            check_config = self.inner_config.get("check")
            if check_config is None:
                check_config = const.OBDIAG_CHECK_DEFAULT_CONFIG
            self.check_report_path = check_config["report"]["report_path"]
            self.check_report_type = check_config["report"]["export_type"]
            self.check_case_package_file = check_config["package_file"]
            self.check_tasks_base_path = check_config["tasks_base_path"]
            self.check_ignore_version = check_config["ignore_version"]
            return True
        except:
            logger.error("checker config init Failed")
            return False

    def init_obproxy_config(self):
        if self.config is None:
            logger.error("obproxy config file not found")
            return False
        try:
            config = self.config.get("obproxy")
            if config:
                self.obproxy_cluster = self.config.get("obproxy")
                return True
        except:
            logger.error("obproxy config init Failed")
            return False

    def init_obcluster_config(self):
        if self.config is None:
            logger.error("obcluster config file not found")
            return False
        try:
            config = self.config.get("obcluster")
            if config:
                self.ob_cluster = self.config.get("obcluster")
                return True
        except:
            logger.error("obcluster config init Failed")
            return False

    def read_config(self, config_file):
        if not os.path.exists(config_file):
            os.system(r"touch {}".format(config_file))
        self.config = read_yaml_data(config_file)

    def read_inner_config(self, config_file):
        self.inner_config = read_yaml_data(config_file)

    def obdiag_version(self, args):
        return print_obdiag_version()

    def obdiag_display(self, args):
        trace_id = ""
        if getattr(args, "trace_id") is not None:
            trace_id = getattr(args, "trace_id")[0]
        return get_obdiag_display(self.obdiag_log_file, trace_id)

    def quick_build_configuration(self, args):
        try:
            user = getattr(args, "u")[0]
            password = getattr(args, "p")[0]
            host = getattr(args, "h")[0]
            port = getattr(args, "P")[0]
            config_helper = ConfigHelper(user, password, host, port)
            config_helper.build_configuration(args, self.config_file, INNER_CONFIG_FILE)
        except:
            logger.error("Configuration generation failed")

    def handle_gather_log_command(self, args):
        self.gather_log_handler = GatherLogHandler(self.observer_nodes, self.default_collect_pack_dir,
                                                   self.gather_timestamp,
                                                   self.basic_config)
        return self.gather_log_handler.handle(args)

    def handle_gather_sysstat_command(self, args):
        self.gather_sysstat_handler = GatherOsInfoHandler(self.observer_nodes, self.default_collect_pack_dir,
                                                          self.gather_timestamp, self.basic_config)
        return self.gather_sysstat_handler.handle(args)


    def handle_gather_perf_command(self, args):
        self.gather_perf_handler = GatherPerfHandler(self.observer_nodes, self.default_collect_pack_dir,
                                                     self.gather_timestamp,
                                                     self.basic_config)
        return self.gather_perf_handler.handle(args)

    def handle_gather_clog_command(self, args):
        self.gather_clog_handler = GatherObAdminHandler(self.observer_nodes, self.default_collect_pack_dir,
                                                        self.gather_timestamp, "clog", self.basic_config)
        return self.gather_clog_handler.handle(args)

    def handle_gather_slog_command(self, args):
        self.gather_slog_handler = GatherObAdminHandler(self.observer_nodes, self.default_collect_pack_dir,
                                                        self.gather_timestamp, "slog", self.basic_config)
        return self.gather_slog_handler.handle(args)

    def handle_gather_awr_command(self, args):
        self.gather_awr_handler = GatherAwrHandler(self.ocp, self.default_collect_pack_dir, self.gather_timestamp)
        return self.gather_awr_handler.handle(args)

    def handle_gather_plan_monitor(self, args):
        self.gather_plan_monitor_handler = GatherPlanMonitorHandler(self.ob_cluster, self.default_collect_pack_dir,
                                                                    self.gather_timestamp)
        return self.gather_plan_monitor_handler.handle(args)

    def handle_gather_obproxy_log_command(self, args):
        self.gather_obproxy_log_handler = GatherObProxyLogHandler(self.obproxy_nodes, self.default_collect_pack_dir,
                                                                  self.gather_timestamp, self.basic_config)
        return self.gather_obproxy_log_handler.handle(args)

    @staticmethod
    def handle_password_encrypt(args):
        logger.info("Input password=[{0}], encrypted password=[{1}]".format(args.password[0],
                                                                            base64.b64encode(
                                                                                args.password[0].encode())))
        return

    def handle_analyze_log_command(self, args):
        self.analyze_log_handler = AnalyzeLogHandler(self.observer_nodes, self.default_collect_pack_dir,
                                                     self.gather_timestamp,
                                                     self.basic_config)
        return self.analyze_log_handler.handle(args)

    def handle_analyze_flt_trace_command(self, args):
        nodes = []
        nodes.extend(self.observer_nodes)
        nodes.extend(self.obproxy_nodes)
        nodes.extend(self.other_nodes)
        self.analyze_flt_trace_handler = AnalyzeFltTraceHandler(nodes, self.default_collect_pack_dir)
        return self.analyze_flt_trace_handler.handle(args)

    def handle_check_command(self, args):
        obproxy_check_handler=None
        observer_check_handler= None
        if self.obproxy_cluster is not None:
            obproxy_check_handler = CheckHandler(ignore_version=self.check_ignore_version, cluster=self.obproxy_cluster, nodes=self.obproxy_nodes,
                                              export_report_path=self.check_report_path,
                                              export_report_type=self.check_report_type,
                                              case_package_file=self.check_case_package_file,
                                              tasks_base_path=self.check_tasks_base_path,
                                                 check_target_type="obproxy")
            obproxy_check_handler.handle(args)
            obproxy_check_handler.execute()
        if self.ob_cluster is not None:
            observer_check_handler = CheckHandler(ignore_version=self.check_ignore_version,  cluster=self.ob_cluster,
                                                  nodes=self.observer_nodes,
                                              export_report_path=self.check_report_path,
                                              export_report_type=self.check_report_type,
                                              case_package_file=self.check_case_package_file,
                                              tasks_base_path=self.check_tasks_base_path)
            observer_check_handler.handle(args)
            observer_check_handler.execute()
        if obproxy_check_handler is not None:
            print("Check obproxy finished. For more details, please run cmd '" + Fore.YELLOW + " cat {0} ".format(
                obproxy_check_handler.report.get_report_path()) + Style.RESET_ALL + "'")
        if observer_check_handler is not None:
            print("Check observer finished. For more details, please run cmd'" + Fore.YELLOW + " cat {0} ".format(
                observer_check_handler.report.get_report_path()) + Style.RESET_ALL + "'")

        return


def get_conf_data_str(value, dafult_value):
    if value is None:
        return dafult_value
    elif isinstance(value, str):
        return value.strip()

    return value
