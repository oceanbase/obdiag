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
from handler.analyzer.analyze_log import AnalyzeLogHandler
from handler.gather.gather_log import GatherLogHandler
from handler.gather.gather_awr import GatherAwrHandler
from handler.gather.gather_obproxy_log import GatherObProxyLogHandler
from handler.gather.gather_sysstat import GatherOsInfoHandler
from handler.gather.gather_obadmin import GatherObAdminHandler
from handler.gather.gather_perf import GatherPerfHandler
from handler.gather.gather_plan_monitor import GatherPlanMonitorHandler
from ocp.config_helper import ConfigHelper
import base64
import os
from common.obdiag_exception import OBDIAGConfNotFoundException
from common.logger import logger
from utils.time_utils import get_current_us_timestamp
from utils.yaml_utils import read_yaml_data
from utils.version_utils import get_obdiag_version

CONFIG_FILE = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "./conf")), "config.yml")

if not os.path.exists(CONFIG_FILE):
    raise OBDIAGConfNotFoundException("Conf file not found at:\n{0}".format("\n".join("./conf/config.yml")))


class OBDIAGClient(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(OBDIAGClient, cls).__new__(cls, *args, **kwargs)
            cls._inited = False
        return cls._inst

    def __init__(self):
        if not self._inited:
            self.gather_timestamp = get_current_us_timestamp()
            self.config = None
            self.nodes = []
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
            self.gather_perf_handler = None
            self.gather_clog_handler = None
            self.gather_slog_handler = None
            self.gather_plan_monitor_handler = None
            self.gather_obproxy_log_handler = None
            # analyze handler
            self.analyze_log_handler = None
            # params
            self.default_collect_pack_dir = ""
            self.cluster = ""
            # ob
            self.ob_install_dir = None
            self.ob_cluster = None
            self.ob_cluster_user = None
            self.ob_cluster_password = None
            self.ob_cluster_ip = None
            self.ob_cluster_port = None
            self.ob_cluster_name = None
            # obdiag basic config
            self.basic_config = None

    def init(self):
        self.read_config(CONFIG_FILE)
        return self

    def read_node_config(self, config_file):
        self.config = read_yaml_data(config_file)
        self.nodes = self.config["nodes"]

    def read_config(self, config_file):
        self.config = read_yaml_data(config_file)
        self.ocp = self.config["OCP"]
        self.ocp_url = self.ocp["LOGIN"]["url"]
        self.ocp_user = self.ocp["LOGIN"]["user"]
        self.ocp_password = self.ocp["LOGIN"]["password"]
        # metadb
        self.ocp_metadb_ip = self.ocp["METADB"]["ip"]
        self.ocp_metadb_port = self.ocp["METADB"]["port"]
        self.ocp_metadb_name = self.ocp["METADB"]["dbname"]
        self.ocp_metadb_user = self.ocp["METADB"]["user"]
        self.ocp_metadb_password = self.ocp["METADB"]["password"]
        # obcluster
        self.ob_cluster = self.config["OBCLUSTER"]
        # nodes
        self.nodes = self.config["NODES"]
        # obdiag basic config
        self.basic_config = self.config["OBDIAG"]["BASIC"]
        self.obdiag_log_file = os.path.join(self.config["OBDIAG"]["LOGGER"]["log_dir"], self.config["OBDIAG"]["LOGGER"]["log_filename"])

    def obdiag_version(self, args):
        return get_obdiag_version()

    def obdiag_display(self, args):
        trace_id = ""
        if getattr(args, "trace_id") is not None:
            trace_id = getattr(args, "trace_id")[0]
        return get_obdiag_display(self.obdiag_log_file, trace_id)

    def quick_build_configuration(self, args):
        config_helper = ConfigHelper(self.ocp_url, self.ocp_user, self.ocp_password,
                                     self.ocp_metadb_ip, self.ocp_metadb_port,
                                     self.ocp_metadb_user, self.ocp_metadb_password,
                                     self.ocp_metadb_name)
        return config_helper.build_configuration(args, CONFIG_FILE)

    def handle_gather_log_command(self, args):
        self.gather_log_handler = GatherLogHandler(self.nodes, self.default_collect_pack_dir, self.gather_timestamp, self.basic_config)
        return self.gather_log_handler.handle(args)

    def handle_gather_sysstat_command(self, args):
        self.gather_sysstat_handler = GatherOsInfoHandler(self.nodes, self.default_collect_pack_dir, self.gather_timestamp,self.basic_config)
        return self.gather_sysstat_handler.handle(args)

    def handle_gather_perf_command(self, args):
        self.gather_perf_handler = GatherPerfHandler(self.nodes, self.default_collect_pack_dir, self.gather_timestamp, self.basic_config)
        return self.gather_perf_handler.handle(args)

    def handle_gather_clog_command(self, args):
        self.gather_clog_handler = GatherObAdminHandler(self.nodes, self.default_collect_pack_dir,
                                                        self.gather_timestamp, "clog", self.basic_config)
        return self.gather_clog_handler.handle(args)

    def handle_gather_slog_command(self, args):
        self.gather_slog_handler = GatherObAdminHandler(self.nodes, self.default_collect_pack_dir,
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
        self.gather_obproxy_log_handler = GatherObProxyLogHandler(self.nodes, self.default_collect_pack_dir, self.gather_timestamp, self.basic_config)
        return self.gather_obproxy_log_handler.handle(args)

    @staticmethod
    def handle_password_encrypt(args):
        logger.info("Input password=[{0}], encrypted password=[{1}]".format(args.password[0],
                                                                            base64.b64encode(
                                                                                args.password[0].encode())))
        return

    def handle_analyze_log_command(self, args):
        self.analyze_log_handler = AnalyzeLogHandler(self.nodes, self.default_collect_pack_dir, self.gather_timestamp, self.basic_config)
        return self.analyze_log_handler.handle(args)
