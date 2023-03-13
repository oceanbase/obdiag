#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@file: obdiag_client.py
@desc:
"""
from handler.shell_handler.gather_log_handler import GatherLogHandler
from handler.http_handler.gather_awr_handler import GatherAwrHandler
from handler.shell_handler.gather_obproxy_log_handler import GatherObProxyLogHandler
from handler.shell_handler.gather_sysstat_handler import GatherOsInfoHandler
from handler.shell_handler.gather_obadmin_handler import GatherObAdminHandler
from handler.shell_handler.gather_perf_hander import GatherPerfHandler
from handler.sql_handler.gather_plan_monitor_handler import GatherPlanMonitorHandler
from ocp.config_helper import ConfigHelper
import base64
import os
from common.obdiag_exception import OBDIAGConfNotFoundException
from common.logger import logger
from utils.time_utils import get_current_us_timestamp
from utils.yaml_utils import read_yaml_data

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
            # handler
            self.gather_awr_handler = None
            self.gather_log_handler = None
            self.gather_sysstat_handler = None
            self.gather_perf_handler = None
            self.gather_clog_handler = None
            self.gather_slog_handler = None
            self.gather_plan_monitor_handler = None
            self.gather_obproxy_log_handler = None
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
