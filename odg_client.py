#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@file: odg_client.py
@desc:
"""
from handler.shell_handler.gather_log_handler import GatherLogHandler
from ocp.config_helper import ConfigHelper
import base64
import os
import json
from common.odg_exception import ODGConfNotFoundException
from common.logger import logger
from utils.time_utils import get_current_us_timestamp

OCP_CONFIG_FILE = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "./conf")), "ocp_config.json")
NODE_CONFIG_FILE = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "./conf")), "node_config.json")

if not os.path.exists(OCP_CONFIG_FILE):
    raise ODGConfNotFoundException("OCP Conf file not found at:\n{0}".format("\n".join("./conf/ocp_config.json")))
if not os.path.exists(NODE_CONFIG_FILE):
    raise ODGConfNotFoundException("Node Conf file not found at:\n{0}".format("\n".join("./conf/node_config.json")))


class ODGClient(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(ODGClient, cls).__new__(cls, *args, **kwargs)
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
            # params
            self.default_collect_pack_dir = ""
            self.cluster = ""
            # ob
            self.ob_log_dir = None

    def init(self):
        self.read_ocp_config(OCP_CONFIG_FILE)
        self.read_node_config(NODE_CONFIG_FILE)
        self.gather_log_handler = GatherLogHandler(self.nodes, self.default_collect_pack_dir, self.ob_log_dir, self.gather_timestamp)
        return self

    def read_node_config(self, config_file):
        self.config = json.load(open(config_file))
        self.nodes = self.config["nodes"]

    def read_ocp_config(self, config_file):
        self.config = json.load(open(config_file))
        self.ocp = self.config["ocp"]
        self.ocp_url = self.config["ocp"]["url"]
        self.ocp_user = self.config["ocp"]["user"]
        self.ocp_password = self.config["ocp"]["password"]
        self.ocp_is_exits = self.config["ocp"]["is_exits"]
        self.ocp_metadb_ip = self.config["ocp_metadb"]["ip"]
        self.ocp_metadb_port = self.config["ocp_metadb"]["port"]
        self.ocp_metadb_name = self.config["ocp_metadb"]["database"]
        self.ocp_metadb_user = self.config["ocp_metadb"]["user"]
        self.ocp_metadb_password = self.config["ocp_metadb"]["password"]

    def quick_build_configuration(self, args):
        config_helper = ConfigHelper(self.ocp_url, self.ocp_user, self.ocp_password,
                                     self.ocp_metadb_ip, self.ocp_metadb_port,
                                     self.ocp_metadb_user, self.ocp_metadb_password,
                                     self.ocp_metadb_name)
        return config_helper.build_configuration(args, NODE_CONFIG_FILE)

    def handle_gather_log_command(self, args):
        return self.gather_log_handler.handle(args)

    @staticmethod
    def handle_password_encrypt(args):
        logger.info("Input password=[{0}], encrypted password=[{1}]".format(args.password[0],
                                                                            base64.b64encode(
                                                                                args.password[0].encode())))
        return
