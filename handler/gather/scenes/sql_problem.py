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
@time: 2024/01/17
@file: sql_problem.py
@desc:
"""

from common.logger import logger
from utils.parser_utils import ParserAction
from handler.gather.gather_log import GatherLogHandler
from handler.gather.gather_obproxy_log import GatherObProxyLogHandler
from handler.gather.gather_plan_monitor import GatherPlanMonitorHandler
from utils.string_utils import parse_mysql_cli_connection_string


class SQLProblemScene(object):
    def __init__(self, scene_name, ob_nodes, obproxy_nodes, cluster, report_path, task_variable_dict=None, args=None, env={}):
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.ob_nodes = ob_nodes
        self.obproxy_nodes = obproxy_nodes
        self.cluster = cluster
        self.report_path = report_path
        self.args = args
        self.env = env
        self.is_ssh = True
        self.scene_name = scene_name
        self.db_conn = {}
        self.trace_id = "FAKE_TRACE_ID"

    def execute(self):
        self.__parse_env()
        self.__gather_log()
        self.__gather_obproxy_log()
        self.__gather_sql_info()

    def __gather_log(self):
        try:
            logger.info("gather observer log start")
            handler = GatherLogHandler(nodes=self.ob_nodes, gather_pack_dir=self.report_path, is_scene=True)
            self.args = ParserAction.add_attribute_to_namespace(self.args, 'grep', "")
            handler.handle(self.args)
            logger.info("gather observer log end")
        except Exception as e:
            logger.error("gather observer log failed, error: {0}".format(e))
            raise Exception("gather observer log failed, error: {0}".format(e))

    def __gather_obproxy_log(self):
        try:
            logger.info("gather obproxy log start")
            handler = GatherObProxyLogHandler(nodes=self.obproxy_nodes, gather_pack_dir=self.report_path, is_scene=True)
            if self.scene_name:
                if self.scene_name ==  "observer.sql_err":
                    self.args = ParserAction.add_attribute_to_namespace(self.args, 'grep', None)
                elif self.scene_name ==  "observer.perf_sql":
                    self.args = ParserAction.add_attribute_to_namespace(self.args, 'grep', self.trace_id)
                else:
                    logger.warn("unsupported scene {0}".format(self.scene_name))
                    return
                self.args = ParserAction.add_attribute_to_namespace(self.args, 'scope', "all")
                self.args = ParserAction.add_attribute_to_namespace(self.args, 'encrypt', "false")
                handler.handle(self.args)
                logger.info("gather obproxy log end")
            else:
                logger.warn("scene is None")
                return
        except Exception as e:
            logger.error("gather obproxy log failed, error: {0}".format(e))
            raise Exception("gather obproxy log failed, error: {0}".format(e))

    def __gather_sql_info(self):
        try:
            logger.info("gather sql info start")
            handler = GatherPlanMonitorHandler(ob_cluster=self.cluster, gather_pack_dir=self.report_path, db_conn=self.db_conn, is_scene=True)
            self.args = ParserAction.add_attribute_to_namespace(self.args, 'trace_id', self.trace_id)
            handler.handle(self.args)
            logger.info("gather sql info end")
        except Exception as e:
            logger.error("gather sql info failed, error: {0}".format(e))
            raise Exception("gather sql info failed, error: {0}".format(e))

    def report(self):
        pass

    def __parse_env(self):
        cli_connection_string = self.env.get("db_connect")
        self.db_conn = parse_mysql_cli_connection_string(cli_connection_string)
        trace_id = self.env.get("trace_id")
        if trace_id:
            self.trace_id = self.env.get("trace_id")
