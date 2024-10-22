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

from stdio import SafeStdio
from handler.gather.gather_log import GatherLogHandler
from handler.gather.gather_obproxy_log import GatherObProxyLogHandler
from handler.gather.gather_plan_monitor import GatherPlanMonitorHandler
from common.tool import StringUtils
from common.ssh_client.ssh import SshClient
from common.command import find_home_path_by_port


class SQLProblemScene(SafeStdio):
    def __init__(self, context, scene_name, report_path, task_variable_dict=None, env={}):
        self.context = context
        self.stdio = context.stdio
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.ob_nodes = self.context.cluster_config['servers']
        self.obproxy_nodes = self.context.obproxy_config['servers']
        self.cluster = self.context.cluster_config
        self.report_path = report_path
        self.env = env
        self.is_ssh = True
        self.scene_name = scene_name
        self.db_conn = {}
        self.trace_id = "FAKE_TRACE_ID"
        self.task_nodes = []

    def execute(self):
        skip_type = self.context.get_variable("gather_skip_type", None)
        if skip_type:
            self.stdio.verbose("needs to be filtered out and not gather type is {0}".format(skip_type))
        if self.__parse_env():
            if skip_type != "ssh":
                self.__gather_log()
                self.__gather_obproxy_log()
            if skip_type != "sql":
                self.__gather_sql_info()

    def __find_home_path_by_port(self, ip_str, internal_port_str):
        for node in self.ob_nodes:
            if node.get("ip") == ip_str:
                remote_ip = node.get("ip")
                remote_user = node.get("ssh_username")
                try:
                    ssh_client = SshClient(self.context, node)
                    return find_home_path_by_port(ssh_client, internal_port_str, self.stdio)
                except Exception as e:
                    self.stdio.error("ssh {0}@{1}: failed, Please check the config".format(remote_user, remote_ip))

    def __gather_log(self):
        try:
            ip_port_str = StringUtils.get_observer_ip_port_from_trace_id(self.trace_id)
            ip_str, internal_port_str = ip_port_str.split(':')
            home_path_str = self.__find_home_path_by_port(ip_str, internal_port_str)
            for node in self.ob_nodes:
                if node.get("ip") == ip_str and node.get("home_path") == home_path_str:
                    self.task_nodes.append(node)
                    break
            self.stdio.verbose("gather observer log start")
            handler = GatherLogHandler(self.context, self.report_path, is_scene=True)
            self.context.set_variable('filter_nodes_list', self.task_nodes)
            self.context.set_variable('gather_grep', self.trace_id)
            handler.handle()
            self.stdio.verbose("gather observer log end")
        except Exception as e:
            self.stdio.error("gather observer log failed, error: {0}".format(e))
            raise Exception("gather observer log failed, error: {0}".format(e))

    def __gather_obproxy_log(self):
        try:
            self.stdio.verbose("gather obproxy log start")
            handler = GatherObProxyLogHandler(self.context, gather_pack_dir=self.report_path, is_scene=True)
            if self.scene_name:
                if self.scene_name == "observer.sql_err" or self.scene_name == "observer.perf_sql":
                    self.context.set_variable('gather_grep', self.trace_id)
                else:
                    self.stdio.warn("unsupported scene {0}".format(self.scene_name))
                    return
                handler.handle()
                self.stdio.verbose("gather obproxy log end")
            else:
                self.stdio.warn("scene is None")
                return
        except Exception as e:
            self.stdio.error("gather obproxy log failed, error: {0}".format(e))
            raise Exception("gather obproxy log failed, error: {0}".format(e))

    def __gather_sql_info(self):
        try:
            self.stdio.verbose("gather sql info start")
            self.stdio.verbose("gather sql info set_variable, key: gather_plan_monitor_trace_id, value:{0}".format(self.trace_id))
            self.context.set_variable('gather_plan_monitor_trace_id', self.trace_id)
            handler = GatherPlanMonitorHandler(self.context, gather_pack_dir=self.report_path, is_scene=True)
            handler.handle()
            self.stdio.verbose("gather sql info end")
        except Exception as e:
            self.stdio.error("gather sql info failed, error: {0}".format(e))
            raise Exception("gather sql info failed, error: {0}".format(e))

    def report(self):
        pass

    def __parse_env(self):
        if self.env:
            cli_connection_string = self.env.get("db_connect")
            self.db_conn = StringUtils.parse_mysql_conn(cli_connection_string)
            trace_id = self.env.get("trace_id")
            if trace_id:
                self.trace_id = self.env.get("trace_id")
                return True
            else:
                self.stdio.error("option env [--trace_id] not found, please run 'obdiag gather scene list' to check usage")
                return False
        else:
            self.stdio.error("option env not found, please run 'obdiag gather scene list' to check usage")
            return False
