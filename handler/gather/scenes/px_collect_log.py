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
@time: 2024/03/13
@file: px_collect_log.py
@desc:
"""
from handler.gather.gather_log import GatherLogHandler
from common.command import uzip_dir_local, analyze_log_get_sqc_addr, delete_file_in_folder, find_home_path_by_port
from common.ssh import SshHelper
import datetime


class SQLPXCollectLogScene(object):
    def __init__(self, context, scene_name, report_path, task_variable_dict=None, env={}):
        self.context = context
        self.stdio = context.stdio
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.report_path = report_path
        self.env = env
        self.is_ssh = True
        self.scene_name = scene_name
        self.db_conn = {}
        self.trace_id = "FAKE_TRACE_ID"
        self.sql_task_node = []
        self.ob_nodes = self.context.cluster_config['servers']
        self.obproxy_nodes = self.context.obproxy_config['servers']
        self.cluster = self.context.cluster_config
        self.search_time = None

    # 考虑到时间较多，有必要指定时间区间段，便于快速拿到日志
    def execute(self):
        # 1. 获取trace id对应的机器地址
        if self.__parse_env():
            # 2. 收集对应机器的日志信息
            self.__gather_log()
            # 3. 分析日志，提取SQC地址
            self.__analyze_log()
            # 解压日志到一个新的目录
            # 分析日志，提取关键字地址
            # 4. 收集SQC机器的日志
            # 如果存在有效地址，则删除本地被解压的日志和压缩包，重新收集并存储于当前地址
            #    否则不存在，则删除被解压的目录
            if len(self.sql_task_node) != 0:
                self.stdio.verbose("delete file start")
                delete_file_in_folder(False, None, self.report_path, self.stdio)
                self.stdio.verbose("delete file end")
                self.__gather_log()
                uzip_dir_local(self.report_path, self.stdio)

    def __gather_log(self):
        try:
            self.stdio.verbose("gather observer log start, trace id: {0}".format(self.trace_id))
            handler = GatherLogHandler(self.context, gather_pack_dir=self.report_path, is_scene=True)
            self.context.set_variable('filter_nodes_list', self.sql_task_node)
            self.context.set_variable('gather_grep', self.trace_id)
            self.context.set_variable('gather_mode', 'trace_id_log')
            from_time_str = (self.search_time - datetime.timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
            to_time_str = (self.search_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.context.set_variable("gather_from", from_time_str)
            self.context.set_variable("gather_to", to_time_str)
            handler.handle()
            self.stdio.verbose("gather observer log end")
        except Exception as e:
            self.stdio.error("gather observer log failed, error: {0}".format(e))
            raise Exception("gather observer log failed, error: {0}".format(e))

    def __analyze_log(self):
        try:
            self.stdio.verbose("analyze observer log start")
            uzip_dir = self.report_path
            uzip_dir_local(uzip_dir, self.stdio)
            ip_port_str = analyze_log_get_sqc_addr(uzip_dir, self.stdio)
            if ip_port_str is None or len(ip_port_str) == 0:
                self.stdio.warn("No logs were found indicating that the SQC interrupted the QC; the error occurred locally in the QC.")
                self.sql_task_node = []
                return
            self.stdio.verbose("find sqc ip_port_str: {0}".format(ip_port_str))
            ip_str, internal_port_str = ip_port_str.split(":")
            home_path_str = self.__find_home_path_by_port(ip_str, internal_port_str)
            sqc_sql_task_node = []
            for node in self.ob_nodes:
                if node["ip"] == ip_str and node["home_path"] == home_path_str:
                    sqc_sql_task_node.append(node)
                    break
            if self.sql_task_node == sqc_sql_task_node:
                self.stdio.verbose("The SQC with an error occurred on the same observer as the QC.")
                self.sql_task_node = []
            else:
                self.stdio.verbose("The SQC with an error info: {0}".format(sqc_sql_task_node))
                self.sql_task_node = sqc_sql_task_node
            self.stdio.verbose("analyze observer log end")
        except Exception as e:
            self.stdio.exception("analyze observer log failed, error: {0}".format(e))
            raise Exception("analyze observer log failed, error: {0}".format(e))

    def __find_home_path_by_port(self, ip_str, internal_port_str):
        for node in self.ob_nodes:
            if node["ip"] == ip_str:
                remote_ip = node.get("ip")
                remote_user = node.get("ssh_username")
                remote_password = node.get("ssh_password")
                remote_port = node.get("ssh_port")
                remote_private_key = node.get("ssh_key_file")
                try:
                    ssh = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key, node, self.stdio)
                except Exception as e:
                    self.stdio.error("ssh {0}@{1}: failed, Please check the config".format(remote_user, remote_ip))
                return find_home_path_by_port(True, ssh, internal_port_str, self.stdio)

    def parse_trace_id(self, trace_id):
        id_ = trace_id.split('-')[0].split('Y')[1]
        uval = int(id_, 16)
        ip = uval & 0xFFFFFFFF
        port = (uval >> 32) & 0xFFFF
        ip_str = "{}.{}.{}.{}".format((ip >> 24) & 0xFF, (ip >> 16) & 0xFF, (ip >> 8) & 0xFF, ip & 0xFF)
        origin_ip_port = "{}:{}".format(ip_str, port)
        return origin_ip_port

    def parse_trace_id2(self, trace_id):
        parts = trace_id.split('-')
        processed_parts = []
        for idx, t in enumerate(parts):
            v = int(t)
            n = hex(v)[2:]
            if idx == 1:
                n = n.zfill(16)
            processed_parts.append(n.upper())
        s = 'Y' + '-'.join(processed_parts)
        origin_ip_port2 = self.parse_trace_id(s)
        return origin_ip_port2

    def analyze_traceid(self, trace_id):
        if len(trace_id) < 50:
            if trace_id[0] == 'Y':
                return self.parse_trace_id(trace_id)
            else:
                return self.parse_trace_id2(trace_id)
        else:
            raise Exception("Trace_id({0}) error!".format(trace_id))

    def __parse_env(self):
        try:
            trace_id = self.env.get("trace_id")
            if trace_id:
                self.trace_id = self.env.get("trace_id")
            else:
                self.stdio.error("option env [--trace_id] not found, please run 'obdiag gather scene list' to check usage")
                return False
            search_time = self.env.get("estimated_time")
            if search_time is None or len(search_time) == 0:
                search_time = datetime.datetime.now()
            else:
                try:
                    search_time = datetime.datetime.strptime(search_time, "%Y-%m-%d %H:%M:%S")
                except Exception as e:
                    self.stdio.error("option env [--estimated_time] format error, please use '%Y-%m-%d %H:%M:%S', run 'obdiag gather scene list' to check usage")
                    return False
            self.search_time = search_time
            self.stdio.verbose("QC addr analyze begin {0}".format(trace_id))
            ip_port_str = self.analyze_traceid(trace_id)
            self.stdio.verbose("analyze text: {0}".format(ip_port_str))
            ip_str, internal_port_str = ip_port_str.split(':')
            home_path_str = self.__find_home_path_by_port(ip_str, internal_port_str)
            for node in self.ob_nodes:
                if node["ip"] == ip_str and node["home_path"] == home_path_str:
                    self.sql_task_node.append(node)
                    break
            self.stdio.verbose("QC addr analyze end {0}".format(self.sql_task_node))
            return True
        except Exception as e:
            self.stdio.error("Parse env fail. Exception : {0} .".format(e))
