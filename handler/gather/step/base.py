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
@time: 2024/01/05
@file: base.py
@desc:
"""
from common.ssh_client.ssh import SshClient
from common.stdio import SafeStdio
from handler.gather.step.ssh import SshHandler
from handler.gather.step.sql import StepSQLHandler
from handler.gather.gather_log import GatherLogHandler
from handler.gather.gather_obproxy_log import GatherObProxyLogHandler
from handler.gather.gather_sysstat import GatherOsInfoHandler


class Base(SafeStdio):
    def __init__(self, context, step, node, cluster, report_path, task_variable_dict=None, env={}, node_number=1):
        self.context = context
        self.stdio = context.stdio
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.step = step
        self.node = node
        self.cluster = cluster
        self.report_path = report_path
        self.env = env
        self.node_number = node_number

    def execute(self):
        self.stdio.verbose("step: {0}".format(self.step))
        no_cluster_name_msg = "(Please set ob_cluster_name or obproxy_cluster_name)"
        try:
            if "ip" in self.node:
                self.task_variable_dict["remote_ip"] = self.node["ip"]
            elif "ssh_type" in self.node and self.node["ssh_type"] == "docker":
                self.stdio.verbose("execute ssh_type is docker")
                ssh_client = SshClient(self.context, self.node)
                self.task_variable_dict["remote_ip"] = ssh_client.get_ip()
            self.task_variable_dict["remote_home_path"] = self.node["home_path"]

            if "type" not in self.step:
                self.stdio.error("Missing field :type")
            skip_type = self.context.get_variable("gather_skip_type", None)
            if skip_type:
                self.stdio.verbose("needs to be filtered out and not gather type is {0}".format(skip_type))
            if (self.node_number > 1) and self.step.get("global") and (self.step.get("global") is True):
                self.stdio.verbose("step sets the value of the global is true and it is processing the {0} node, skipping gather".format(self.node_number))
            else:
                if self.step["type"] == "ssh" and (skip_type != "ssh"):
                    handler = SshHandler(self.context, self.step, self.node, self.report_path, self.task_variable_dict)
                    handler.execute()
                elif self.step["type"] == "sql" and (skip_type != "sql"):
                    handler = StepSQLHandler(self.context, self.step, self.cluster, self.report_path, self.task_variable_dict)
                    handler.execute()
                elif self.step["type"] == "log" and (skip_type != "ssh"):
                    if self.node.get("host_type") and self.node.get("host_type") == "OBSERVER":
                        handler = GatherLogHandler(self.context, gather_pack_dir=self.report_path, is_scene=True)
                        self.context.set_variable('filter_nodes_list', [self.node])
                        self.context.set_variable('gather_grep', self.step.get("grep"))
                        handler.handle()
                    else:
                        self.stdio.verbose("node host_type is {0} not OBSERVER, skipping gather log".format(self.node.get("host_type")))
                elif self.step["type"] == "obproxy_log" and (skip_type != "ssh"):
                    if self.node.get("host_type") and self.node.get("host_type") == "OBPROXY":
                        handler = GatherObProxyLogHandler(self.context, gather_pack_dir=self.report_path, is_scene=True)
                        self.context.set_variable('filter_nodes_list', [self.node])
                        self.context.set_variable('gather_grep', self.step.get("grep"))
                        handler.handle()
                    else:
                        self.stdio.verbose("node host_type is {0} not OBPROXY, skipping gather log".format(self.node.get("host_type")))
                elif self.step["type"] == "sysstat" and (skip_type != "ssh"):
                    handler = GatherOsInfoHandler(self.context, gather_pack_dir=self.report_path, is_scene=True)
                    self.context.set_variable('filter_nodes_list', [self.node])
                    handler.handle()
                else:
                    support_types = ["ssh", "sql", "log", "obproxy_log", "sysstat"]
                    if self.step["type"] not in support_types:
                        self.stdio.error("{0} is an unsupported type. The currently supported types are {1}. {0}".format(self.step["type"], support_types))
        except Exception as e:
            self.stdio.error("StepBase handler.execute fail, error: {0}".format(e))
            if self.step["type"] == "sql":
                self.stdio.error("[cluster:{0}] {1}]".format(self.cluster.get("ob_cluster_name") or self.cluster.get("obproxy_cluster_name") or no_cluster_name_msg, e))
            else:
                self.stdio.error("[{0}:{1}] {2}]".format(self.node.get("ssh_type") or "", self.node.get("container_name") or self.task_variable_dict.get("remote_ip") or "", e))
            self.stdio.error("StepBase handler.execute fail, error: {0}".format(e))

    def update_task_variable_dict(self):
        return self.task_variable_dict
