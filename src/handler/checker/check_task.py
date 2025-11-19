#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
@time: 2023/9/26
@file: check_task.py
@desc:
"""
import threading

from src.common.command import get_observer_version, get_obproxy_version, get_obproxy_full_version
from src.common.ob_connector import OBConnector
from src.common.ssh_client.ssh import SshClient
from src.handler.checker.check_exception import StepResultFailException, StepExecuteFailException, StepResultFalseException, TaskException
from src.handler.checker.step.stepbase import StepBase
from src.common.tool import StringUtils
from src.common.scene import filter_by_version


class Task:
    def __init__(self, context, task, nodes, cluster, report, task_variable_dict=None):
        super().__init__()
        self.context = context
        self.stdio = context.stdio
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.task = task
        self.cluster = cluster
        self.nodes = nodes
        self.report = report

    def execute(self):
        self.stdio.verbose("task_base execute")
        if len(self.task) > 0 and self.task[0].get("task_type") and self.task[0]["task_type"] == "py":
            module = self.task[0]["module"]
            module.init(self.context, self.report)
            module.execute()
            return
        if self.cluster.get("version") is None:
            steps_nu = 0
        else:
            steps_nu = filter_by_version(self.task, self.cluster, self.stdio)
        if steps_nu < 0:
            self.stdio.verbose("Unadapted by version. SKIP")
            return "Unadapted by version.SKIP"
        self.stdio.verbose("filter_by_version is return {0}".format(steps_nu))
        if len(self.nodes) == 0:
            raise Exception("node is not exist")
        work_threads = []
        for node in self.nodes:
            t = threading.Thread(target=self.execute_one_node, args=(steps_nu, node))
            work_threads.append(t)
            t.start()
        for t in work_threads:
            t.join()

        self.stdio.verbose("task execute end")

    def execute_one_node(self, steps_nu, node):
        try:
            self.stdio.verbose("run task in node: {0}".format(StringUtils.node_cut_passwd_for_log(node)))
            steps = self.task[steps_nu]
            nu = 1
            task_variable_dict = {}
            for step in steps["steps"]:
                try:
                    self.stdio.verbose("step nu: {0}".format(nu))
                    if len(self.cluster) == 0:
                        raise Exception("cluster is not exist")
                    step_run = StepBase(self.context, step, node, self.cluster, task_variable_dict)
                    self.stdio.verbose("step nu: {0} initted, to execute".format(nu))
                    step_run.execute(self.report)
                    task_variable_dict = step_run.update_task_variable_dict()
                    if "report_type" in step["result"] and step["result"]["report_type"] == "execution":
                        self.stdio.verbose("report_type stop this step")
                        return
                except StepExecuteFailException as e:
                    self.stdio.error("Task execute CheckStepFailException: {0} . Do Next Task".format(e))
                    return
                except StepResultFalseException as e:
                    self.stdio.warn("Task execute StepResultFalseException: {0} .".format(e))
                    continue
                except StepResultFailException as e:
                    self.stdio.warn("Task execute StepResultFailException: {0}".format(e))
                    return
                except Exception as e:
                    self.stdio.error("Task execute Exception: {0}".format(e))
                    raise TaskException("Task execute Exception:  {0}".format(e))

                self.stdio.verbose("step nu: {0} execute end ".format(nu))
                nu = nu + 1
        except Exception as e:
            self.stdio.error("Task execute Exception: {0}".format(e))
            raise e


# for python task
class TaskBase:
    def __init__(self):
        self.work_path = None
        self.gather_log = None
        self.stdio = None
        self.input_parameters = None
        self.ob_cluster = None
        self.ob_connector = None
        self.store_dir = None
        self.obproxy_version = None
        self.obproxy_full_version = None
        self.observer_version = None
        self.report = None
        self.obproxy_nodes = []
        self.observer_nodes = []
        self.oms_nodes = []
        self.context = None
        self.name = type(self).__name__
        self.Result = None

    def init(self, context, report):
        self.report = report
        self.context = context
        self.stdio = context.stdio
        # get ob_cluster
        self.ob_cluster = self.context.cluster_config
        # set report
        self.report = report
        # get obproxy_nodes and observer_nodes
        observer_nodes = self.context.cluster_config.get("servers")
        if observer_nodes:
            for node in observer_nodes:
                # add ssher
                ssher = None
                try:
                    ssher = SshClient(context, node)
                except Exception as e:
                    self.stdio.warn("StepBase get SshHelper fail on{0} ,Exception: {1}".format(node.get("ip"), e))
                node["ssher"] = ssher
                self.observer_nodes.append(node)
        obproxy_nodes = self.context.obproxy_config.get("servers")
        if obproxy_nodes:
            for node in obproxy_nodes:
                # add ssher
                ssher = None
                try:
                    ssher = SshClient(context, node)
                except Exception as e:
                    self.stdio.warn("StepBase get SshHelper fail on{0} ,Exception: {1}".format(node.get("ip"), e))
                node["ssher"] = ssher
                self.obproxy_nodes.append(node)

        # build observer_version by sql or ssher. If using SSHer, the observer_version is set to node[0].
        self.observer_version = ""
        try:
            self.observer_version = get_observer_version(self.context)
        except Exception as e:
            self.stdio.error("get observer_version fail: {0}".format(e))
        self.ob_connector = OBConnector(
            context=self.context,
            ip=self.ob_cluster.get("db_host"),
            port=self.ob_cluster.get("db_port"),
            username=self.ob_cluster.get("tenant_sys").get("user"),
            password=self.ob_cluster.get("tenant_sys").get("password"),
            timeout=10000,
        )
        try:
            self.obproxy_version = get_obproxy_version(self.context)
        except Exception as e:
            self.stdio.error("get obproxy_version fail: {0}".format(e))
        try:
            self.obproxy_full_version = get_obproxy_full_version(self.context)
        except Exception as e:
            self.stdio.error("get obproxy_full_version fail: {0}".format(e))

    def check_ob_version_min(self, min_version: str):
        if self.observer_version is None:
            return False
        if self.observer_version == min_version:
            return True
        if StringUtils.compare_versions_greater(self.observer_version, min_version):
            return True
        else:
            return False

    def check_obproxy_version_min(self, min_version):
        if self.obproxy_version is None:
            return False
        if self.obproxy_version == min_version:
            return True
        if StringUtils.compare_versions_greater(self.obproxy_version, min_version):
            return True
        else:
            return False

    def check_ob_version_max(self, max_version):
        if self.observer_version is None:
            return False
        if self.observer_version == max_version:
            return True
        if StringUtils.compare_versions_greater(max_version, self.observer_version):
            return True
        else:
            return False

    def get_obproxy_parameter(self, parameter_name):
        try:
            sql = "show proxyconfig like '{0}';".format(parameter_name)
            data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            return data
        except Exception as e:
            self.stdio.error("get {0} fail:{1} .please check".format(parameter_name, e))
            return []

    def check_command_exist(self, ssh_client, command):
        if ssh_client is None:
            return False
        try:
            result = ssh_client.exec_cmd("command -v " + command)
            if result is None or len(result) == 0:
                return False
            return True
        except Exception as e:
            self.stdio.error("check_command_exist error: {0}".format(e))
            return False

    def get_system_parameter(self, ssh_client, parameter_name):
        try:
            parameter_name = parameter_name.replace(".", "/")
            # check parameter_name is exist
            if ssh_client.exec_cmd('find /proc/sys/ -name "{0}"'.format(parameter_name.split("/")[-1])) == "":
                self.stdio.warn("{0} is not exist".format(parameter_name))
                return None
            parameter_value = ssh_client.exec_cmd("cat /proc/sys/" + parameter_name).strip()
            return parameter_value
        except Exception as e:
            self.stdio.warn("get {0} fail:{1} .please check, the parameter_value will be set -1".format(parameter_name, e))
