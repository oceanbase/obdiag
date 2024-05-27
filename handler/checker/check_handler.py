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
@time: 2023/9/26
@file: check_handler.py
@desc:
"""

import os
import queue
import time

import yaml

from common.ob_connector import OBConnector
from common.ssh import SshHelper
from handler.checker.check_exception import CheckException
from handler.checker.check_report import TaskReport, CheckReport, CheckrReportException
from handler.checker.check_task import TaskBase
from common.scene import get_version
import re
from common.tool import Util
from common.tool import YamlUtils
from common.tool import StringUtils


class CheckHandler:

    def __init__(self, context, check_target_type="observer"):
        self.context = context
        self.stdio = context.stdio
        # init input parameters
        self.report = None
        self.tasks = None
        self.work_path = os.path.expanduser(self.context.inner_config["check"]["work_path"] or "~/.obdiag/check")
        self.export_report_path = os.path.expanduser(self.context.inner_config["check"]["report"]["report_path"] or "./check_report/")
        self.export_report_type = self.context.inner_config["check"]["report"]["export_type"] or "table"
        self.ignore_version = self.context.inner_config["check"]["ignore_version"] or False
        self.cluster = self.context.cluster_config
        if check_target_type == "observer":
            self.nodes = self.context.cluster_config.get("servers")
        if check_target_type == "obproxy":
            self.nodes = self.context.obproxy_config.get("servers")
        self.tasks_base_path = os.path.expanduser(self.work_path + "/tasks/")
        self.check_target_type = check_target_type

        self.stdio.verbose(
            "CheckHandler input. ignore_version is {0} , cluster is {1} , nodes is {2}, "
            "export_report_path is {3}, export_report_type is {4} , check_target_type is {5}, "
            " tasks_base_path is {6}.".format(
                self.ignore_version,
                self.cluster.get("ob_cluster_name") or self.cluster.get("obproxy_cluster_name"),
                StringUtils.node_cut_passwd_for_log(self.nodes),
                self.export_report_path,
                self.export_report_type,
                self.check_target_type,
                self.tasks_base_path,
            )
        )

        # case_package_file
        # build case_package_file
        if check_target_type is not None:
            case_package_file = self.work_path + "/" + check_target_type + "_check_package.yaml"
        else:
            raise CheckException("check_target_type is null. Please check the conf")
        case_package_file = os.path.expanduser(case_package_file)
        if os.path.exists(case_package_file):
            self.package_file_name = case_package_file
        else:
            raise CheckException("case_package_file {0} is not exist".format(case_package_file))
        self.stdio.verbose("case_package_file is " + self.package_file_name)
        # checker tasks_base_path
        # build tasks_base_path
        if check_target_type is not None:
            tasks_base_path = self.tasks_base_path + "/" + check_target_type
        else:
            raise CheckException("check_target_type is null. Please check the conf")
        tasks_base_path = os.path.expanduser(tasks_base_path)
        if os.path.exists(tasks_base_path):
            self.tasks_base_path = tasks_base_path
        else:
            raise CheckException("tasks_base_path {0} is not exist".format(tasks_base_path))
        self.stdio.verbose("tasks_base_path is " + self.tasks_base_path)
        # input_param
        self.options = self.context.options

        # add ssher
        new_node = []
        for node in self.nodes:
            # add ssher
            ssher = None
            try:
                ssher = SshHelper(True, node.get("ip"), node.get("ssh_username"), node.get("ssh_password"), node.get("ssh_port"), node.get("ssh_key_file"), node)
            except Exception as e:
                self.stdio.warn("StepBase get SshHelper fail on{0} ,Exception: {1}".format(node.get("ip"), e))
            node["ssher"] = ssher
            new_node.append(node)
        self.nodes = new_node
        self.version = get_version(self.nodes, self.check_target_type, self.cluster, self.stdio)

        # add OBConnectorPool
        try:
            obConnectorPool = checkOBConnectorPool(context, 3, self.cluster)

        except Exception as e:
            self.stdio.warn("obConnector init error. Error info is {0}".format(e))
        finally:
            self.context.set_variable('check_obConnector_pool', obConnectorPool)

    def handle(self):
        try:
            package_name = None
            if self.check_target_type == "obproxy" and Util.get_option(self.options, 'obproxy_cases'):
                package_name = Util.get_option(self.options, 'obproxy_cases')

            if self.check_target_type == "observer" and Util.get_option(self.options, 'cases'):
                package_name = Util.get_option(self.options, 'cases')
            if Util.get_option(self.options, 'store_dir'):
                self.export_report_path = Util.get_option(self.options, 'store_dir')
                self.stdio.verbose("export_report_path change to " + self.export_report_path)
            self.export_report_path = os.path.expanduser(self.export_report_path)
            if not os.path.exists(self.export_report_path):
                self.stdio.warn("{0} not exists. mkdir it!".format(self.export_report_path))
                os.mkdir(self.export_report_path)
            self.stdio.verbose("export_report_path is " + self.export_report_path)

            # get package's by package_name
            self.tasks = {}
            if package_name:
                self.stdio.verbose("package_name is {0}".format(package_name))
                package_tasks_by_name = self.get_package_tasks(package_name)
                self.get_all_tasks()
                end_tasks = {}
                for package_task in package_tasks_by_name:
                    if package_task in self.tasks:
                        end_tasks[package_task] = self.tasks[package_task]
                    for task_name, value in self.tasks.items():
                        if re.match(package_task, task_name):
                            end_tasks[package_task] = self.tasks[task_name]
                self.tasks = end_tasks
            else:
                self.stdio.verbose("tasks_package is all")
                self.get_all_tasks()
                filter_tasks = self.get_package_tasks("filter")
                if len(filter_tasks) > 0:
                    self.tasks = {key: value for key, value in self.tasks.items() if key not in filter_tasks}
                    new_tasks = {}
                    for filter_task in filter_tasks:
                        for task_name, task_value in self.tasks.items():
                            if re.match(filter_task.strip(), task_name.strip()) is None:
                                new_tasks[task_name] = task_value
                    self.tasks = new_tasks
            self.stdio.verbose("tasks is {0}".format(self.tasks.keys()))
        except Exception as e:
            self.stdio.error(e)

    # get all tasks
    def get_all_tasks(self):
        current_path = self.tasks_base_path
        tasks = {}
        for root, dirs, files in os.walk(current_path):
            for file in files:
                if file.endswith('.yaml'):
                    folder_name = os.path.basename(root)
                    task_name = "{}.{}".format(folder_name, file.split('.')[0])
                    task_data = YamlUtils.read_yaml_data(os.path.join(root, file))
                    tasks[task_name] = task_data
        if len(tasks) == 0:
            raise Exception("the len of tasks is 0")
        self.tasks = tasks

    # need  package_name
    def get_package_tasks(self, package_name):
        # Obtain information within the package file
        with open(self.package_file_name, 'r') as file:
            package_file_data = yaml.safe_load(file)
            packege_tasks = package_file_data
        if package_name not in packege_tasks:
            if package_name == "filter":
                return []
            else:
                raise CheckException("no cases name is {0}".format(package_name))
        self.stdio.verbose("by cases name: {0} , get cases: {1}".format(package_name, packege_tasks[package_name]))
        if packege_tasks[package_name].get("tasks") is None:
            return []
        return packege_tasks[package_name].get("tasks")

    # execute task
    def execute_one(self, task_name):
        try:
            self.stdio.verbose("execute tasks is {0}".format(task_name))
            # Verify if the version is within a reasonable range
            report = TaskReport(self.context, task_name)
            if not self.ignore_version:
                version = self.version
                if version:
                    self.cluster["version"] = version
                    self.stdio.verbose("cluster.version is {0}".format(self.cluster["version"]))
                    task = TaskBase(self.context, self.tasks[task_name]["task"], self.nodes, self.cluster, report)
                    self.stdio.verbose("{0} execute!".format(task_name))
                    task.execute()
                    self.stdio.verbose("execute tasks end : {0}".format(task_name))
                    return report
                else:
                    self.stdio.error("can't get version")
            else:
                self.stdio.verbose("ignore version")
        except Exception as e:
            self.stdio.error("execute_one Exception : {0}".format(e))
            raise CheckException("execute_one Exception : {0}".format(e))

    def execute(self):
        try:
            self.stdio.verbose("execute_all_tasks. the number of tasks is {0} ,tasks is {1}".format(len(self.tasks.keys()), self.tasks.keys()))
            self.report = CheckReport(self.context, export_report_path=self.export_report_path, export_report_type=self.export_report_type, report_target=self.check_target_type)
            # one of tasks to execute
            for task in self.tasks:
                t_report = self.execute_one(task)
                self.report.add_task_report(t_report)
            self.report.export_report()
        except CheckrReportException as e:
            self.stdio.error("Report error :{0}".format(e))
        except Exception as e:
            self.stdio.error("Internal error :{0}".format(e))


class checkOBConnectorPool:
    def __init__(self, context, max_size, cluster):
        self.max_size = max_size
        self.cluster = cluster
        self.connections = queue.Queue(maxsize=max_size)
        self.stdio = context.stdio
        self.stdio.verbose("obConnectorPool init success!")
        try:
            for i in range(max_size):
                conn = OBConnector(ip=self.cluster.get("db_host"), port=self.cluster.get("db_port"), username=self.cluster.get("tenant_sys").get("user"), password=self.cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=10000)
                self.connections.put(conn)
            self.stdio.verbose("obConnectorPool init success!")
        except Exception as e:
            self.stdio.error("obConnectorPool init fail! err:".format(e))

    def get_connection(self):
        try:
            return self.connections.get()
        except Exception as e:
            self.stdio.error("get connection fail! err:".format(e))
            return None

    def release_connection(self, conn):

        if conn is not None:
            self.connections.put(conn)
        return
