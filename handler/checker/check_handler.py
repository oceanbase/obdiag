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
import uuid
import yaml
from common.logger import logger
from handler.checker.check_exception import CheckException
from handler.checker.check_report import TaskReport, CheckReport, CheckrReportException
from handler.checker.check_task import TaskBase
from common.command import get_observer_version, get_obproxy_version
import re
from utils.utils import display_trace, node_cut_passwd_for_log
from utils.yaml_utils import read_yaml_data
from utils.shell_utils import SshHelper


class CheckHandler:

    def __init__(self, ignore_version, cluster, nodes, export_report_path, export_report_type,
                 check_target_type="observer",
                 case_package_file="./check_package.yaml", tasks_base_path="./handler/checker/tasks/"):
        # init input parameters
        self.report = None
        self.tasks = None
        self.export_report_path = export_report_path
        self.export_report_type = export_report_type
        self.ignore_version = ignore_version
        self.cluster = cluster
        self.nodes = nodes
        self.tasks_base_path = tasks_base_path
        self.check_target_type = check_target_type

        logger.debug("CheckHandler input. ignore_version is {0} , cluster is {1} , nodes is {2}, "
                     "export_report_path is {3}, export_report_type is {4} , check_target_type is {5}, "
                     "case_package_file is {6}, tasks_base_path is {7}.".format(ignore_version,
                                                                                cluster.get(
                                                                                    "ob_cluster_name") or cluster.get(
                                                                                    "obproxy_cluster_name"),
                                                                                node_cut_passwd_for_log(nodes),
                                                                                export_report_path,
                                                                                export_report_type,
                                                                                check_target_type,
                                                                                case_package_file,
                                                                                tasks_base_path))

        # case_package_file
        # build case_package_file
        if check_target_type is not None and check_target_type != "observer":
            file_name = os.path.basename(case_package_file)
            file_dir = os.path.dirname(case_package_file)
            case_package_file = file_dir+"/"+check_target_type + "_" + file_name
        case_package_file = os.path.expanduser(case_package_file)
        if os.path.exists(case_package_file):
            self.package_file_name = case_package_file
        else:
            raise CheckException("case_package_file {0} is not exist".format(case_package_file))
        logger.info("case_package_file is " + self.package_file_name)

        # checker tasks_base_path
        # build tasks_base_path
        if check_target_type is not None:
            tasks_base_path = tasks_base_path + "/" + check_target_type
        tasks_base_path = os.path.expanduser(tasks_base_path)
        if os.path.exists(tasks_base_path):
            self.tasks_base_path = tasks_base_path
        else:
            raise CheckException("tasks_base_path {0} is not exist".format(tasks_base_path))
        logger.info("tasks_base_path is " + self.tasks_base_path)

        # checker export_report_path
        export_report_path = os.path.expanduser(export_report_path)
        if not os.path.exists(export_report_path):
            logger.warning("{0} not exists. mkdir it!".format(self.export_report_path))
            os.mkdir(export_report_path)
        self.export_report_path = export_report_path
        logger.info("export_report_path is " + self.export_report_path)

    def handle(self, args):
        package_name = None
        if self.check_target_type == "obproxy" and getattr(args, "obproxy_cases"):
            obproxy_cases = getattr(args, "obproxy_cases")
            if isinstance(obproxy_cases, list):
                package_name = obproxy_cases[0]
            else:
                package_name = getattr(args, "obproxy_cases")

        if self.check_target_type == "observer" and getattr(args, "cases"):
            package_name = getattr(args, "cases")
            if isinstance(package_name, list):
                package_name = package_name[0]
            else:
                package_name = getattr(args, "cases")

        logger.info("package_name is {0}".format(package_name))
        # get package's by package_name
        self.tasks = {}
        if package_name:
            package_tasks_by_name = self.get_package_tasks(package_name)
            self.get_all_tasks()
            end_tasks = {}
            for package_task in package_tasks_by_name:
                if package_task in self.tasks:
                    end_tasks[package_task] = self.tasks[package_task]
            self.tasks = end_tasks
        else:
            logger.debug("tasks_package is all")
            self.get_all_tasks()
            filter_tasks = self.get_package_tasks("filter")
            self.tasks = {key: value for key, value in self.tasks.items() if key not in
                          filter_tasks}
        logger.info("tasks is {0}".format(self.tasks.keys()))

    # get all tasks
    def get_all_tasks(self):
        current_path = self.tasks_base_path
        tasks = {}
        for root, dirs, files in os.walk(current_path):
            for file in files:
                if file.endswith('.yaml'):
                    folder_name = os.path.basename(root)
                    task_name = "{}.{}".format(folder_name, file.split('.')[0])
                    task_data = read_yaml_data(os.path.join(root, file))
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
        logger.debug("by cases name: {0} , get cases: {1}".format(package_name, packege_tasks[package_name]))
        if packege_tasks[package_name] is None:
            return []
        return packege_tasks[package_name]

    # execute task
    def execute_one(self, task_name):
        try:
            logger.info("execute tasks is {0}".format(task_name))
            # Verify if the version is within a reasonable range
            report = TaskReport(task_name)
            if not self.ignore_version:
                try:
                    node = self.nodes[0]
                    ssh = SshHelper(True, node.get("ip"),
                                    node.get("user"),
                                    node.get("password"),
                                    node.get("port"),
                                    node.get("private_key"),
                                    node)

                    if self.check_target_type == "observer":
                        version = get_observer_version(True, ssh, self.nodes[0]["home_path"])
                    elif self.check_target_type == "obproxy":
                        version = get_obproxy_version(True, ssh, self.nodes[0]["home_path"])
                    else:
                        raise Exception(
                            "check_target_type is {0} . No func to get the version".format(self.check_target_type))
                    self.cluster["version"] = re.findall(r'\d+\.\d+\.\d+\.\d+', version)[0]
                    logger.info("cluster.version is {0}".format(self.cluster["version"]))
                except Exception as e:
                    logger.error("can't get version, Exception: {0}".format(e))
                    raise Exception("can't get version, Exception: {0}".format(e))
            else:
                logger.info("ignore version")
            task = TaskBase(self.tasks[task_name]["task"], self.nodes, self.cluster, report)
            logger.info("{0} execute!".format(task_name))
            task.execute()
            logger.info("execute tasks end : {0}".format(task_name))
            return report
        except Exception as e:
            logger.error("execute_one Exception : {0}".format(e))
            raise CheckException("execute_one Exception : {0}".format(e))

    def execute(self):
        try:
            logger.info("execute_all_tasks. the number of tasks is {0} ,tasks is {1}".format(len(self.tasks.keys()),
                                                                                             self.tasks.keys()))
            self.report = CheckReport(export_report_path=self.export_report_path,
                                      export_report_type=self.export_report_type,
                                      report_target=self.check_target_type)
            # one of tasks to execute
            for task in self.tasks:
                t_report = self.execute_one(task)
                self.report.add_task_report(t_report)
            self.report.export_report()
        except CheckrReportException as e:
            logger.error("Report error :{0}".format(e))
        except Exception as e:
            logger.error("Internal error :{0}".format(e))
        finally:
            display_trace(uuid.uuid3(uuid.NAMESPACE_DNS, str(os.getpid())))
