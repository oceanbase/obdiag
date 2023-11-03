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
from common.command import get_observer_version_by_sql
import re
from utils.utils import display_trace
from utils.yaml_utils import read_yaml_data


class CheckHandler:

    def __init__(self, ignore_obversion, package_name, cluster, nodes, export_report_path, export_report_type,
                 case_package_file="./check_package.yaml", tasks_base_path="./handler/checker/tasks/"):

        self.tasks = None
        self.export_report_path = export_report_path
        self.export_report_type = export_report_type
        self.ignore_obversion = ignore_obversion
        self.cluster = cluster
        self.nodes = nodes
        self.tasks_base_path = tasks_base_path
        logger.info("check_package_file is {0} ,package_name is {1}, ignore_obversion is{2} , tasks_base_path is {3},"
                    "cluster is {4}, nodes is {5}, export_report_path is {6}, export_report_type is {7},".format(
            case_package_file, package_name,
            ignore_obversion, tasks_base_path, cluster, nodes, export_report_path, export_report_type))
        # case_package_file
        case_package_file = os.path.expanduser(case_package_file)
        if os.path.exists(case_package_file):
            self.package_file_name = case_package_file
        elif os.path.exists("./check_package.yaml"):
            logger.warning(
                "case_package_file {0} is not exist . use default ./check_package.yaml".format(case_package_file))
            self.tasks_base_path = "./check_package.yaml"
        else:
            raise CheckException("case_package_file {0} is not exist".format(case_package_file))
        logger.info("case_package_file is " + self.package_file_name)
        # checker tasks_base_path
        if not tasks_base_path == "":
            tasks_base_path = os.path.expanduser(tasks_base_path)
            if os.path.exists(tasks_base_path):
                self.tasks_base_path = tasks_base_path
            elif os.path.exists("./handler/checker/tasks/"):
                logger.warning(
                    "tasks_base_path {0} is not exist . use default ./handler/checker/tasks/".format(tasks_base_path))
                self.tasks_base_path = "./handler/checker/tasks/"
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
        if getattr(args, "cases"):
            package_name = getattr(args, "cases")[0]
            logger.info("cases name is {0}".format(package_name))
        else:
            logger.info("cases name is None")
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
            logger.info("tasks_package is all")
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
            # Verify if the obversion is within a reasonable range
            report = TaskReport(task_name)
            if not self.ignore_obversion:
                obversion = get_observer_version_by_sql(self.cluster)
                logger.info("cluster.obversion is {0}".format(obversion))
                if len(obversion) < 5:
                    raise CheckException("execute_one Exception : obversion len <5")

                self.cluster["obversion"] = re.findall(r'\d+\.\d+\.\d+\.\d+', obversion)[0]
            else:
                logger.info("ignore obversion")
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
            report = CheckReport(export_report_path=self.export_report_path, export_report_type=self.export_report_type)
            # one of tasks to execute
            for task in self.tasks:
                t_report = self.execute_one(task)
                report.add_task_report(t_report)
            report.export_report()
        except CheckrReportException as e:
            logger.error("Report error :{0}".format(e))
        except Exception as e:
            logger.error("Internal error :{0}".format(e))
        finally:
            display_trace(uuid.uuid3(uuid.NAMESPACE_DNS, str(os.getpid())))
