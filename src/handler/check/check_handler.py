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
@time: 2024/12/29
@file: check_handler.py
@desc: Handler for executing Python check tasks
"""

import os
import queue
import traceback
import re
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from src.common.ob_connector import OBConnector
from src.common.scene import get_version_by_type
from src.handler.check.check_exception import CheckException
from src.handler.check.check_report import TaskReport, CheckReport, CheckReportException
from src.common.tool import Util, DynamicLoading
from src.common.tool import StringUtils


class CheckHandler:
    """
    Handler for executing Python check tasks.

    This handler:
    1. Loads Python check tasks from the tasks directory
    2. Executes tasks based on specified cases or filters
    3. Generates check reports in various formats
    """

    def __init__(self, context, check_target_type="observer"):
        self.version = None
        self.context = context
        self.stdio = context.stdio
        # init input parameters
        self.report = None
        self.tasks = None
        # concurrent execution config, default 12 threads
        self.max_workers = self.context.inner_config.get("check", {}).get("max_workers", 12)
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
        self.options = self.context.options
        env_option = Util.get_option(self.options, 'env')
        self.input_env = StringUtils.parse_env_display(env_option) or {}

        # init output parameters
        self.stdio.verbose(
            "CheckHandler input. ignore_version is {0}, cluster is {1}, nodes is {2}, "
            "export_report_path is {3}, export_report_type is {4}, check_target_type is {5}, "
            "tasks_base_path is {6}, input_env is {7}".format(
                self.ignore_version,
                self.cluster.get("ob_cluster_name") or self.cluster.get("obproxy_cluster_name"),
                StringUtils.node_cut_passwd_for_log(self.nodes),
                self.export_report_path,
                self.export_report_type,
                self.check_target_type,
                self.tasks_base_path,
                self.input_env,
            )
        )

        # case_package_file
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

        # Note: SSH connections are created per-task in __execute_one to avoid connection contention
        # Each task gets its own independent SSH connections for thread safety

        # get version
        if Util.get_option(self.options, 'cases') != "build_before":
            self.version = get_version_by_type(self.context, self.check_target_type, self.stdio)
            obConnectorPool = None
            try:
                # Connection pool size matches max_workers for optimal concurrency
                pool_size = min(self.max_workers, 12)  # max 12 connections to avoid overloading DB
                obConnectorPool = CheckOBConnectorPool(context, pool_size, self.cluster)
            except Exception as e:
                self.stdio.warn("obConnector init error. Error info is {0}".format(e))
            finally:
                self.context.set_variable('check_obConnector_pool', obConnectorPool)
        else:
            self.stdio.warn("check cases is build_before, so don't get version")

    def handle(self):
        """Main entry point for check execution."""
        try:
            package_name = None
            input_tasks = None

            # get input tasks or package name
            if self.check_target_type == "obproxy":
                if Util.get_option(self.options, 'obproxy_tasks'):
                    input_tasks = Util.get_option(self.options, 'obproxy_tasks')
                if Util.get_option(self.options, 'obproxy_cases'):
                    package_name = Util.get_option(self.options, 'obproxy_cases')
            if self.check_target_type == "observer":
                if Util.get_option(self.options, 'observer_tasks'):
                    input_tasks = Util.get_option(self.options, 'observer_tasks')
                if Util.get_option(self.options, 'cases'):
                    package_name = Util.get_option(self.options, 'cases')

            if Util.get_option(self.options, 'cases') == "build_before" and self.check_target_type == "obproxy":
                self.stdio.print("when cases is build_before, not check obproxy")
                return

            if Util.get_option(self.options, 'store_dir'):
                self.export_report_path = Util.get_option(self.options, 'store_dir')
                self.stdio.verbose("export_report_path change to " + self.export_report_path)
            self.export_report_path = os.path.expanduser(self.export_report_path)
            if not os.path.exists(self.export_report_path):
                self.stdio.warn("{0} not exists. mkdir it!".format(self.export_report_path))
                os.mkdir(self.export_report_path)

            # change self.export_report_type
            if Util.get_option(self.options, 'report_type'):
                self.export_report_type = Util.get_option(self.options, 'report_type')
                if self.export_report_type not in ["table", "json", "xml", "yaml", "html"]:
                    raise CheckException("report_type must be table, json, xml, yaml, html")
            self.stdio.verbose("export_report_path is " + self.export_report_path)

            # get tasks
            self.tasks = {}
            if input_tasks:
                input_tasks = input_tasks.replace(" ", "")
                input_tasks = input_tasks.split(";")
                self.get_all_tasks()
                end_tasks = {}
                for package_task in input_tasks:
                    if package_task in self.tasks:
                        end_tasks[package_task] = self.tasks[package_task]
                    for task_name, value in self.tasks.items():
                        if re.match(package_task, task_name):
                            end_tasks[task_name] = self.tasks[task_name]
                if len(end_tasks) == 0:
                    raise CheckException("no cases is check by *_tasks: {0}".format(input_tasks))
                self.tasks = end_tasks
                self.stdio.verbose("input_tasks is {0}".format(input_tasks))
            elif package_name:
                self.stdio.verbose("package_name is {0}".format(package_name))
                package_tasks_by_name = self.get_package_tasks(package_name)
                self.get_all_tasks()
                end_tasks = {}
                for package_task in package_tasks_by_name:
                    if package_task in self.tasks:
                        end_tasks[package_task] = self.tasks[package_task]
                    for task_name, value in self.tasks.items():
                        if re.match(package_task, task_name):
                            end_tasks[task_name] = self.tasks[task_name]
                self.tasks = end_tasks
            else:
                self.stdio.verbose("tasks_package is all")
                self.get_all_tasks()
                filter_tasks = self.get_package_tasks("filter")
                if len(filter_tasks) > 0:
                    self.tasks = {key: value for key, value in self.tasks.items() if key not in filter_tasks}
                    new_tasks = {}
                    for task_name, task_value in self.tasks.items():
                        filter_tag = False
                        for filter_task in filter_tasks:
                            if re.match(filter_task.strip(), task_name.strip()):
                                filter_tag = True
                        if not filter_tag:
                            new_tasks[task_name] = task_value
                    self.tasks = new_tasks

            self.stdio.verbose("tasks is {0}".format(self.tasks.keys()))
            return self.__execute()
        except Exception as e:
            self.stdio.error("Get package tasks failed. Error info is {0}".format(e))
            self.stdio.verbose(traceback.format_exc())
            raise CheckException("Internal error: {0}".format(e))

    def get_all_tasks(self):
        """Load all Python check tasks from the tasks directory."""
        self.stdio.verbose("get all tasks")
        current_path = self.tasks_base_path
        tasks = {}

        for root, dirs, files in os.walk(current_path):
            for file in files:
                # Only load Python files
                if file.endswith('.py') and not file.startswith('__'):
                    folder_name = os.path.basename(root)
                    task_name = "{}.{}".format(folder_name, file.split('.')[0])
                    try:
                        DynamicLoading.add_lib_path(root)
                        task_module = DynamicLoading.import_module(file[:-3], self.stdio)
                        attr_name = task_name.split('.')[-1]
                        if task_module is None:
                            self.stdio.error("{0} import_module failed: module is None".format(task_name))
                            continue
                        if not hasattr(task_module, attr_name):
                            self.stdio.error("{0} import_module failed: missing {1} attribute. Module attrs: {2}".format(task_name, attr_name, [x for x in dir(task_module) if not x.startswith('_')]))
                            continue
                        tasks[task_name] = getattr(task_module, attr_name)
                    except Exception as e:
                        self.stdio.error("import_module {0} failed: {1}".format(task_name, e))
                        raise CheckException("import_module {0} failed: {1}".format(task_name, e))

        if len(tasks) == 0:
            raise CheckException("No tasks found in {0}".format(current_path))
        self.tasks = tasks

    def get_package_tasks(self, package_name):
        """Get task list from package configuration file."""
        with open(self.package_file_name, 'r', encoding='utf-8') as file:
            package_file_data = yaml.safe_load(file)
            packege_tasks = package_file_data
        if package_name not in packege_tasks:
            if package_name == "filter":
                return []
            else:
                raise CheckException("no cases name is {0}".format(package_name))
        self.stdio.verbose("by cases name: {0}, get cases: {1}".format(package_name, packege_tasks[package_name]))
        if packege_tasks[package_name].get("tasks") is None:
            return []
        return packege_tasks[package_name].get("tasks")

    def __execute_one(self, task_name):
        """Execute a single check task."""
        task_instance = None
        try:
            self.stdio.verbose("execute task: {0}".format(task_name))
            report = TaskReport(self.context, task_name)

            # Pre-check: verify OS compatibility
            task_instance = self.tasks[task_name]
            task_info = task_instance.get_task_info()
            supported_os = task_info.get("supported_os")

            if supported_os:
                # Check if current OS is supported
                current_os = self.__get_current_os()
                if current_os not in supported_os:
                    self.stdio.verbose("Task {0} skipped: requires {1}, current OS is {2}".format(task_name, supported_os, current_os))
                    report.add_warning("Task skipped: requires OS {0}, current is {1}".format(supported_os, current_os))
                    return report

            if not self.ignore_version:
                version = self.version
                if version or Util.get_option(self.options, 'cases') == "build_before":
                    self.cluster["version"] = version
                    self.stdio.verbose("cluster.version is {0}".format(self.cluster["version"]))

                    # Execute Python task
                    # SSH connections are created in TaskBase.init() for thread safety
                    task_instance = self.tasks[task_name]
                    task_instance.init(self.context, report)
                    task_instance.execute()

                    self.stdio.verbose("execute task end: {0}".format(task_name))
                    return report
                else:
                    self.stdio.error("can't get version")
            else:
                self.stdio.verbose("ignore version")
                # Execute Python task without version check
                # SSH connections are created in TaskBase.init() for thread safety
                task_instance = self.tasks[task_name]
                task_instance.init(self.context, report)
                task_instance.execute()
                return report
        except Exception as e:
            self.stdio.error("execute_one Exception: {0}".format(e))
            raise CheckException("execute_one Exception: {0}".format(e))
        finally:
            # Cleanup task resources (release connection back to pool and close SSH connections)
            if task_instance and hasattr(task_instance, 'cleanup'):
                try:
                    task_instance.cleanup()
                except Exception as cleanup_error:
                    self.stdio.warn("task cleanup error: {0}".format(cleanup_error))

    def __execute(self):
        """Execute all check tasks concurrently and generate report."""
        try:
            task_count = len(self.tasks.keys())
            self.stdio.verbose("execute_all_tasks. the number of tasks is {0}, tasks is {1}".format(task_count, self.tasks.keys()))
            self.report = CheckReport(self.context, export_report_path=self.export_report_path, export_report_type=self.export_report_type, report_target=self.check_target_type)

            # Determine actual worker count (don't use more workers than tasks)
            actual_workers = min(self.max_workers, task_count) if task_count > 0 else 1
            self.stdio.verbose("Starting concurrent execution with {0} workers".format(actual_workers))

            # Execute tasks concurrently using ThreadPoolExecutor
            task_names = list(self.tasks.keys())
            failed_tasks = []

            with ThreadPoolExecutor(max_workers=actual_workers) as executor:
                # Submit all tasks
                future_to_task = {executor.submit(self.__execute_one_safe, task_name): task_name for task_name in task_names}

                # Collect results as they complete
                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    try:
                        t_report = future.result()
                        if t_report:
                            self.report.add_task_report(t_report)
                    except Exception as e:
                        failed_tasks.append(task_name)
                        self.stdio.error("Task {0} failed with exception: {1}".format(task_name, e))

            if failed_tasks:
                self.stdio.warn("The following tasks failed: {0}".format(failed_tasks))

            self.report.export_report()
            return self.report.report_tobeMap()
        except CheckReportException as e:
            self.stdio.error("Report error: {0}".format(e))
            raise CheckException("Report error: {0}".format(e))
        except Exception as e:
            raise CheckException("Internal error: {0}".format(e))
        finally:
            # Cleanup: close SSH connections
            self.__cleanup()

    def __execute_one_safe(self, task_name):
        """Thread-safe wrapper for __execute_one that catches exceptions."""
        try:
            return self.__execute_one(task_name)
        except Exception as e:
            self.stdio.error("execute_one_safe Exception for task {0}: {1}".format(task_name, e))
            # Return a failed report instead of raising
            report = TaskReport(self.context, task_name)
            report.add_fail("Task execution failed: {0}".format(str(e)))
            return report


    def __get_current_os(self):
        """
        Get the current operating system type.
        Returns: 'linux', 'darwin' (macOS), or 'unknown'
        """
        import platform

        system = platform.system().lower()
        if system == "linux":
            return "linux"
        elif system == "darwin":
            return "darwin"
        else:
            return "unknown"

    def __cleanup(self):
        """Cleanup all resources after check execution."""
        # Note: SSH connections are now created per-task and cleaned up in __execute_one
        # No need to cleanup here as connections are task-specific
        self.stdio.verbose("Check execution cleanup completed")


class CheckOBConnectorPool:
    """Connection pool for OceanBase database connections."""

    def __init__(self, context, max_size, cluster):
        self.max_size = max_size
        self.cluster = cluster
        self.connections = queue.Queue(maxsize=max_size)
        self.stdio = context.stdio
        self.stdio.verbose("CheckOBConnectorPool init success!")
        try:
            for i in range(max_size):
                conn = OBConnector(context=context, ip=self.cluster.get("db_host"), port=self.cluster.get("db_port"), username=self.cluster.get("tenant_sys").get("user"), password=self.cluster.get("tenant_sys").get("password"), timeout=10000)
                self.connections.put(conn)
            self.stdio.verbose("CheckOBConnectorPool init success!")
        except Exception as e:
            self.stdio.error("CheckOBConnectorPool init fail! err: {0}".format(e))

    def get_connection(self):
        """Get a connection from the pool."""
        try:
            return self.connections.get()
        except Exception as e:
            self.stdio.error("get connection fail! err: {0}".format(e))
            return None

    def release_connection(self, conn):
        """Release a connection back to the pool."""
        if conn is not None:
            self.connections.put(conn)
        return
