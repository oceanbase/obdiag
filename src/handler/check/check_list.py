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
@file: check_list.py
@desc: Handler for listing available check tasks
"""

import os
import yaml

from src.common.result_type import ObdiagResult
from src.common.tool import Util, DynamicLoading


class CheckListHandler:
    """Handler for listing available check tasks and packages."""

    def __init__(self, context):
        self.context = context
        self.options = self.context.options
        self.stdio = context.stdio
        self.work_path = os.path.expanduser(self.context.inner_config["check"]["work_path"] or "~/.obdiag/check")

    def handle(self):
        """List all available check cases and tasks."""
        try:
            self.stdio.verbose("list check cases")
            entries = os.listdir(self.work_path)
            files = [f for f in entries if os.path.isfile(os.path.join(self.work_path, f))]
            result_map = {}

            for file in files:
                if "check_package" in file:
                    cases_map = {"all": {"name": "all", "command": "obdiag check run", "info_en": "default check all task without filter", "info_cn": "默认执行除filter组里的所有巡检项"}}
                    # Parse package file name
                    parts = file.split('_')
                    if len(parts) < 1:
                        self.stdio.warn("invalid check package name: {0}, Please don't add file which 'check_package' in the name".format(file))
                        continue
                    target = parts[0]
                    file_path = "{0}/{1}".format(self.work_path, file)

                    # read yaml file
                    with open(file_path, 'r') as f:
                        package_file_data = yaml.safe_load(f)
                        result_map[target] = {}
                        result_map[target]["commands"] = []
                        if not package_file_data or len(package_file_data) == 0:
                            self.stdio.warn("No data in check package: {0}".format(file_path))
                            continue

                        for package_data in package_file_data:
                            if package_data == "filter":
                                continue
                            package_target = target
                            if target == "observer":
                                package_target = "cases"
                            else:
                                package_target = "{0}_cases".format(target)

                            cases_map[package_data] = {
                                "name": package_data,
                                "command": "obdiag check run --{0}={1}".format(package_target, package_data),
                                "info_en": package_file_data[package_data].get("info_en") or "",
                                "info_cn": package_file_data[package_data].get("info_cn") or "",
                            }
                            result_map[target]["commands"].append(
                                {
                                    "name": package_data,
                                    "command": "obdiag check run --{0}={1}".format(package_target, package_data),
                                    "info_en": package_file_data[package_data].get("info_en") or "",
                                    "info_cn": package_file_data[package_data].get("info_cn") or "",
                                }
                            )

                    Util.print_title("check cases about {0}".format(target), stdio=self.stdio)
                    Util.print_scene(cases_map, stdio=self.stdio)

            # Check if --all option is provided
            show_all_tasks = False
            if Util.get_option(self.options, 'all'):
                show_all_tasks = True

            if show_all_tasks:
                get_task_list = self.get_task_list()
                for target in get_task_list:
                    if get_task_list[target] is None:
                        continue
                    self.stdio.print("\n\n")
                    self.stdio.print("tasks of {0}:".format(target))
                    result_map[target]["tasks"] = get_task_list[target]
                    for task_name in result_map[target]["tasks"]:
                        task_data = get_task_list[target][task_name]
                        task_info = task_data.get("info", "") if isinstance(task_data, dict) else task_data
                        task_issue_link = task_data.get("issue_link", "") if isinstance(task_data, dict) else ""
                        if task_issue_link:
                            self.stdio.print("name: {0}\ninfo: {1}\nissue_link: {2}\n".format(task_name, task_info, task_issue_link))
                        else:
                            self.stdio.print("name: {0}\ninfo: {1}\n".format(task_name, task_info))

            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=result_map)
        except Exception as e:
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=str(e))

    def get_task_list(self):
        """Get list of all tasks for each target."""
        self.stdio.verbose("get_task_list")
        tasks_list = {"obproxy": None, "observer": None}
        tasks_list["obproxy"] = self.get_task_list_by_target("obproxy")
        tasks_list["observer"] = self.get_task_list_by_target("observer")
        return tasks_list

    def get_task_list_by_target(self, target):
        """
        Get all Python tasks for a specific target.

        Args:
            target: Either "obproxy" or "observer"

        Returns:
            dict: Task name -> task info mapping (includes 'info' and optional 'issue_link')
        """
        self.stdio.verbose("get all tasks by target: {0}".format(target))
        current_path = os.path.join(os.path.expanduser("~/.obdiag/check/tasks"), target)
        tasks_info = {}

        for root, dirs, files in os.walk(current_path):
            for file in files:
                # Only load Python files
                if file.endswith('.py') and not file.startswith('__'):
                    lib_path = root
                    module_name = os.path.basename(file)[:-3]
                    task_name = "{}.{}".format(os.path.basename(root), module_name)
                    try:
                        DynamicLoading.add_lib_path(lib_path)
                        module = DynamicLoading.import_module(module_name, None)
                        if not hasattr(module, module_name):
                            self.stdio.error("{0} import_module failed: missing {1} attribute".format(task_name, module_name))
                            continue
                        # get task info including issue_link
                        task_info = getattr(module, module_name).get_task_info()
                        tasks_info[task_name] = {"info": task_info.get("info", ""), "issue_link": task_info.get("issue_link", "")}
                    except Exception as e:
                        self.stdio.error("load task {0} failed: {1}".format(task_name, e))

        return tasks_info
