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
@time: 2024/2/1
@file: check_list.py
@desc:
"""
import os
import yaml

from src.common.result_type import ObdiagResult
from src.common.tool import Util, DynamicLoading


class CheckListHandler:
    def __init__(self, context):
        self.context = context
        self.options = self.context.options
        self.stdio = context.stdio
        self.work_path = os.path.expanduser(self.context.inner_config["check"]["work_path"] or "~/.obdiag/check")

    def handle(self):
        try:
            self.stdio.verbose("list check cases")
            entries = os.listdir(self.work_path)
            files = [f for f in entries if os.path.isfile(os.path.join(self.work_path, f))]
            result_map = {}
            for file in files:
                if "check_package" in file:
                    cases_map = {"all": {"name": "all", "command": "obdiag check run", "info_en": "default check all task without filter", "info_cn": "默认执行除filter组里的所有巡检项"}}
                    # Obtain which files match and corresponding header files
                    # Using string segmentation methods
                    parts = file.split('_')
                    if len(parts) < 1:
                        self.stdio.warn("invalid check package name :{0} , Please don't add file, which 'check_package' in the name".format(file))
                        continue
                    target = parts[0]
                    file = "{0}/{1}".format(self.work_path, file)
                    package_file_data = None
                    # read yaml file
                    with open(file, 'r') as f:
                        package_file_data = yaml.safe_load(f)
                        result_map[target] = {}
                        result_map[target]["commands"] = []
                        if not package_file_data or len(package_file_data) == 0:
                            self.stdio.warn("No data check package data :{0} ".format(file))
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
                        self.stdio.print("name: {0}\ninfo: {1}\n".format(task_name, get_task_list[target][task_name]))

            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=result_map)
        except Exception as e:
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=str(e))

    def get_task_list(self):
        self.stdio.verbose("get_task_list")
        # get traget dir
        tasks_list = {"obproxy": None, "observer": None}
        # for obproxy
        tasks_list["obproxy"] = self.get_task_list_by_target("obproxy")
        # for observer
        tasks_list["observer"] = self.get_task_list_by_target("observer")
        return tasks_list

    # just get info
    def get_task_list_by_target(self, target):
        self.stdio.verbose("get all tasks by target: {0}".format(target))
        current_path = os.path.join(os.path.expanduser("~/.obdiag/check/tasks"), target)
        tasks_info = {}

        # load task data and get the value of info
        def load_task_data(file):
            with open(file, 'r', encoding='utf-8') as f:
                task_data = yaml.safe_load(f)
                if "info" not in task_data:
                    raise Exception("the info field is not in the task data. Please check the task file")
                return task_data["info"]

        # get traget dir
        for root, dirs, files in os.walk(current_path):
            for file in files:
                if file.endswith('.yaml'):
                    folder_name = os.path.basename(root)
                    task_name = "{}.{}".format(folder_name, file.split('.')[0])
                    try:
                        tasks_info[task_name] = load_task_data(os.path.join(root, file))
                    except Exception as e:
                        self.stdio.error("load task data fail, file: {0}, e:".format(os.path.join(root, file), e))
                elif file.endswith('.py'):
                    lib_path = root
                    module_name = os.path.basename(file)[:-3]
                    task_name = "{}.{}".format(os.path.basename(root), module_name)
                    DynamicLoading.add_lib_path(lib_path)
                    module = DynamicLoading.import_module(os.path.basename(file)[:-3], None)
                    if not hasattr(module, module_name):
                        self.stdio.error("{0} import_module failed".format(module_name))
                        continue
                    # get task info
                    tasks_info[task_name] = getattr(module, module_name).get_task_info().get("info", "")
        return tasks_info
