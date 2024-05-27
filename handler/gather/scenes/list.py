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
@time: 2024/01/10
@file: list.py
@desc:
"""

import os
from stdio import SafeStdio
from common.tool import YamlUtils
from handler.gather.scenes.register import hardcode_scene_list
from common.tool import Util


class GatherScenesListHandler(SafeStdio):
    def __init__(self, context, yaml_tasks_base_path="~/.obdiag/gather/tasks/"):
        self.context = context
        self.stdio = context.stdio
        self.observer_tasks = {}
        self.obproxy_tasks = {}
        self.other_tasks = {}
        self.yaml_tasks_base_path = yaml_tasks_base_path
        base_path = os.path.expanduser(yaml_tasks_base_path)
        if os.path.exists(base_path):
            self.yaml_tasks_base_path = base_path
        else:
            self.stdio.error("Failed to find yaml task path: {0}".format(base_path))

    def handle(self):
        self.stdio.verbose("list gather scene")
        self.get_all_yaml_tasks()
        self.get_all_code_tasks()
        self.stdio.verbose("len of observer_tasks: {0}; len of observer_tasks: {1}; len of observer_tasks: {2};".format(len(self.observer_tasks), len(self.obproxy_tasks), len(self.other_tasks)))
        if (len(self.observer_tasks) + len(self.obproxy_tasks) + len(self.other_tasks)) == 0:
            self.stdio.error("Failed to find any tasks")
        else:
            self.print_scene_data()

    def get_all_yaml_tasks(self):
        try:
            current_path = self.yaml_tasks_base_path
            for root, dirs, files in os.walk(current_path):
                for file in files:
                    if file.endswith('.yaml'):
                        folder_name = os.path.basename(root)
                        task_name = "{}.{}".format(folder_name, file.split('.')[0])
                        task_data = YamlUtils.read_yaml_data(os.path.join(root, file))
                        task_data["name"] = task_name
                        if folder_name == "observer":
                            self.observer_tasks[task_name] = task_data
                        elif folder_name == "obproxy":
                            self.obproxy_tasks[task_name] = task_data
                        else:
                            self.other_tasks[task_name] = task_data
        except Exception as e:
            self.stdio.error("get all yaml task failed, error: ", e)

    def get_all_code_tasks(self):
        try:
            for scene in hardcode_scene_list:
                if "observer" in scene.name:
                    self.observer_tasks[scene.name] = self.__get_hardcode_task(scene)
                elif "obproxy" in scene.name:
                    self.obproxy_tasks[scene.name] = self.__get_hardcode_task(scene)
                else:
                    self.other_tasks[scene.name] = self.__get_hardcode_task(scene)
        except Exception as e:
            self.stdio.error("get all hard code task failed, error: ", e)

    def __get_hardcode_task(self, scene):
        return {
            "name": scene.name,
            "command": scene.command,
            "info_en": scene.info_en,
            "info_cn": scene.info_cn,
        }

    def get_one_yaml_task(self, name):
        try:
            task_data = None
            current_path = self.yaml_tasks_base_path
            for root, dirs, files in os.walk(current_path):
                for file in files:
                    if file.endswith('.yaml'):
                        folder_name = os.path.basename(root)
                        task_name = "{}.{}".format(folder_name, file.split('.')[0])
                        if name == task_name:
                            task_data = YamlUtils.read_yaml_data(os.path.join(root, file))
                            task_data["name"] = task_name
            return task_data
        except Exception as e:
            self.stdio.error("get one yaml task failed, error: ", e)

    def is_code_task(self, name):
        try:
            for scene in hardcode_scene_list:
                if scene.name == name:
                    return True
            return False
        except Exception as e:
            self.stdio.error("get one code task failed, error: ", e)
            return False

    def print_scene_data(self):
        sorted_observer_tasks_dict = {}
        sorted_obproxy_tasks_dict = {}
        sorted_other_tasks_dict = {}
        if self.other_tasks:
            sorted_other_tasks = sorted(self.other_tasks.items(), key=lambda x: x[0])
            sorted_other_tasks_dict = {k: v for k, v in sorted_other_tasks}
            Util.print_title("Other Problem Gather Scenes")
            Util.print_scene(sorted_other_tasks_dict)
        if self.obproxy_tasks:
            sorted_obproxy_tasks = sorted(self.obproxy_tasks.items(), key=lambda x: x[0])
            sorted_obproxy_tasks_dict = {k: v for k, v in sorted_obproxy_tasks}
            Util.print_title("Obproxy Problem Gather Scenes")
            Util.print_scene(sorted_obproxy_tasks_dict)
        if self.observer_tasks:
            sorted_observer_tasks = sorted(self.observer_tasks.items(), key=lambda x: x[0])
            sorted_observer_tasks_dict = {k: v for k, v in sorted_observer_tasks}
            Util.print_title("Observer Problem Gather Scenes")
            Util.print_scene(sorted_observer_tasks_dict)
