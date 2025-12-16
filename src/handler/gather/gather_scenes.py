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
@time: 2024/01/04
@file: gather_scene_handler.py
@desc:
"""

import os
import re
from src.common.result_type import ObdiagResult
from src.common.stdio import SafeStdio
import datetime
from src.handler.gather.scenes.base import SceneBase
from src.common.obdiag_exception import OBDIAGFormatException
from src.handler.gather.scenes.list import GatherScenesListHandler
from src.common.tool import DirectoryUtil
from src.common.tool import StringUtils
from src.common.scene import get_version_by_type
from colorama import Fore, Style
from src.common.tool import Util
from src.common.tool import TimeUtils


class GatherSceneHandler(SafeStdio):

    def __init__(self, context, gather_pack_dir='./', tasks_base_path="~/.obdiag/gather/tasks/", task_type="observer", is_inner=False):
        self.context = context
        self.stdio = context.stdio
        self.is_ssh = True
        self.report = None
        self.gather_pack_dir = gather_pack_dir
        self.report_path = None
        self.yaml_tasks = {}
        self.code_tasks = {}
        self.env = {}
        self.scene = "observer.base"
        self.tasks_base_path = tasks_base_path
        self.task_type = task_type
        self.variables = {}
        self.is_inner = is_inner
        self.temp_dir = '/tmp'
        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        self.cluster = self.context.cluster_config
        self.obproxy_nodes = self.context.obproxy_config['servers']
        self.ob_nodes = self.context.cluster_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.ob_nodes, self.stdio)
        if new_nodes:
            self.nodes = new_nodes
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init option failed")
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init config failed")
        self.context.set_variable('temp_dir', self.temp_dir)
        self.__init_variables()
        self.__init_report_path()
        self.__init_task_names()
        self.execute()
        if self.is_inner:
            result = self.__get_sql_result()
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.report_path})
        else:
            self.__print_result()
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.report_path})

    def execute(self):
        try:
            self.stdio.verbose("execute_tasks. the number of tasks is {0} ,tasks is {1}".format(len(self.yaml_tasks.keys()), self.yaml_tasks.keys()))
            for key, value in zip(self.yaml_tasks.keys(), self.yaml_tasks.values()):
                self.__execute_yaml_task_one(key, value)
            for key, value in zip(self.code_tasks.keys(), self.code_tasks.values()):
                self.__execute_code_task_one(key, value)
        except Exception as e:
            self.stdio.error("Internal error :{0}".format(e))

    # execute yaml task
    def __execute_yaml_task_one(self, task_name, task_data):
        try:
            self.stdio.print("execute tasks: {0}".format(task_name))
            task_type = self.__get_task_type(task_name)
            version = get_version_by_type(self.context, task_type)
            if version:
                match = re.search(r'\d+(\.\d+){2}(?:\.\d+)?', version)
                if match:
                    self.cluster["version"] = match.group(0)
                else:
                    self.stdio.erroe("get cluster.version failed")
                    return
                task = SceneBase(context=self.context, scene=task_data["task"], report_dir=self.report_path, env=self.env, scene_variable_dict=self.variables, task_type=task_type)
                self.stdio.verbose("{0} execute!".format(task_name))
                task.execute()
                self.stdio.verbose("execute tasks end : {0}".format(task_name))
            else:
                self.stdio.error("can't get version")
        except Exception as e:
            self.stdio.error("__execute_yaml_task_one Exception : {0}".format(e))

    # execute code task
    def __execute_code_task_one(self, task_name, task_data):
        try:
            self.stdio.verbose("execute tasks is {0}".format(task_name))
            task = task_data["module"]
            task.init(self.context, task_name, self.report_path, self.variables, self.env)
            self.stdio.verbose("{0} execute!".format(task_name))
            task.execute()
            self.stdio.verbose("execute tasks end : {0}".format(task_name))
        except Exception as e:
            self.stdio.exception("__execute_code_task_one Exception : {0}".format(e))

    def __init_task_names(self):
        if self.scene:
            new = re.sub(r'\{|\}', '', self.scene)
            items = re.split(r'[;,]', new)
            scene = GatherScenesListHandler(self.context)
            for item in items:
                task_data = scene.get_one_task(item)
                if task_data["task_type"] == 'py':
                    self.code_tasks[item] = task_data
                elif task_data["task_type"] == 'yaml':
                    self.yaml_tasks[item] = task_data
                else:
                    self.stdio.error("Invalid Task :{0}. Please check the task is exist.".format(item))
                    if ".yaml" in item:
                        self.stdio.suggest("'.yaml' in task :{0}. Maybe you can remove it. use '--scene={1}'".format(item, item.replace(".yaml", "")))
            # hard code add gather observer.base
            if len(self.code_tasks) > 0:
                self.yaml_tasks["observer.base"] = scene.get_one_task("observer.base")
        else:
            self.stdio.error("get task name failed")

    def __init_report_path(self):
        try:
            self.report_path = os.path.join(self.gather_pack_dir, "obdiag_gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp), self.stdio))
            self.stdio.verbose("Use {0} as pack dir.".format(self.report_path))
            DirectoryUtil.mkdir(path=self.report_path, stdio=self.stdio)
        except Exception as e:
            self.stdio.error("init_report_path failed, error:{0}".format(e))

    def __init_variables(self):
        try:
            self.variables = {
                "observer_data_dir": self.ob_nodes[0].get("home_path") if self.ob_nodes and self.ob_nodes[0].get("home_path") else "",
                "obproxy_data_dir": self.obproxy_nodes[0].get("home_path") if self.obproxy_nodes and self.obproxy_nodes[0].get("home_path") else "",
                "from_time": self.from_time_str,
                "to_time": self.to_time_str,
            }
            self.stdio.verbose("gather scene variables: {0}".format(self.variables))
        except Exception as e:
            self.stdio.error("init gather scene variables failed, error: {0}".format(e))

    def __get_task_type(self, s):
        trimmed_str = s.strip()
        if '.' in trimmed_str:
            parts = trimmed_str.split('.', 1)
            return parts[0]
        else:
            return None

    def init_option(self):
        options = self.context.options
        from_option = Util.get_option(options, 'from')
        to_option = Util.get_option(options, 'to')
        since_option = Util.get_option(options, 'since')
        store_dir_option = Util.get_option(options, 'store_dir')
        env_option = Util.get_option(options, 'env')
        scene_option = Util.get_option(options, 'scene')
        temp_dir_option = Util.get_option(options, 'temp_dir')
        skip_type_option = Util.get_option(options, 'skip_type')
        if from_option is not None and to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(from_option)
                to_timestamp = TimeUtils.parse_time_str(to_option)
                self.from_time_str = from_option
                self.to_time_str = to_option
            except OBDIAGFormatException:
                self.stdio.exception('Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. from_datetime={0}, to_datetime={1}'.format(from_option, to_option))
                return False
            if to_timestamp <= from_timestamp:
                self.stdio.exception('Error: from datetime is larger than to datetime, please check.')
                return False
        elif (from_option is None or to_option is None) and since_option is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.print('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        if scene_option:
            self.scene = scene_option
        if env_option:
            env_dict = StringUtils.parse_env_display(env_option)
            self.env = env_dict
            self.context.set_variable("env", self.env)
        if temp_dir_option:
            self.temp_dir = temp_dir_option
        if skip_type_option:
            self.context.set_variable('gather_skip_type', skip_type_option)
        return True

    def __get_sql_result(self):
        try:
            file_path = os.path.join(self.report_path, "sql_result.txt")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = f.read()
            return data
        except Exception as e:
            self.stdio.error(e)
            return None

    def __print_result(self):
        if self.context.get_variable("adapted_version", default=True) == True:
            self.stdio.print(Fore.YELLOW + "\nGather scene results stored in this directory: {0}\n".format(self.report_path) + Style.RESET_ALL)
        return self.report_path
