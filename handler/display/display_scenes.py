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
@time: 2024/01/04
@file: display_scene_handler.py
@desc:
"""

import os
import re
from result_type import ObdiagResult
from stdio import SafeStdio
import datetime
from handler.display.scenes.base import SceneBase
from common.obdiag_exception import OBDIAGFormatException
from handler.display.scenes.list import DisplayScenesListHandler
from common.tool import DirectoryUtil
from common.tool import StringUtils
from common.scene import get_version_by_type
from colorama import Fore, Style
from common.tool import Util
from common.tool import TimeUtils
from common.ob_connector import OBConnector


class DisplaySceneHandler(SafeStdio):

    def __init__(self, context, display_pack_dir='./', tasks_base_path="~/.obdiag/display/tasks/", task_type="observer", is_inner=False):
        self.context = context
        self.stdio = context.stdio
        self.is_ssh = True
        self.report = None
        self.display_pack_dir = display_pack_dir
        self.yaml_tasks = {}
        self.code_tasks = []
        self.env = {}
        self.scene = "observer.base"
        self.tasks_base_path = tasks_base_path
        self.task_type = task_type
        self.variables = {}
        self.is_inner = is_inner
        self.temp_dir = '/tmp'
        if self.context.get_variable("display_timestamp", None):
            self.display_timestamp = self.context.get_variable("display_timestamp")
        else:
            self.display_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        self.cluster = self.context.cluster_config
        self.sys_connector = OBConnector(ip=self.cluster.get("db_host"), port=self.cluster.get("db_port"), username=self.cluster.get("tenant_sys").get("user"), password=self.cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=100)
        self.obproxy_nodes = self.context.obproxy_config['servers']
        self.ob_nodes = self.context.cluster_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.ob_nodes, self.stdio)
        if new_nodes:
            self.nodes = new_nodes
        return True

    def handle(self):
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init config failed")
        if not self.init_option():
            self.stdio.error('init option failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init option failed")
        self.context.set_variable('temp_dir', self.temp_dir)
        self.__init_variables()
        self.__init_task_names()
        self.execute()
        return ObdiagResult(ObdiagResult.SUCCESS_CODE)

    def execute(self):
        try:
            self.stdio.verbose("execute_tasks. the number of tasks is {0} ,tasks is {1}".format(len(self.yaml_tasks.keys()), self.yaml_tasks.keys()))
            for key, value in zip(self.yaml_tasks.keys(), self.yaml_tasks.values()):
                self.__execute_yaml_task_one(key, value)
            for task in self.code_tasks:
                self.__execute_code_task_one(task)
        except Exception as e:
            self.stdio.error("Internal error :{0}".format(e))

    def __init_db_connector(self):
        self.db_connector = OBConnector(ip=self.db_conn.get("host"), port=self.db_conn.get("port"), username=self.db_conn.get("user"), password=self.db_conn.get("password"), database=self.db_conn.get("database"), stdio=self.stdio, timeout=100)

    def __init_db_conn(self, env_option):
        try:       
            self.db_conn = StringUtils.parse_mysql_conn(cli_connection_string)
            if StringUtils.validate_db_info(self.db_conn):
                self.__init_db_connector()
            else:
                self.stdio.error("db connection information requird [db_connect = '-hxx -Pxx -uxx -pxx -Dxx'] but provided {0}, please check the --env {0}".format(env_dict))
                self.db_connector = self.sys_connector
        except Exception as e:
            self.stdio.exception("init db connector, error: {0}, please check --env option ")

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
                    self.stdio.error("get cluster.version failed")
                    return
                task = SceneBase(context=self.context, scene=task_data["task"], env=self.env, scene_variable_dict=self.variables, task_type=task_type, db_connector=self.db_connector)
                self.stdio.verbose("{0} execute!".format(task_name))
                task.execute()
                self.stdio.verbose("execute tasks end : {0}".format(task_name))
            else:
                self.stdio.error("can't get version")
        except Exception as e:
            self.stdio.error("__execute_yaml_task_one Exception : {0}".format(e))

    # execute code task
    def __execute_code_task_one(self, task_name):
        try:
            self.stdio.verbose("execute tasks is {0}".format(task_name))
            scene = {"name": task_name}
            task = SceneBase(context=self.context, scene=scene, env=self.env, mode='code', task_type=task_name)
            self.stdio.verbose("{0} execute!".format(task_name))
            task.execute()
            self.stdio.verbose("execute tasks end : {0}".format(task_name))
        except Exception as e:
            self.stdio.error("__execute_code_task_one Exception : {0}".format(e))

    def __init_task_names(self):
        if self.scene:
            new = re.sub(r'\{|\}', '', self.scene)
            items = re.split(r'[;,]', new)
            scene = DisplayScenesListHandler(self.context)
            for item in items:
                yaml_task_data = scene.get_one_yaml_task(item)
                is_code_task = scene.is_code_task(item)
                if is_code_task:
                    self.code_tasks.append(item)
                else:
                    if yaml_task_data:
                        self.yaml_tasks[item] = yaml_task_data
                    else:
                        self.stdio.error("Invalid Task :{0}".format(item))
            # hard code add display observer.base
            if len(self.code_tasks) > 0:
                yaml_task_base = scene.get_one_yaml_task("observer.base")
                self.yaml_tasks["observer.base"] = yaml_task_base
        else:
            self.stdio.error("get task name failed")

    def __init_variables(self):
        try:
            self.variables = {
                "observer_data_dir": self.ob_nodes[0].get("home_path") if self.ob_nodes and self.ob_nodes[0].get("home_path") else "",
                "obproxy_data_dir": self.obproxy_nodes[0].get("home_path") if self.obproxy_nodes and self.obproxy_nodes[0].get("home_path") else "",
                "from_time": self.from_time_str,
                "to_time": self.to_time_str,
            }
            self.stdio.verbose("display scene variables: {0}".format(self.variables))
        except Exception as e:
            self.stdio.error("init display scene variables failed, error: {0}".format(e))

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
        env_option = Util.get_option(options, 'env')
        scene_option = Util.get_option(options, 'scene')
        temp_dir_option = Util.get_option(options, 'temp_dir')
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
        else:
            self.stdio.print('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
        if scene_option:
            self.scene = scene_option
        if env_option:
            env_dict = StringUtils.parse_env_display(env_option)
            self.env = env_dict
            cli_connection_string = self.env.get("db_connect")
            if cli_connection_string != None:
                self.__init_db_conn(cli_connection_string)
            else:
                self.db_connector = self.sys_connector
        else:
            self.db_connector = self.sys_connector
        if temp_dir_option:
            self.temp_dir = temp_dir_option
        return True
