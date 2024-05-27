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
@time: 2024/03/08
@file: gather.py
@desc:
"""
import os.path
import zipfile

from handler.gather.gather_log import GatherLogHandler
from common.tool import Util
from handler.gather.gather_obproxy_log import GatherObProxyLogHandler


class Gather_log:
    def __init__(self, context):
        self.conf_map = {}
        self.context = context
        self.stdio = context.stdio

        self.work_path = context.get_variable("store_dir") + "/gather_log"
        self.options = self.context.options
        self.greps_key = []
        self.nodes = []
        self.init_parameters()

    def init_parameters(self):
        self.conf_map["filter_nodes_list"] = []
        self.conf_map["gather_from"] = ""
        self.conf_map["gather_to"] = ""
        self.conf_map["gather_since"] = ""
        self.conf_map["gather_scope"] = ""
        self.conf_map["store_dir"] = self.work_path
        self.conf_map["gather_target"] = "observer"

    def grep(self, key):
        if key is None or len(key) < 1 or type(key) != str:
            raise Exception("The keyword cannot be empty!")
        self.greps_key.append(key)

    def execute(self, save_path=""):
        try:
            self.stdio.verbose("Gather_log execute,the greps_key: {0}".format(self.greps_key))
            if save_path is None or save_path == '':
                save_path = self.work_path
            save_path = os.path.expanduser(save_path)
            if os.path.exists(save_path):
                self.work_path = save_path
            else:
                os.mkdir(save_path)
                self.stdio.verbose("{0} is not exist, create it.".format(save_path))
                self.work_path = save_path
            self.conf_map["store_dir"] = self.work_path
            self.stdio.verbose("Gather_log execute,the conf_map: {0}".format(self.conf_map))
            if len(self.greps_key) == 0:
                self.stdio.error("The keyword cannot be empty!")
                raise Exception("The keyword cannot be empty!")
            self.context.set_variable("gather_grep", self.greps_key)
            self.stdio.verbose("gather_grep is {0}".format(self.greps_key))
            nodes_list = []
            if not self.conf_map["filter_nodes_list"] or len(self.conf_map["filter_nodes_list"]) == 0:
                self.context.set_variable("filter_nodes_list", self.conf_map["filter_nodes_list"])
                # execute on all nodes_list
            handle = None
            for conf in self.conf_map:
                self.context.set_variable(conf, self.conf_map[conf])
            if self.conf_map["gather_target"] == 'observer':
                all_node = self.context.cluster_config.get("servers")
                if self.conf_map["filter_nodes_list"] and len(self.conf_map["filter_nodes_list"] > 0):
                    # execute on specific nodes_list
                    for gather_node in self.conf_map["filter_nodes_list"]:
                        for node in all_node:
                            if node["ip"] in gather_node["ip"] and node["port"] in gather_node["port"]:
                                nodes_list.append(node)
                                self.stdio.verbose("{0} is in the nodes list".format(node.get("ip")))
                    self.conf_map["filter_nodes_list"] = nodes_list
                handle = GatherLogHandler(self.context)
            elif self.conf_map["gather_target"] == 'obproxy':
                all_node = self.context.get_variable('obproxy_nodes')
                if self.conf_map["filter_nodes_list"]:
                    # execute on specific nodes_list
                    for node in all_node:
                        if node not in self.conf_map["filter_nodes_list"]:
                            self.stdio.warn("{0} is not in the nodes list".format(node.get("ip")))
                            continue
                        else:
                            nodes_list.append(node)
                    self.conf_map["filter_nodes_list"] = nodes_list
                handle = GatherObProxyLogHandler(self.context)

            if handle is None:
                self.stdio.error("rca gather handle the target cannot be empty!")
                raise Exception("rca gather handle the target cannot be empty!")
            else:
                handle.handle()
            gather_result = handle.pack_dir_this_command
            zip_files = os.listdir(gather_result)
            result_log_files = []
            for zip_file in zip_files:
                if "zip" not in zip_file:
                    continue

                # open zip file
                self.stdio.verbose("open zip file: {0}".format(os.path.join(gather_result, zip_file)))
                with zipfile.ZipFile(os.path.join(gather_result, zip_file), 'r') as zip_ref:
                    # Extract all files to the current directory
                    zip_ref.extractall(gather_result)
            for file_name in os.listdir(gather_result):
                if "zip" not in file_name and "result_summary.txt" not in file_name:
                    log_dir = os.path.join(gather_result, file_name)
                    for log_file in os.listdir(log_dir):
                        result_log_files.append(os.path.join(log_dir, log_file))
                        self.stdio.verbose("result_log_files add {0}".format(os.path.join(log_dir, log_file)))

            self.reset()

            return result_log_files
        except Exception as e:
            raise Exception("rca plugins Gather_log execute error: {0}".format(e))

    def set_parameters(self, parameter, value):
        parameter = "gather_{0}".format(parameter)
        if parameter in self.conf_map:
            self.conf_map[parameter] = value
            return True
        return False

    def reset(self):
        self.init_parameters()
