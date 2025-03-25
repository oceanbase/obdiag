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

from src.handler.gather.gather_component_log import GatherComponentLogHandler


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
        self.conf_map["gather_from"] = None
        self.conf_map["gather_to"] = None
        self.conf_map["gather_since"] = None
        self.conf_map["gather_scope"] = ""
        self.conf_map["store_dir"] = self.work_path
        self.conf_map["gather_target"] = "observer"
        self.conf_map["gather_oms_component_id"] = None
        self.greps_key = []

    def grep(self, key):
        if key is None or len(key) < 1 or type(key) != str:
            raise Exception("The keyword {0} cannot be empty!".format(key))
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
            self.stdio.verbose("gather_grep is {0}".format(self.greps_key))
            nodes_list = []
            # execute on all nodes_list
            handler = None
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
                handler = GatherComponentLogHandler()
                handler.init(
                    self.context,
                    target="observer",
                    nodes=nodes_list,
                    from_option=self.conf_map.get("gather_from"),
                    to_option=self.conf_map.get("gather_to"),
                    since=self.conf_map.get("gather_since"),
                    scope=self.conf_map.get("gather_scope"),
                    grep=self.greps_key,
                    store_dir=self.work_path,
                )

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
                handler = GatherComponentLogHandler()
                handler.init(
                    self.context,
                    target="obproxy",
                    nodes=nodes_list,
                    from_option=self.conf_map.get("gather_from"),
                    to_option=self.conf_map.get("gather_to"),
                    since=self.conf_map.get("gather_since"),
                    scope=self.conf_map.get("gather_scope"),
                    grep=self.greps_key,
                    store_dir=self.work_path,
                )
            elif self.conf_map["gather_target"] == 'oms':
                all_node = self.context.get_variable("oms_nodes")
                if self.conf_map["filter_nodes_list"]:
                    # execute on specific nodes_list
                    for node in all_node:
                        if node not in self.conf_map["filter_nodes_list"]:
                            self.stdio.warn("{0} is not in the nodes list".format(node.get("ip")))
                            continue
                        else:
                            nodes_list.append(node)
                    self.conf_map["filter_nodes_list"] = nodes_list
                handler = GatherComponentLogHandler()
                handler.init(
                    self.context,
                    target="oms",
                    nodes=nodes_list,
                    from_option=self.conf_map.get("gather_from"),
                    to_option=self.conf_map.get("gather_to"),
                    since=self.conf_map.get("gather_since"),
                    scope=self.conf_map.get("gather_scope"),
                    grep=self.greps_key,
                    store_dir=self.work_path,
                    oms_component_id=self.conf_map.get("gather_oms_component_id"),
                )
            elif self.conf_map["gather_target"] == 'oms_cdc':
                all_node = self.context.get_variable("oms_nodes")
                if self.conf_map["filter_nodes_list"]:
                    # execute on specific nodes_list
                    for node in all_node:
                        if node not in self.conf_map["filter_nodes_list"]:
                            self.stdio.warn("{0} is not in the nodes list".format(node.get("ip")))
                            continue
                        else:
                            nodes_list.append(node)
                    self.conf_map["filter_nodes_list"] = nodes_list
                handler = GatherComponentLogHandler()
                handler.init(
                    self.context,
                    target="oms_cdc",
                    nodes=nodes_list,
                    from_option=self.conf_map.get("gather_from"),
                    to_option=self.conf_map.get("gather_to"),
                    since=self.conf_map.get("gather_since"),
                    scope=self.conf_map.get("gather_scope"),
                    grep=self.greps_key,
                    store_dir=self.work_path,
                    oms_component_id=self.conf_map.get("gather_oms_component_id"),
                )

            if handler is None:
                self.stdio.error("rca gather handle the target cannot be empty!")
                raise Exception("rca gather handle the target cannot be empty!")
            else:
                handler.handle()
            result_log_files = []
            result_log_dir_data = handler.open_all_file()
            for dir_name in result_log_dir_data:
                result_log_files.extend(result_log_dir_data[dir_name])
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
