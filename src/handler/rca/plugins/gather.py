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
@time: 2024/03/08
@file: gather.py
@desc: RCA plugin for gathering component logs (observer, obproxy, oms)
"""
import os.path

from src.handler.gather.gather_component_log import GatherComponentLogHandler


class Gather_log:
    """
    RCA plugin for gathering logs from different components.
    
    Supported targets:
    - observer: OceanBase observer logs
    - obproxy: OBProxy logs
    - oms: OMS logs (including CDC logs when oms_component_id is provided)
    
    For OMS CDC logs, use target="oms" with scope="cdc" and set oms_component_id.
    """
    
    # Mapping of target to node config getter
    TARGET_NODE_CONFIG = {
        'observer': lambda ctx: ctx.cluster_config.get("servers"),
        'obproxy': lambda ctx: ctx.get_variable('obproxy_nodes'),
        'oms': lambda ctx: ctx.get_variable("oms_nodes"),
    }

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
        """Initialize default parameters"""
        self.conf_map = {
            "gather_from": None,
            "gather_to": None,
            "gather_since": None,
            "gather_scope": "",
            "gather_target": "observer",
            "gather_oms_component_id": None,
            "filter_nodes_list": [],
            "store_dir": self.work_path,
        }
        self.greps_key = []

    def grep(self, key):
        """Add a grep keyword filter"""
        if key is None or len(key) < 1 or not isinstance(key, str):
            raise Exception("The keyword {0} cannot be empty!".format(key))
        self.greps_key.append(key)

    def _get_all_nodes(self, target):
        """Get all nodes for the specified target"""
        node_getter = self.TARGET_NODE_CONFIG.get(target)
        if node_getter is None:
            raise Exception("Unsupported target: {0}. Supported targets: {1}".format(
                target, list(self.TARGET_NODE_CONFIG.keys())))
        return node_getter(self.context) or []

    def _filter_nodes(self, all_nodes, filter_list):
        """Filter nodes based on filter_nodes_list"""
        if not filter_list:
            return []
        
        filtered = []
        for node in all_nodes:
            node_ip = node.get("ip")
            if node in filter_list:
                filtered.append(node)
                self.stdio.verbose("{0} is in the filter nodes list".format(node_ip))
            else:
                self.stdio.verbose("{0} is not in the filter nodes list, skipping".format(node_ip))
        return filtered

    def _prepare_save_path(self, save_path):
        """Prepare and validate save path"""
        if not save_path:
            save_path = self.work_path
        save_path = os.path.expanduser(save_path)
        
        if not os.path.exists(save_path):
            os.makedirs(save_path)
            self.stdio.verbose("{0} does not exist, created it.".format(save_path))
        
        self.work_path = save_path
        self.conf_map["store_dir"] = self.work_path
        return save_path

    def execute(self, save_path=""):
        """
        Execute log gathering.
        
        Args:
            save_path: Optional path to save gathered logs
            
        Returns:
            List of gathered log file paths
        """
        try:
            self.stdio.verbose("Gather_log execute, greps_key: {0}".format(self.greps_key))
            self._prepare_save_path(save_path)
            self.stdio.verbose("Gather_log execute, conf_map: {0}".format(self.conf_map))

            target = self.conf_map.get("gather_target", "observer")
            
            # Get and filter nodes
            all_nodes = self._get_all_nodes(target)
            filter_list = self.conf_map.get("filter_nodes_list", [])
            nodes_list = self._filter_nodes(all_nodes, filter_list) if filter_list else []

            # Create and initialize handler
            handler = GatherComponentLogHandler()
            handler.init(
                self.context,
                target=target,
                nodes=nodes_list,
                from_option=self.conf_map.get("gather_from"),
                to_option=self.conf_map.get("gather_to"),
                since=self.conf_map.get("gather_since"),
                scope=self.conf_map.get("gather_scope"),
                grep=self.greps_key,
                store_dir=self.work_path,
                oms_component_id=self.conf_map.get("gather_oms_component_id"),
            )

            # Execute gathering
            handler.handle()
            
            # Collect result files
            result_log_files = []
            result_log_dir_data = handler.open_all_file()
            for dir_name in result_log_dir_data:
                result_log_files.extend(result_log_dir_data[dir_name])
            
            self.reset()
            return result_log_files
            
        except Exception as e:
            raise Exception("rca plugins Gather_log execute error: {0}".format(e))

    def set_parameters(self, parameter, value):
        """
        Set a gather parameter.
        
        Args:
            parameter: Parameter name (without 'gather_' prefix)
            value: Parameter value
            
        Returns:
            True if parameter was set, False if parameter not found
        """
        parameter = "gather_{0}".format(parameter)
        if parameter in self.conf_map:
            self.conf_map[parameter] = value
            return True
        return False

    def reset(self):
        """Reset all parameters to default values"""
        self.init_parameters()
