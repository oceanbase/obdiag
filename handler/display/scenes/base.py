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
@time: 2024/08/31
@file: base.py
@desc:
"""
from stdio import SafeStdio
from common.scene import filter_by_version
from handler.display.step.base import Base


class SceneBase(SafeStdio):
    def __init__(self, context, scene, db_connector, report_dir=None, scene_variable_dict=None, env=None, mode="yaml", task_type="observer"):
        if env is None:
            env = {}
        if scene_variable_dict is None:
            scene_variable_dict = {}
        self.context = context
        self.stdio = context.stdio
        self.scene_variable_dict = scene_variable_dict
        self.scene = scene
        self.cluster = context.cluster_config
        self.ob_nodes = context.cluster_config['servers']
        self.obproxy_nodes = context.obproxy_config['servers']
        self.mode = mode
        self.env = env
        self.task_type = task_type
        self.db_connector = db_connector

    def execute(self):
        try:
            if self.mode == "yaml":
                if self.task_type == "observer":
                    self.__execute_yaml_mode(self.ob_nodes)
                elif self.task_type == "obproxy":
                    self.__execute_yaml_mode(self.obproxy_nodes)
                elif self.task_type == "other":
                    self.__execute_yaml_mode(self.ob_nodes)
                    self.__execute_yaml_mode(self.obproxy_nodes)
            elif self.mode == "code":
                self.__execute_code_mode()
            else:
                self.stdio.error("Unsupported mode. SKIP")
                raise Exception("Unsupported mode. SKIP")
        except Exception as e:
            raise Exception("execute failed, error: {0}".format(e))

    def __execute_yaml_mode(self, nodes):
        steps_nu = filter_by_version(self.scene, self.cluster, self.stdio)
        if steps_nu < 0:
            self.stdio.verbose("Unadapted by version. SKIP")
            return "Unadapted by version.SKIP"
        self.stdio.verbose("filter_by_version is return {0}".format(steps_nu))
        if len(nodes) == 0:
            self.stdio.warn("node is not exist")
            return
        node_number = 0
        for node in nodes:
            # self.stdio.print("run scene excute yaml mode in node: {0} start".format(StringUtils.node_cut_passwd_for_log(node['ip'], self.stdio)))
            steps = self.scene[steps_nu]
            nu = 1
            node_number = node_number + 1
            for step in steps["steps"]:
                try:
                    self.stdio.verbose("step nu: {0}".format(nu))
                    if len(self.cluster) == 0:
                        self.stdio.error("cluster is not exist")
                        return
                    step_run = Base(self.context, step, node, self.cluster, self.scene_variable_dict, self.env, node_number, self.db_connector)
                    self.stdio.verbose("step nu: {0} initted, to execute".format(nu))
                    step_run.execute()
                    self.scene_variable_dict = step_run.update_task_variable_dict()
                except Exception as e:
                    self.stdio.error("SceneBase execute Exception: {0}".format(e))
                    return
                self.stdio.verbose("step nu: {0} execute end ".format(nu))
                nu = nu + 1
            # self.stdio.print("run scene excute yaml mode in node: {0} end".format(StringUtils.node_cut_passwd_for_log(node['ip'], self.stdio)))
        self.stdio.verbose("run scene excute yaml mode in node")
