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
@time: 2024/1/10
@file: base.py
@desc:
"""
from stdio import SafeStdio
from common.scene import filter_by_version
from handler.gather.step.base import Base
from common.tool import StringUtils
from handler.gather.scenes.sql_problem import SQLProblemScene
from handler.gather.scenes.cpu_high import CPUHighScene
from handler.gather.scenes.px_collect_log import SQLPXCollectLogScene


class SceneBase(SafeStdio):
    def __init__(self, context, scene, report_dir=None, scene_variable_dict={}, env={}, mode="yaml", task_type="observer"):
        self.context = context
        self.stdio = context.stdio
        self.scene_variable_dict = scene_variable_dict
        self.scene = scene
        self.cluster = context.cluster_config
        self.ob_nodes = context.cluster_config['servers']
        self.obproxy_nodes = context.obproxy_config['servers']
        self.report_dir = report_dir
        self.mode = mode
        self.env = env
        self.task_type = task_type

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
            self.stdio.print("run scene excute yaml mode in node: {0} start".format(StringUtils.node_cut_passwd_for_log(node['ip'], self.stdio)))
            steps = self.scene[steps_nu]
            nu = 1
            node_number = node_number + 1
            for step in steps["steps"]:
                try:
                    self.stdio.verbose("step nu: {0}".format(nu))
                    if len(self.cluster) == 0:
                        self.stdio.error("cluster is not exist")
                        return
                    step_run = Base(self.context, step, node, self.cluster, self.report_dir, self.scene_variable_dict, self.env, node_number)
                    self.stdio.verbose("step nu: {0} initted, to execute".format(nu))
                    step_run.execute()
                    self.scene_variable_dict = step_run.update_task_variable_dict()
                except Exception as e:
                    self.stdio.error("SceneBase execute Exception: {0}".format(e))
                    return
                self.stdio.verbose("step nu: {0} execute end ".format(nu))
                nu = nu + 1
            self.stdio.print("run scene excute yaml mode in node: {0} end".format(StringUtils.node_cut_passwd_for_log(node['ip'], self.stdio)))
        self.stdio.verbose("run scene excute yaml mode in node")

    def __execute_code_mode(self):
        if self.scene["name"] == "observer.perf_sql" or self.scene["name"] == "observer.sql_err":
            scene = SQLProblemScene(self.context, self.scene["name"], self.report_dir, self.scene_variable_dict, self.env)
        elif self.scene["name"] == "observer.cpu_high":
            scene = CPUHighScene(self.context, self.report_dir, self.scene_variable_dict, self.env)
        elif self.scene["name"] == "observer.px_collect_log":
            scene = SQLPXCollectLogScene(self.context, self.scene["name"], self.report_dir, self.scene_variable_dict, self.env)
        else:
            self.stdio.error("unsupported hard code scene {0}".format(self.scene["name"]))
            return
        try:
            self.stdio.verbose("hard code scene {0} execute start".format(self.scene["name"]))
            scene.execute()
            self.stdio.verbose("hard code scene {0} execute end".format(self.scene["name"]))
        except Exception as e:
            self.stdio.error("hard code scene execute failed, error :{0}".format(e))
