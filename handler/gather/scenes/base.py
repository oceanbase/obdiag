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

from common.logger import logger
from common.scene import filter_by_version
from handler.gather.step.base import Base
from utils.utils import node_cut_passwd_for_log
from handler.gather.scenes.sql_problem import SQLProblemScene
from handler.gather.scenes.cpu_high import CPUHighScene


class SceneBase(object):
    def __init__(self, scene, obproxy_nodes, ob_nodes, cluster, report_dir=None, scene_variable_dict={}, args=None, env={}, mode="yaml", task_type="observer"):
        self.scene_variable_dict = scene_variable_dict
        self.scene = scene
        self.cluster = cluster
        self.ob_nodes = ob_nodes
        self.obproxy_nodes = obproxy_nodes
        self.report_dir = report_dir
        self.args = args
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
                logger.error("Unsupported mode. SKIP")
                raise Exception("Unsupported mode. SKIP")
        except Exception as e:
            raise Exception("execute failed, error: {0}".format(e))

    def __execute_yaml_mode(self, nodes):
        steps_nu = filter_by_version(self.scene, self.cluster)
        if steps_nu < 0:
            logger.warning("Unadapted by version. SKIP")
            return "Unadapted by version.SKIP"
        logger.info("filter_by_version is return {0}".format(steps_nu))
        if len(nodes)==0:
            logger.error("node is not exist")
            return
        node_number = 0
        for node in nodes:
            logger.info("run scene in node: {0}".format(node_cut_passwd_for_log(node)))
            steps = self.scene[steps_nu]
            nu = 1
            node_number = node_number + 1
            for step in steps["steps"]:
                try:
                    logger.debug("step nu: {0}".format(nu))
                    if len(self.cluster)==0:
                        logger.error("cluster is not exist")
                        return
                    step_run = Base(step, node, self.cluster, self.report_dir, self.scene_variable_dict, self.args, self.env, node_number)
                    logger.info("step nu: {0} initted, to execute".format(nu))
                    step_run.execute()
                    self.scene_variable_dict = step_run.update_task_variable_dict()
                except Exception as e:
                    logger.error("SceneBase execute Exception: {0}".format(e))
                    return
                logger.info("step nu: {0} execute end ".format(nu))
                nu = nu + 1
        logger.info("scene execute end")

    def __execute_code_mode(self):
        if self.scene["name"] == "observer.perf_sql" or self.scene["name"] == "observer.sql_err":
            scene = SQLProblemScene(self.scene["name"], self.ob_nodes, self.obproxy_nodes, self.cluster, self.report_dir, self.scene_variable_dict, self.args, self.env)
        elif self.scene["name"] == "observer.cpu_high":
            scene = CPUHighScene(self.ob_nodes, self.cluster, self.report_dir, self.scene_variable_dict, self.args, self.env)
        else:
            logger.error("unsupported hard code scene {0}".format(self.scene["name"]))
            return
        try:
            logger.info("hard code scene {0} execute start".format(self.scene["name"]))
            scene.execute()
            logger.info("hard code scene {0} execute end".format(self.scene["name"]))
        except Exception as e:
            logger.error("hard code scene execute failed, error :{0}".format(e))

