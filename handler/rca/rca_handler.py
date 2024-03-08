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
@time: 2023/12/22
@file: rca_handler.py
@desc:
"""
import datetime

from common.logger import logger
from handler.rca.rca_exception import RCANotNeedExecuteException
from handler.rca.rca_scene import rca_map
from utils.utils import node_cut_passwd_for_log


def scene_exist(scene_name):
    if scene_name in rca_map:
        return True
    else:
        return False


class RCAHandler:

    def __init__(self, cluster, nodes, obproxy_nodes,
                 result_path="./rca/"):
        self.rca_scene_parameters = None
        self.rca_scene = None
        self.cluster = cluster
        self.nodes = nodes
        self.obproxy_nodes = obproxy_nodes
        self.result_path = result_path

        # init input parameters
        self.report = None
        self.tasks = None
        logger.debug("RCAHandler init.cluster:{0}, init.nodes:{1}, init.obproxy_nodes:{2}, init.result_path:{3}".format(
            self.cluster.get(
                "ob_cluster_name") or self.cluster.get(
                "obproxy_cluster_name"), node_cut_passwd_for_log(self.nodes), node_cut_passwd_for_log(self.obproxy_nodes), self.result_path))

    def get_result_path(self):
        return self.result_path

    def handle(self, args):
        if getattr(args, "parameters"):
            self.rca_scene_parameters = getattr(args, "parameters", "")[0].strip()
        if getattr(args, "store_dir"):
            self.result_path = getattr(args, "store_dir", "./rca/")[0].strip()

        if getattr(args, "scene") and scene_exist(getattr(args, "scene")[0]):
            self.rca_scene = rca_map[getattr(args, "scene")[0]]
            self.result_path = "{0}/{1}_{2}".format(self.result_path, getattr(args, "scene")[0].strip(),
                                                     datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            self.rca_scene.init(self.cluster, self.nodes, self.obproxy_nodes,
                                env=self.rca_scene_parameters, result_path=self.result_path)

        else:
            raise Exception("rca_scene :{0} is not exist or not input".format(getattr(args, "scene", "")))

    # get all tasks
    def execute(self):
        try:
            self.rca_scene.execute()
        except RCANotNeedExecuteException as e:
            logger.warning("rca_scene.execute not need execute: {0}".format(e))
            pass
        except Exception as e:
            logger.error("rca_scene.execute err: {0}".format(e))
            raise Exception("rca_scene.execute err: {0}".format(e))
        try:
            self.rca_scene.export_result()
        except Exception as e:
            logger.error("rca_scene.export_result err: {0}".format(e))
            raise Exception("rca_scene.export_result err: {0}".format(e))

