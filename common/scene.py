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
@time: 2024/01/16
@file: scene.py
@desc:
"""


from common.logger import logger
from utils.shell_utils import SshHelper
from utils.version_utils import compare_versions_greater
from common.command import get_observer_version, get_obproxy_version

def filter_by_version(scene, cluster):
    try:
        steps = scene
        steps_nu = 0
        # get observer version
        if "version" not in cluster or cluster["version"] == "":
            return steps_nu
        for now_steps in steps:
            if "version" in now_steps:
                steps_versions = now_steps["version"]
                if not isinstance(steps_versions, str):
                    logger.error("filter_by_version steps_version Exception : {0}".format("the type of version is not string"))
                    raise Exception("filter_by_version steps_version Exception : {0}".format("the type of version is not string"))
                version_real = cluster["version"]
                logger.info("version_int is {0} steps_versions is {1}".format(version_real, steps_versions))

                steps_versions = steps_versions.replace(" ", "")
                steps_versions = steps_versions[1:-1]
                steps_versions_list = steps_versions.split(",")
                minVersion = steps_versions_list[0]
                maxVersion = steps_versions_list[1]
                # min
                if minVersion == "*":
                    minVersion = "-1"
                if maxVersion == "*":
                    maxVersion = "999"
                if compare_versions_greater(version_real, minVersion) and compare_versions_greater(maxVersion, version_real):
                    break
            else:
                logger.info("not version in now_steps")
                break
            steps_nu = steps_nu + 1
        if steps_nu > len(steps) - 1:
            logger.warning("not version in this scene")
            return -1
        return steps_nu
    except Exception as e:
        logger.error("filter_by_version Exception : {0}".format(e))
        raise Exception("filter_by_version Exception : {0}".format(e))

def get_version(nodes, type):
    try:
        if len(nodes) < 1:
            raise Exception("input nodes is empty, please check your config")
        node = nodes[0]
        ssh = SshHelper(True, node.get("ip"), node.get("user"), node.get("password"), node.get("port"), node.get("private_key"), node)
        if type == "observer":
            version = get_observer_version(True, ssh, nodes[0]["home_path"])
        elif type == "obproxy":
            version = get_obproxy_version(True, ssh, nodes[0]["home_path"])
        return version
    except Exception as e:
        logger.error("can't get version, Exception: {0}".format(e))
        raise Exception("can't get version, Exception: {0}".format(e))

def get_obproxy_and_ob_version(obproxy_nodes, nodes, type):
    try:
        if type == "observer" or type == "other":
            if len(nodes) < 1:
                raise Exception("input nodes is empty, please check your config")
            node = nodes[0]
            ssh = SshHelper(True, node.get("ip"), node.get("user"), node.get("password"), node.get("port"), node.get("private_key"), node)
            version = get_observer_version(True, ssh, nodes[0]["home_path"])
        elif type == "obproxy":
            if len(nodes) < 1:
                raise Exception("input obproxy nodes is empty, please check your config")
            node = obproxy_nodes[0]
            ssh = SshHelper(True, node.get("ip"), node.get("user"), node.get("password"), node.get("port"), node.get("private_key"), node)
            version = get_obproxy_version(True, ssh, nodes[0]["home_path"])
        else:
            raise Exception(
                "type is {0} . No func to get the version".format(type))
        return version
    except Exception as e:
        logger.error("can't get version, Exception: {0}".format(e))
        raise Exception("can't get version, Exception: {0}".format(e))