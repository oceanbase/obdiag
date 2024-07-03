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
from common.tool import StringUtils
from common.command import get_observer_version, get_obproxy_version


def filter_by_version(scene, cluster, stdio=None):
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
                    stdio.exception("filter_by_version steps_version Exception : {0}".format("the type of version is not string"))
                    raise Exception("filter_by_version steps_version Exception : {0}".format("the type of version is not string"))
                version_real = cluster["version"]
                stdio.verbose("version_int is {0} steps_versions is {1}".format(version_real, steps_versions))

                steps_versions = steps_versions.replace(" ", "")
                steps_versions_data = steps_versions[1:-1]
                steps_versions_list = steps_versions_data.split(",")
                minVersion = steps_versions_list[0]
                maxVersion = steps_versions_list[1]
                # min
                if minVersion == "*":
                    minVersion = "-1"
                if maxVersion == "*":
                    maxVersion = "999"
                stdio.verbose("minVersion is {0}, maxVersion is {1}".format(minVersion, maxVersion))
                if steps_versions[0] == "[" and version_real == minVersion:
                    break
                if steps_versions[-1] == "]" and version_real == maxVersion:
                    break
                if StringUtils.compare_versions_greater(version_real, minVersion) and StringUtils.compare_versions_greater(maxVersion, version_real):
                    break
            else:
                stdio.verbose("not version in now_steps")
                break
            steps_nu = steps_nu + 1
        if steps_nu > len(steps) - 1:
            stdio.verbose("not version in this scene")
            return -1
        return steps_nu
    except Exception as e:
        stdio.exception("filter_by_version Exception : {0}".format(e))
        raise Exception("filter_by_version Exception : {0}".format(e))


def get_version_by_type(context, type, stdio=None):
    try:
        stdio = context.stdio
        version = ""
        if type == "observer" or type == "other":
            try:
                version = get_observer_version(context)
            except Exception as e:
                stdio.warn("get observer version fail . Exception:{0}".format(e))
                raise Exception("can't get observer version, Exception: {0}".format(e))
        elif type == "obproxy":
            version = get_obproxy_version(context)
        else:
            raise Exception("type is {0} . No support to get the version".format(type))
        return version
    except Exception as e:
        stdio.exception("can't get {0} version, Exception: {1}".format(type, e))
        raise Exception("can't get {0} version, Exception: {1}".format(type, e))
