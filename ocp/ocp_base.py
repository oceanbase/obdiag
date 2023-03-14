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
@time: 2022/6/24
# File       : ocp_api.py
# Description：
"""
import json
from common.logger import logger
from utils import utils


class OcpBase:
    def __init__(self, url, username, password):
        self.url = url
        self.username = username
        self.password = password

    def check_ocp_site(self):
        logger.info("Checking if OCP is accessible...")
        output = utils.execute_command(
            "curl -s --user {}:{} -X GET '{}/api/v2/git-info'"
            .format(self.username, self.password, self.url))
        response = json.loads(output)
        logger.info("check ocp response: {0}".format(response))
        try:
            response["commitId"]
        except KeyError:
            raise Exception("OCP is inaccessible because: {}".format(response["error"]))
