# !/usr/bin/env python3
# -*-coding:utf-8 -*-

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
