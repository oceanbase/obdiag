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
@time: 2022/6/21
@file: base_handler.py
@desc:
"""
import requests

from common.logger import logger


# 所有需要使用http的handler继承自该handler
class BaseHttpHandler(object):
    def __init__(self, ocp):
        self.ocp = ocp
        self.ocp_user = ocp["login"]["user"]
        self.ocp_password = ocp["login"]["password"]
        self.ocp_url = ocp["login"]["url"]
        self.auth = (self.ocp_user, self.ocp_password)

    @staticmethod
    def http_framework(func, retry=3):
        resp = {
            "error": False,
        }
        request_exception_msg = None
        http_resp = None
        for retry in range(retry, 0, -1):
            try:
                http_resp = func()
            except Exception as e:
                logger.warning("Oceanbase Diagnostic Tool Request exception: {0}. retry={1}".format(e, retry))
                request_exception_msg = "{0}".format(e)
                continue
            if http_resp is None:
                request_exception_msg = "resp obj is None"
                continue
            request_exception_msg = None
            if http_resp.status_code != 200:
                logger.warning("Oceanbase Diagnostic Tool Request get {0} status code. retry={1}".format(
                    http_resp.status_code, retry))
            else:
                break
        # handle error in http request
        if request_exception_msg is not None:
            resp["error"] = True
            resp["error_msg"] = "Oceanbase Diagnostic Tool Request exception: {0}.".format(request_exception_msg)
            return resp
        if http_resp.status_code != 200:
            resp["error"] = True
            logger.error("Oceanbase Diagnostic Tool Request get {0} status code.".format(http_resp.status_code))
            resp["error_msg"] = "Oceanbase Diagnostic Tool Request get {0} status code.".format(http_resp.status_code)
            return resp
        resp_dict = http_resp.json()
        resp["raw_resp"] = resp_dict
        # handle error
        if resp_dict["error_code"] != 0:
            resp["error"] = True
            resp["error_msg"] = resp_dict["error_msg"]
        return resp

    @classmethod
    def http_post_json(cls, url, data_dict, param, retry=3, timeout=300):
        return cls.http_framework(
            lambda: requests.post(url, json=data_dict, param=param, timeout=timeout), retry=retry)

    @classmethod
    def http_get(cls, url, data_dict, param, retry=3, timeout=300):
        return cls.http_framework(
            lambda: requests.get(url, json=data_dict, param=param, timeout=timeout), retry=retry)

    @staticmethod
    def download(url, as_file_path, auth, timeout=300):
        with open(as_file_path, "wb") as write_fd:
            write_fd.write(requests.get(url, auth=auth, timeout=timeout).content)
        return as_file_path
