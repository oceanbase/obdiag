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
@time: 2022/6/20
@file: base_rpc.py
@desc:
"""
import random
from common.snowflake import SnowFlake
from utils.time_utils import get_current_us_timestamp


class Request(object):
    """
    All class inheriting Request must implement to_dict()
    """
    DUMP_KEY_LIST = ["request_timestamp", "request_id"]

    def __init__(self, timestamp=None):
        self.id_generator = SnowFlake(random.randint(100,199))
        self.request_timestamp = get_current_us_timestamp() if timestamp is None else timestamp
        self.request_id = self.generate_request_id()

    def generate_request_id(self):
        return self.id_generator.get_id()

    def to_dict(self):
        ret_dict = {}
        for key in self.DUMP_KEY_LIST:
            ret_dict[key] = getattr(self, key)
        return ret_dict


class Response(object):
    """
    All class inheriting Response class must implement to_dict() and from_dict()
    """
    DUMP_KEY_LIST = ["response_id", "response_timestamp", "error_code", "error_msg",
                     "unready_logs"]

    def __init__(self, request_id, timestamp=None):
        self.id_generator = SnowFlake(random.randint(100,199))
        self.response_timestamp = get_current_us_timestamp() if timestamp is None else timestamp
        self.request_id = request_id
        self.response_id = self.generate_request_id()
        self.error_code = 0
        self.error_msg = ""
        self.unready_logs = []

    def generate_request_id(self):
        return self.id_generator.get_id()

    def to_dict(self):
        ret_dict = {}
        for key in self.DUMP_KEY_LIST:
            ret_dict[key] = getattr(self, key)
        return ret_dict

    def from_dict(self, content_dict):
        for key in self.DUMP_KEY_LIST:
            setattr(self, key, content_dict[key])


class RawDictRequest(Request):
    def __init__(self, raw_dict, timestamp=None):
        super(RawDictRequest, self).__init__(timestamp=timestamp)

        self.payload = raw_dict

    def to_dict(self):
        parent_dict = super(RawDictRequest, self).to_dict()
        parent_dict["payload"] = self.payload
        return parent_dict


class RawDictResponse(Response):
    def __init__(self, request, raw_dict, request_id, timestamp=None, err_code=0, err_msg=""):
        super(RawDictResponse, self).__init__(request_id=request_id,
                                              timestamp=timestamp)
        self.payload = raw_dict
        self.error_code = err_code
        self.error_msg = err_msg

    def to_dict(self):
        parent_dict = super(RawDictResponse, self).to_dict()
        parent_dict["payload"] = self.payload
        return parent_dict

    def from_dict(self, content_dict):
        super(RawDictResponse, self).from_dict(content_dict)
        self.payload = content_dict["payload"]
