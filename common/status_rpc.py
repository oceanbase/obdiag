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
@file: status_rpc.py
@desc:
"""
from common.base_rpc import Request, Response


class StatusRequest(Request):
    def __init__(self, timestamp=None, request_id=None):
        super(StatusRequest, self).__init__(timestamp=timestamp)
        if request_id is not None:
            self.request_id = request_id

    def to_dict(self):
        parent_dict = super(StatusRequest, self).to_dict()
        return parent_dict


class StatusResponse(Response):
    def __init__(self, request, status_result_dict, timestamp=None):
        super(StatusResponse, self).__init__(request.request_id,
                                             timestamp=timestamp)
        self.status_result_dict = status_result_dict

    def to_dict(self):
        parent_dict = super(StatusResponse, self).to_dict()
        parent_dict["status_dict"] = self.status_result_dict
        return parent_dict

    def from_dict(self, content_dict):
        super(StatusResponse, self).from_dict(content_dict)
        self.status_result_dict = content_dict["status_dict"]
