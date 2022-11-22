#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
        # log_filename -> (log_size, grepped_log_size, log_datetime)
        self.status_result_dict = status_result_dict

    def to_dict(self):
        parent_dict = super(StatusResponse, self).to_dict()
        parent_dict["status_dict"] = self.status_result_dict
        return parent_dict

    def from_dict(self, content_dict):
        super(StatusResponse, self).from_dict(content_dict)
        self.status_result_dict = content_dict["status_dict"]
