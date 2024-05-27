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
@time: 2023/10/20
@file: ob_log_level.log
@desc:
"""


class OBLogLevel(object):
    CRITICAL = 50
    FATAL = 50
    ERROR = 40
    EDIAG = 35
    WARN = 30
    WDIAG = 25
    INFO = 20
    TRACE = 15
    DEBUG = 10
    NOTSET = 0

    def get_log_level(self, level_str):
        if level_str == "CRITICAL":
            return self.CRITICAL
        elif level_str == "FATAL":
            return self.FATAL
        elif level_str == "ERROR":
            return self.ERROR
        elif level_str == "EDIAG":
            return self.EDIAG
        elif level_str == "WARN":
            return self.WARN
        elif level_str == "WDIAG":
            return self.WDIAG
        elif level_str == "INFO":
            return self.INFO
        elif level_str == "TRACE":
            return self.TRACE
        elif level_str == "DEBUG":
            return self.DEBUG
        else:
            return self.NOTSET
