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
@file: err.py
@desc:
"""

from __future__ import absolute_import, division, print_function


class OBDIAGErrorCode(object):

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return self.msg


class OBDIAGErrorCodeTemplate(object):

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
        self._str_ = ('OBDIAG-%04d: ' % code) + msg

    def format(self, *args, **kwargs):
        return OBDIAGErrorCode(
            self.code,
            self._str_.format(*args, **kwargs),
        )

    def __str__(self):
        return self.msg


class FixEval(object):

    DEL = 0
    SET = 1

    def __init__(self, operation, key, value=None, is_global=False):
        self.operation = operation
        self.key = key
        self.value = value
        self.is_global = is_global


class OBDIAGErrorSuggestion(object):

    def __init__(self, msg, auto_fix=False, fix_eval=[]):
        self.msg = msg
        self.auto_fix = auto_fix
        self.fix_eval = fix_eval


class OBDIAGErrorSuggestionTemplate(object):

    def __init__(self, msg, auto_fix=False, fix_eval=[]):
        self._msg = msg
        self.auto_fix = auto_fix
        self.fix_eval = fix_eval if isinstance(fix_eval, list) else [fix_eval]

    def format(self, *args, **kwargs):
        return OBDIAGErrorSuggestion(self._msg.format(*args, **kwargs), auto_fix=kwargs.get('auto_fix', self.auto_fix), fix_eval=kwargs.get('fix_eval', self.fix_eval))


class CheckStatus(object):

    FAIL = "FAIL"
    PASS = "PASS"
    WAIT = "WAIT"

    def __init__(self, status=WAIT, error=None, suggests=[]):
        self.status = status
        self.error = error
        self.suggests = suggests


SUG_SSH_FAILED = OBDIAGErrorSuggestionTemplate('Please check user config and network')
EC_SSH_CONNECT = OBDIAGErrorCodeTemplate(1013, '{user}@{ip} connect failed: {message}')
EC_SQL_EXECUTE_FAILED = OBDIAGErrorCodeTemplate(5000, "{sql} execute failed")
