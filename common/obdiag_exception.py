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
@file: obdiag_exception.py
@desc:
"""
import pprint


class OBDIAGException(Exception):
    pass


class OBDIAGIgnoreException(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class OBDIAGFormatException(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class OBDIAGConfNotFoundException(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class OBDIAGArgsNotFoundException(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class OBDIAGInvalidArgs(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class OBDIAGSSHConnException(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class OBDIAGDBConnException(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class OBDIAGShellCmdException(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class OBDIAGAPIException(OBDIAGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)