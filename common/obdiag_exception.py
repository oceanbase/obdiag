#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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