#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/20
@file: odg_exception.py
@desc:
"""
import pprint


class ODGException(Exception):
    pass


class ODGIgnoreException(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class ODGFormatException(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class ODGConfNotFoundException(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class ODGArgsNotFoundException(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class ODGInvalidArgs(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class ODGSSHConnException(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class ODGDBConnException(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class ODGShellCmdException(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class ODGAPIException(ODGException):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)
