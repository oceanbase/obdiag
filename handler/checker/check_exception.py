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
@time: 2023/9/26
@file: check_exception.py
@desc:
"""
import pprint


class CheckException(Exception):
    def __init__(self, msg=None, obj=None):
        self.msg, self.obj = msg, obj

    def __repr__(self):
        return '%s %s' % (self.msg, self.obj is not None and pprint.pformat(self.obj) or '')

    def __str__(self):
        return repr(self)


class StepVerifyException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(StepVerifyException, self).__init__(msg, obj)


# for step to task
class StepExecuteFailException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(StepExecuteFailException, self).__init__(msg, obj)


class StepResultFalseException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(StepResultFalseException, self).__init__(msg, obj)


class StepResultFailException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(StepResultFailException, self).__init__(msg, obj)


class ResultFalseException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(ResultFalseException, self).__init__(msg, obj)


class ResultFailException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(ResultFailException, self).__init__(msg, obj)


class VerifyFalseException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(VerifyFalseException, self).__init__(msg, obj)


class VerifyFailException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(VerifyFailException, self).__init__(msg, obj)


class TaskException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(TaskException, self).__init__(msg, obj)
