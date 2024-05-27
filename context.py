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
@file: context.py
@desc:
"""
from __future__ import absolute_import, division, print_function
from optparse import Values


class HandlerContextNamespace:

    def __init__(self, spacename):
        self.spacename = spacename
        self._variables = {}
        self._return = {}
        self._options = Values()

    @property
    def variables(self):
        return self._variables

    def get_variable(self, name, default=None):
        return self._variables.get(name, default)

    def set_variable(self, name, value):
        self._variables[name] = value

    def get_option(self, name, default=None):
        return self._variables.get(name, default)

    def set_option(self, name, value):
        self._options[name] = value

    def get_return(self, handler_name):
        ret = self._return.get(handler_name)
        if isinstance(ret, HandlerReturn):
            return ret
        return None

    def set_return(self, handler_name, handler_return):
        self._return[handler_name] = handler_return


class HandlerReturn(object):

    def __init__(self, value=False, *arg, **kwargs):
        self._return_value = value
        self._return_args = arg
        self._return_kwargs = kwargs

    def __nonzero__(self):
        return self.__bool__()

    def __bool__(self):
        return True if self._return_value else False

    @property
    def value(self):
        return self._return_value

    @property
    def args(self):
        return self._return_args

    @property
    def kwargs(self):
        return self._return_kwargs

    def get_return(self, key, default=None):
        return self.kwargs.get(key, default)

    def set_args(self, *args):
        self._return_args = args

    def set_kwargs(self, **kwargs):
        self._return_kwargs = kwargs

    def set_return(self, value):
        self._return_value = value

    def return_true(self, *args, **kwargs):
        self.set_return(True)
        self.set_args(*args)
        self.set_kwargs(**kwargs)

    def return_false(self, *args, **kwargs):
        self.set_return(False)
        self.set_args(*args)
        self.set_kwargs(**kwargs)


class HandlerContext(object):

    def __init__(self, handler_name=None, namespace=None, namespaces=None, cluster_config=None, obproxy_config=None, ocp_config=None, inner_config=None, cmd=None, options=None, stdio=None):
        self.namespace = HandlerContextNamespace(namespace)
        self.namespaces = namespaces
        self.handler_name = handler_name
        self.cluster_config = cluster_config
        self.obproxy_config = obproxy_config
        self.ocp_config = ocp_config
        self.inner_config = inner_config
        self.cmds = cmd
        self.options = options
        self.stdio = stdio
        self._return = HandlerReturn()

    def get_return(self, handler_name=None, spacename=None):
        if spacename:
            namespace = self.namespaces.get(spacename)
        else:
            namespace = self.namespace
        if handler_name is None:
            handler_name = self.handler_name
        return namespace.get_return(handler_name) if namespace else None

    def return_true(self, *args, **kwargs):
        self._return.return_true(*args, **kwargs)
        self.namespace.set_return(self.handler_name, self._return)

    def return_false(self, *args, **kwargs):
        self._return.return_false(*args, **kwargs)
        self.namespace.set_return(self.handler_name, self._return)

    def get_variable(self, name, spacename=None, default=None):
        if spacename:
            namespace = self.namespaces.get(spacename)
        else:
            namespace = self.namespace
        return namespace.get_variable(name, default) if namespace else None

    def set_variable(self, name, value):
        self.namespace.set_variable(name, value)

    def get_option(self, name, spacename=None, default=None):
        if spacename:
            namespace = self.namespaces.get(spacename)
        else:
            namespace = self.namespace
        return namespace.get_option(name, default) if namespace else None

    def set_option(self, name, value):
        self.namespace.set_option(name, value)
