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
@time: 2024/4/2
@file: types.py
@desc:
"""

from __future__ import absolute_import, division, print_function

import os
import re
import uuid
import traceback

__all__ = ("Moment", "Time", "Capacity", "CapacityWithB", "CapacityMB", "StringList", "Dict", "List", "StringOrKvList", "Double", "Boolean", "Integer", "String", "Path", "SafeString", "PathList", "SafeStringList", "DBUrl", "WebUrl", "OBUser")


class Null(object):

    def __init__(self):
        pass


class ConfigItemType(object):
    TYPE_STR = None
    NULL = Null()

    def __init__(self, s):
        try:
            self._origin = s
            self._value = 0
            self.value = self.NULL
            self._format()
            if self.value == self.NULL:
                self.value = self._origin
        except:
            raise Exception("'%s' is not %s" % (self._origin, self._type_str))

    @property
    def _type_str(self):
        if self.TYPE_STR is None:
            self.TYPE_STR = str(self.__class__.__name__).split('.')[-1]
        return self.TYPE_STR

    def _format(self):
        raise NotImplementedError

    def __str__(self):
        return str(self._origin)

    def __hash__(self):
        return self._origin.__hash__()

    @property
    def __cmp_value__(self):
        return self._value

    def __eq__(self, value):
        if value is None:
            return False
        return self.__cmp_value__ == value.__cmp_value__

    def __gt__(self, value):
        if value is None:
            return True
        return self.__cmp_value__ > value.__cmp_value__

    def __ge__(self, value):
        if value is None:
            return True
        return self.__eq__(value) or self.__gt__(value)

    def __lt__(self, value):
        if value is None:
            return False
        return self.__cmp_value__ < value.__cmp_value__

    def __le__(self, value):
        if value is None:
            return False
        return self.__eq__(value) or self.__lt__(value)


class Moment(ConfigItemType):

    def _format(self):
        if self._origin:
            if self._origin.upper() == 'DISABLE':
                self._value = 0
            else:
                r = re.match('^(\d{1,2}):(\d{1,2})$', self._origin)
                h, m = r.groups()
                h, m = int(h), int(m)
                if 0 <= h <= 23 and 0 <= m <= 60:
                    self._value = h * 60 + m
                else:
                    raise Exception('Invalid Value')
        else:
            self._value = 0


class Time(ConfigItemType):
    UNITS = {'ns': 0.000000001, 'us': 0.000001, 'ms': 0.001, 's': 1, 'm': 60, 'h': 3600, 'd': 86400}

    def _format(self):
        if self._origin:
            self._origin = str(self._origin).strip()
            if self._origin.isdigit():
                n = self._origin
                unit = self.UNITS['s']
            else:
                r = re.match('^(\d+)(\w+)$', self._origin.lower())
                n, u = r.groups()
            unit = self.UNITS.get(u.lower())
            if unit:
                self._value = int(n) * unit
            else:
                raise Exception('Invalid Value')
        else:
            self._value = 0


class DecimalValue:

    def __init__(self, value, precision=None):
        if isinstance(value, str):
            self.value = float(value)
        else:
            self.value = value
        self.precision = precision

    def __repr__(self):
        if self.precision is not None:
            return "%.*f" % (self.precision, self.value)
        return str(self.value)

    def __add__(self, other):
        if isinstance(other, DecimalValue):
            return DecimalValue(self.value + other.value, self.precision if self.precision is not None else other.precision)
        return DecimalValue(self.value + other, self.precision)

    def __sub__(self, other):
        if isinstance(other, DecimalValue):
            return DecimalValue(self.value - other.value, self.precision if self.precision is not None else other.precision)
        return DecimalValue(self.value - other, self.precision)

    def __mul__(self, other):
        if isinstance(other, DecimalValue):
            return DecimalValue(self.value * other.value, self.precision if self.precision is not None else other.precision)
        return DecimalValue(self.value * other, self.precision)

    def __truediv__(self, other):
        if isinstance(other, DecimalValue):
            return DecimalValue(self.value / other.value, self.precision if self.precision is not None else other.precision)
        return DecimalValue(self.value / other, self.precision)


class Capacity(ConfigItemType):
    UNITS = {"B": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40, "P": 1 << 50}

    LENGTHS = {"B": 4, "K": 8, "M": 12, "G": 16, "T": 20, "P": 24}

    def __init__(self, s, precision=0):
        self.precision = precision
        super(Capacity, self).__init__(s)

    def __str__(self):
        return str(self.value)

    @property
    def btyes(self):
        return self._value

    def _format(self):
        if self._origin:
            if not isinstance(self._origin, str) or self._origin.strip().isdigit():
                self._origin = int(float(self._origin))
                n = self._origin
                unit = self.UNITS['B']
                for u in self.LENGTHS:
                    if len(str(self._origin)) < self.LENGTHS[u]:
                        break
                else:
                    u = 'P'
            else:
                groups = re.match("^(\d+)\s*([BKMGTP])((IB)|B)?\s*$", self._origin.upper())
                if not groups:
                    raise ValueError("Invalid capacity string: %s" % self._origin)
                n, u, _, _ = groups.groups()
                unit = self.UNITS.get(u.upper())
            if unit:
                self._value = int(n) * unit
                self.value = str(DecimalValue(self._value, self.precision) / self.UNITS[u]) + u
            else:
                raise Exception('Invalid Value')
        else:
            self._value = 0
            self.value = str(DecimalValue(0, self.precision))


class CapacityWithB(Capacity):

    def __init__(self, s):
        super(CapacityWithB, self).__init__(s, precision=0)

    def _format(self):
        super(CapacityWithB, self)._format()
        self.value = self.value + 'B'


class CapacityMB(Capacity):

    def _format(self):
        super(CapacityMB, self)._format()
        if isinstance(self._origin, str) and self._origin.isdigit():
            self.value = self._origin + 'M'
            self._value *= self.UNITS['M']
        if not self._origin:
            self.value = '0M'


class StringList(ConfigItemType):

    def _format(self):
        if self._origin:
            self._origin = str(self._origin).strip()
            self._value = self._origin.split(';')
        else:
            self._value = []


class Dict(ConfigItemType):

    def _format(self):
        if self._origin:
            if not isinstance(self._origin, dict):
                raise Exception("Invalid Value")
            self._value = self._origin
        else:
            self._value = self.value = {}


class List(ConfigItemType):

    def _format(self):
        if self._origin:
            if not isinstance(self._origin, list):
                raise Exception("Invalid value: {} is not a list.".format(self._origin))
            self._value = self._origin
        else:
            self._value = self.value = []


class StringOrKvList(ConfigItemType):

    def _format(self):
        if self._origin:
            if not isinstance(self._origin, list):
                raise Exception("Invalid value: {} is not a list.".format(self._origin))
            for item in self._origin:
                if not item:
                    continue
                if not isinstance(item, (str, dict)):
                    raise Exception("Invalid value: {} should be string or key-value format.".format(item))
                if isinstance(item, dict):
                    if len(item.keys()) != 1:
                        raise Exception("Invalid value: {} should be single key-value format".format(item))
            self._value = self._origin
        else:
            self._value = self.value = []


class Double(ConfigItemType):

    def _format(self):
        self.value = self._value = float(self._origin) if self._origin else 0


class Boolean(ConfigItemType):

    def _format(self):
        if isinstance(self._origin, bool):
            self._value = self._origin
        else:
            _origin = str(self._origin).lower()
            if _origin == 'true':
                self._value = True
            elif _origin == 'false':
                self._value = False
            elif _origin.isdigit():
                self._value = bool(self._origin)
            else:
                raise Exception('%s is not Boolean' % _origin)
        self.value = self._value


class Integer(ConfigItemType):

    def _format(self):
        if self._origin is None:
            self._value = 0
            self._origin = 0
        else:
            _origin = str(self._origin)
            try:
                self.value = self._value = int(_origin)
            except:
                raise Exception('%s is not Integer' % _origin)


class String(ConfigItemType):

    def _format(self):
        self.value = self._value = str(self._origin) if self._origin else ''


# this type is used to ensure the parameter is a valid oceanbase user
class OBUser(ConfigItemType):
    OB_USER_PATTERN = re.compile("^[a-zA-Z0-9_\.-]+(@[a-zA-Z0-9_\.-]+)?(#[a-zA-Z0-9_\.-]+)?$")

    def _format(self):
        if not self.OB_USER_PATTERN.match(str(self._origin)):
            raise Exception("%s is not a valid config" % self._origin)
        self.value = self._value = str(self._origin) if self._origin else ''


# this type is used to ensure the parameter not containing special characters to inject command
class SafeString(ConfigItemType):
    SAFE_STRING_PATTERN = re.compile("^[a-zA-Z0-9\u4e00-\u9fa5\-_:@/\.]*$")

    def _format(self):
        if not self.SAFE_STRING_PATTERN.match(str(self._origin)):
            raise Exception("%s is not a valid config" % self._origin)
        self.value = self._value = str(self._origin) if self._origin else ''


# this type is used to ensure the parameter not containing special characters to inject command
class SafeStringList(ConfigItemType):
    SAFE_STRING_PATTERN = re.compile("^[a-zA-Z0-9\u4e00-\u9fa5\-_:@/\.]*$")

    def _format(self):
        if self._origin:
            self._origin = str(self._origin).strip()
            self._value = self._origin.split(';')
            for v in self._value:
                if not self.SAFE_STRING_PATTERN.match(v):
                    raise Exception("%s is not a valid config" % v)
        else:
            self._value = []


# this type is used to ensure the parameter is a valid path by checking it's only certaining certain characters and not crossing path
class Path(ConfigItemType):
    PATH_PATTERN = re.compile("^[a-zA-Z0-9\u4e00-\u9fa5\-_:@/\.]*$")

    def _format(self):
        parent_path = "/{0}".format(uuid.uuid4().hex)
        absolute_path = "/".join([parent_path, str(self._origin)])
        normalized_path = os.path.normpath(absolute_path)

        if not (self.PATH_PATTERN.match(str(self._origin)) and normalized_path.startswith(parent_path)):
            raise Exception("%s is not a valid path" % self._origin)
        self.value = self._value = str(self._origin) if self._origin else ''


# this type is used to ensure the parameter is a valid path by checking it's only certaining certain characters and not crossing path
class PathList(ConfigItemType):
    PATH_PATTERN = re.compile("^[a-zA-Z0-9\u4e00-\u9fa5\-_:@/\.]*$")

    def _format(self):
        parent_path = "/{0}".format(uuid.uuid4().hex)
        if self._origin:
            self._origin = str(self._origin).strip()
            self._value = self._origin.split(';')
            for v in self._value:
                absolute_path = "/".join([parent_path, v])
                normalized_path = os.path.normpath(absolute_path)
                if not (self.PATH_PATTERN.match(v) and normalized_path.startswith(parent_path)):
                    raise Exception("%s is not a valid path" % v)
        else:
            self._value = []


# this type is used to ensure the parameter is a valid database connection url
class DBUrl(ConfigItemType):
    DBURL_PATTERN = re.compile("^jdbc:(mysql|oceanbase):(\/\/)([a-zA-Z0-9_.-]+)(:[0-9]{1,5})?\/([a-zA-Z0-9_\-]+)(\?[a-zA-Z0-9_&;=.-]*)?$")

    def _format(self):
        if not self.DBURL_PATTERN.match(str(self._origin)):
            raise Exception("%s is not a valid config" % self._origin)
        self.value = self._value = str(self._origin) if self._origin else ''


# this type is used to ensure the parameter is a valid web url
class WebUrl(ConfigItemType):
    WEBURL_PATTERN = re.compile("^(https?:\/\/)?([\da-z_.-]+)(:[0-9]{1,5})?([\/\w \.-]*)*\/?(?:\?[\w&=_.-]*)?$")

    def _format(self):
        if not self.WEBURL_PATTERN.match(str(self._origin)):
            raise Exception("%s is not a valid config" % self._origin)
        self.value = self._value = str(self._origin) if self._origin else ''
