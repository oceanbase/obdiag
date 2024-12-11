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
@file: log.py
@desc:
"""

from __future__ import absolute_import, division, print_function
import logging


class Logger(logging.Logger):

    def __init__(self, name, level=logging.DEBUG):
        super(Logger, self).__init__(name, level)
        self.buffer = []
        self.buffer_size = 0

    def _log(self, level, msg, args, end='\n', **kwargs):
        return super(Logger, self)._log(level, msg, args, **kwargs)
