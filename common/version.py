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
@time: 2022/6/22
@file: version.py
@desc:
"""

# obdiag version
OBDIAG_VERSION = '<VERSION>'
# obdiag build time
OBDIAG_BUILD_TIME = '<B_TIME>'


def get_obdiag_version():
    version = '''OceanBase Diagnostic Tool: %s
BUILD_TIME: %s
Copyright (C) 2022 OceanBase
License Mulan PSL v2: http://license.coscl.org.cn/MulanPSL2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
There is NO WARRANTY, to the extent permitted by law.''' % (
        OBDIAG_VERSION,
        OBDIAG_BUILD_TIME,
    )
    return version
