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
@time: 2023/9/20
@file: check_meta.py
@desc:
"""


class GlobalCheckMeta:
    _check_dict = {}

    def _init(self):
        global _check_dict
        self._check_dict = {}

    def set_value(self, key, value):
        self._check_dict[key] = value

    def get_value(self, key):
        try:
            return self._check_dict[key]
        except:
            print('get' + key + 'failed\r\n')

    def rm_value(self, key):
        try:
            return self._check_dict.pop(key)
        except:
            print('delete' + key + 'failed\r\n')


check_dict = GlobalCheckMeta()
check_dict.set_value(
    "check_verify_shell",
    '''
if ${new_expr}; then
    echo "true"
else
    echo "false"
fi
                ''',
)
