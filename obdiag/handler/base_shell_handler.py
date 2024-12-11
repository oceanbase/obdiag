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
@time: 2022/6/21
@file: base_handler.py
@desc:
"""


# 所有需要连接到各个节点执行ssh命令的继承自该handler，通过 paramiko 到目标机器上执行shell命令来来得到结果


class BaseShellHandler(object):
    def __init__(self) -> None:
        self._stdio_func = None

    def _call_stdio(self, func, msg, *arg, **kwarg):
        if func not in self._stdio_func:
            return None
        return self._stdio_func[func](msg, *arg, **kwarg)
