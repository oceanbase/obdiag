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
@time: 2024/01/10
@file: register.py
@desc:
"""

from dataclasses import dataclass
import datetime


@dataclass
class RegisteredHardCodeScene:
    name: str
    command: str
    info_en: str
    info_cn: str


# 对于不适合通过yaml编排的复杂场景可以用这个类注册，注册后通过代码实现采集逻辑
db_connect = '-h127.0.0.1 -P2881 -utest@test -p****** -Dtest'
trace_id = 'Yxx'
estimated_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

hardcode_scene_list = [
    RegisteredHardCodeScene('observer.perf_sql', f'''obdiag gather scene run --scene=observer.perf_sql --env "{{db_connect='{db_connect}', trace_id='{trace_id}'}}"''', '[SQL performance problem]', '[SQL性能问题]'),
    RegisteredHardCodeScene('observer.sql_err', f'''obdiag gather scene run --scene=observer.sql_err --env "{{db_connect='{db_connect}', trace_id='{trace_id}'}}"''', '[SQL execution error]', '[SQL 执行出错]'),
    RegisteredHardCodeScene('observer.cpu_high', 'obdiag gather scene run --scene=observer.cpu_high', '[High CPU]', '[CPU高]'),
    RegisteredHardCodeScene(
        'observer.px_collect_log', f'''obdiag gather scene run --scene=observer.px_collect_log --env "{{trace_id='{trace_id}', estimated_time='{estimated_time}'}}"''', '[Collect error source node logs for SQL PX]', '[SQL PX 收集报错源节点日志]'
    ),
]
