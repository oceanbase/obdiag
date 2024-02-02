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
@time: 2024/01/17
@file: string_utils.py
@desc:
"""

import re

def parse_mysql_cli_connection_string(cli_conn_str):
    db_info = {}
    # 处理密码选项，注意区分短选项和长选项的密码
    password_pattern = re.compile(r'(-p\s*|--password=)([^ ]*)')
    password_match = password_pattern.search(cli_conn_str)
    if password_match:
        password = password_match.group(2)
        db_info['password'] = password
        # 去除密码部分，避免后续解析出错
        cli_conn_str = cli_conn_str[:password_match.start()] + cli_conn_str[password_match.end():]

    # 模式匹配短选项
    short_opt_pattern = re.compile(r'-(\w)\s*(\S*)')
    matches = short_opt_pattern.finditer(cli_conn_str)
    for match in matches:
        opt = match.group(1)
        value = match.group(2)
        if opt == 'h':
            db_info['host'] = value
        elif opt == 'u':
            db_info['user'] = value
        elif opt == 'P':
            db_info['port'] = int(value)
        elif opt == 'D':
            db_info['database'] = value

    # 模式匹配长选项
    long_opt_pattern = re.compile(r'--(\w+)=([^ ]+)')
    long_matches = long_opt_pattern.finditer(cli_conn_str)
    for match in long_matches:
        opt = match.group(1)
        value = match.group(2)
        if opt == 'host':
            db_info['host'] = value
        elif opt == 'user':
            db_info['user'] = value
        elif opt == 'port':
            db_info['port'] = int(value)
        elif opt in ['dbname', 'database']:
            db_info['database'] = value

    # 如果存在命令行最后的参数，且不是一个选项，则认为是数据库名
    last_param = cli_conn_str.split()[-1]
    if last_param[0] != '-' and 'database' not in db_info:
        db_info['database'] = last_param
    return db_info

def validate_db_info(db_info):
    required_keys = {'database', 'host', 'user', 'port'}
    if not required_keys.issubset(db_info.keys()) or any(not value for value in db_info.values()):
        return False
    if not isinstance(db_info['port'], int):
        return False
    for key, value in db_info.items():
        if key != 'port' and not isinstance(value, str):
            return False
    return True

def parse_custom_env_string(env_string):
    env_dict = {}
    # 去除花括号
    inner_str = env_string[1:-1]
    pairs = inner_str.split(',')
    for pair in pairs:
        key_value = pair.strip().split('=')
        if len(key_value) == 2:
            key, value = key_value
            # 处理可能含有单引号或双引号的情况
            if value.startswith('"') and value.endswith('"'):
                value = value[1:-1]
            elif value.startswith("'") and value.endswith("'"):
                value = value[1:-1]
            env_dict[key.strip()] = value.strip()

    return env_dict