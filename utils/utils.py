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
@file: utils.py
@desc:
"""
import datetime
import decimal
import json
import os
import re
import sys
import subprocess
import socket

from prettytable import PrettyTable

from common.logger import logger


def print_error(msg):
    logger.error("\033[0;31m%s\033[0m" % msg)


def check_version_gt_36():
    if sys.version_info[0] >= 3 and sys.version_info[1] >= 6:
        return True
    return False


def containVarInString(containVar, stringVar):
    try:
        if isinstance(stringVar, str):
            if stringVar.find(containVar):
                return True
            else:
                return False
        else:
            return False
    except Exception as e:
        pass


def execute_command(cmd):
    logger.debug("Executing: {}".format(cmd))
    output = None
    try:
        output = subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True)
    except subprocess.CalledProcessError as err:
        logger.error("Failed to execute cmd={}, returncode={}, output={}".format(cmd, err.returncode, err.output))
        raise
    logger.debug("Executed cmd={}, output={}".format(cmd, output))
    return output


def get_localhost_inner_ip():
    localhost_ip = "127.0.0.1"
    try:
        localhost_ip = socket.gethostbyname(socket.gethostname())
        return localhost_ip
    except Exception as e:
        return localhost_ip


def get_observer_ip_from_trace_id(content):
    if content[0] == 'Y' and len(content) >= 12:
        sep = content.find('-')
        uval = int(content[1:sep], 16)
        ip = uval & 0xffffffff
        port = (uval >> 32) & 0xffff
        return "%d.%d.%d.%d:%d" % ((ip >> 24 & 0xff), (ip >> 16 & 0xff), (ip >> 8 & 0xff), (ip >> 0 & 0xff), port)
    else:
        return ""


def skip_char(sub, b):
    if len(sub) <= 0:
        return False
    if sub[0] == b:
        sub = sub[1:]
        return True
    return False


def convert_to_number(s):
    if isinstance(s,int) or isinstance(s,decimal.Decimal):
        return s
    if s.startswith("-"):
        if s[1:].isdigit():
            return int(s)
        elif s[1:].isdecimal():  # 判断字符串是否全为数字或小数点
            return float(s)  # 如果是，转换为浮点数
        else:
            return str(s)  # 如果都不是，保持原样
    if s.isdigit():  # 判断字符串是否全为数字
        return int(s)  # 如果是，转换为整数
    elif s.isdecimal():  # 判断字符串是否全为数字或小数点
        return float(s)  # 如果是，转换为浮点数
    else:
        return str(s)  # 如果都不是，保持原样


def parse_range_string(range_str, nu):
    # parse_range_string: Determine whether variable 'nu' is within the range of 'range_str'
    # 提取范围字符串中的数字
    nu = int(nu)
    range_str = range_str.replace(" ", "")
    # range_str = range_str.replace(".", "")
    start, end = range_str[1:-1].split(',')
    need_less = True
    need_than = True
    # 将数字转换为整数
    if start.strip() == "*":
        need_less = False
    else:
        start = float(start.strip())
    if end.strip() == "*":
        need_than = False
    else:
        end = float(end.strip())
    logger.info("range_str is {0}".format(range_str))

    if need_less:
        if range_str[0] == "(":
            if nu <= start:
                return False
        elif range_str[0] == "[":
            if nu < start:
                return False
    if need_than:
        if range_str[-1] == ")":
            if nu >= end:
                return False
        elif range_str[-1] == "]":
            if nu > end:
                return False
    return True


def build_str_on_expr_by_dict(expr, variable_dict):
    s = expr
    d = variable_dict

    def replacer(match):
        key = match.group(1)
        return str(d.get(key, match.group(0)))

    return re.sub(r'#\{(\w+)\}', replacer, s)


def display_trace(uuid):
    print("If you want to view detailed obdiag logs, please run:' obdiag display-trace --trace_id {0} '".format(uuid))


def node_cut_passwd_for_log(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for key, value in obj.items():
            if key == "password" or key == "ssh_password":
                continue
            new_obj[key] = node_cut_passwd_for_log(value)
        return new_obj
    elif isinstance(obj, list):
        return [node_cut_passwd_for_log(item) for item in obj]
    else:
        return obj


def obcluster_cut_passwd_for_log(obcluster):
    new_obj = obcluster.copy()
    if "tenant_sys" in new_obj and "password" in new_obj["tenant_sys"]:
        del new_obj["tenant_sys"]["password"]
    return new_obj


def split_ip(ip_str):
    pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    result = re.findall(pattern, ip_str)
    return result
