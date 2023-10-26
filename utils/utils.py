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

def display_trace(uuid):
    print("If you want to view detailed obdiag logs, please run:'obdiag display-trace --trace_id {0}'".format(uuid))
