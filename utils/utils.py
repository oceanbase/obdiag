#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/22
@file: utils.py
@desc:
"""
import sys
import subprocess

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
