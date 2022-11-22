#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@file: odg_main.py
@desc:
"""

from common.logger import logger

from odg_client import ODGClient
from utils.parser_utils import ArgParser

CONFIG_PARSE_IGNORE_ATTR = ["start_date", "end_date"]
DEFAULT_SINCE_HOURS = 12

if __name__ == '__main__':
    odg = ODGClient().init()
    arg_parser = ArgParser(odg)
    odg_args = arg_parser.parse_argv()
    try:
        if odg_args.config:
            odg_args.config(odg_args)
    except AttributeError:
        logger.debug("object has no attribute 'config' pass quick config\n")
    try:
        if odg_args.gather_log:
            odg_args.gather_log(odg_args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_log' pass gather log\n")
