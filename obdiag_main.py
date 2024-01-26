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
@file: obdiag_main.py
@desc:
"""

from common.logger import logger

from obdiag_client import OBDIAGClient
from telemetry.telemetry import telemetry
from utils.parser_utils import ArgParser

CONFIG_PARSE_IGNORE_ATTR = ["start_date", "end_date"]
DEFAULT_SINCE_HOURS = 12


def pharse_config(args):
    try:
        if args.config:
            args.config(args)
    except AttributeError:
        logger.debug("object has no attribute 'config' pass quick config\n")


def gather_log(args):
    try:
        if args.gather_log:
            args.gather_log(args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_log' pass gather log\n")


def gather_awr(args):
    try:
        if args.gather_awr:
            args.gather_awr(args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_awr' pass gather awr\n")


def gather_sysstat(args):
    try:
        if args.gather_sysstat:
            args.gather_sysstat(args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_sysstat' pass gather sysstat info\n")


def gather_perf(args):
    try:
        if args.gather_perf:
            args.gather_perf(args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_perf' pass gather perf info\n")


def gather_plan_monitor(args):
    try:
        if args.gather_plan_monitor:
            args.gather_plan_monitor(args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_plan_monitor' pass gather ob sql plan monitor\n")


def gather_clog(args):
    try:
        if args.gather_clog:
            args.gather_clog(args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_clog' pass gather clog\n")


def gather_slog(args):
    try:
        if args.gather_slog:
            args.gather_slog(args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_slog' pass gather slog\n")


def gather_obproxy_log(args):
    try:
        if args.gather_obproxy_log:
            args.gather_obproxy_log(args)
    except AttributeError:
        logger.debug("object has no attribute 'gather_obproxy_log' pass gather obproxy log\n")


def get_version(args):
    try:
        if args.version:
            args.version(args)
    except AttributeError:
        logger.debug("object has no attribute 'version'\n")


def get_obdiag_trace_log(args):
    try:
        if args.display:
            args.display(args)
    except AttributeError:
        logger.debug("object has no attribute 'display'\n")


def analyze_log(args):
    try:
        if args.analyze_log:
            args.analyze_log(args)
    except AttributeError:
        logger.debug("object has no attribute 'analyze_log' pass analyze log\n")


def analyze_flt_trace(args):
    try:
        if args.analyze_flt_trace:
            args.analyze_flt_trace(args)
    except AttributeError:
        logger.debug("object has no attribute 'analyze_flt_trace' pass analyze trace log\n")

def check(args):
    try:
        if args.check:
            args.check(args)
    except AttributeError as e:
        logger.debug("object has no attribute 'check' pass check\n")


if __name__ == '__main__':
    obdiag = OBDIAGClient()
    arg_parser = ArgParser(obdiag)
    obdiag_args = arg_parser.parse_argv()
    get_version(obdiag_args)
    get_obdiag_trace_log(obdiag_args)
    pharse_config(obdiag_args)
    telemetry.push_cmd_info(obdiag_args)
    if obdiag.init(obdiag_args):
        telemetry.set_cluster_conn(obdiag.ob_cluster)
        gather_log(obdiag_args)
        gather_awr(obdiag_args)
        gather_sysstat(obdiag_args)
        gather_perf(obdiag_args)
        gather_plan_monitor(obdiag_args)
        gather_clog(obdiag_args)
        gather_slog(obdiag_args)
        gather_obproxy_log(obdiag_args)
        analyze_log(obdiag_args)
        analyze_flt_trace(obdiag_args)
        check(obdiag_args)
        telemetry.put_data()
