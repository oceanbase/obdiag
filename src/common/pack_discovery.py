#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
"""
@file: pack_discovery.py
@desc: Discover log files from obdiag gather pack directory structure.
       Used by analyze log, analyze memory, rca when --log_dir is specified.
"""
import fnmatch
import os

# Standard log file names in OB + OS logs for cluster_down RCA
LOG_NAMES = [
    "observer.log",
    "observer.log.wf",
    "rootservice.log",
    "rootservice.log.wf",
    "election.log",
    "election.log.wf",
    "trace.log",
    "trace.log.wf",
    "dmesg",
    "message.log",
    "syslog",
]
# Glob patterns for rotated logs + coredump
LOG_PATTERNS = [
    "observer.log.*",
    "observer.log.wf.*",
    "rootservice.log.*",
    "rootservice.log.wf.*",
    "election.log.*",
    "election.log.wf.*",
    "trace.log.*",
    "trace.log.wf.*",
    "core*",
    "*.core",
]


def discover_log_files(log_dir):
    """
    Discover OceanBase log files from a pack directory (obdiag gather output).
    Supports:
    - obdiag_gather_pack_<timestamp>/ with node subdirs (observer_log_*)
    - Direct log dir with observer.log etc.
    :param log_dir: root path of pack or log directory
    :return: list of absolute paths to log files
    """
    if not log_dir or not os.path.isdir(log_dir):
        return []
    log_dir = os.path.abspath(log_dir)
    found = []

    def add_file(p):
        if os.path.isfile(p) and p not in found:
            found.append(p)

    def scan_dir(d):
        for name in os.listdir(d):
            full = os.path.join(d, name)
            if os.path.isfile(full):
                if name in LOG_NAMES:
                    add_file(full)
                else:
                    for pat in LOG_PATTERNS:
                        if fnmatch.fnmatch(name, pat):
                            add_file(full)
                            break
            elif os.path.isdir(full):
                scan_dir(full)

    scan_dir(log_dir)
    return sorted(found)
