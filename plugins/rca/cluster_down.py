#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
"""
@file: cluster_down.py
@desc: RCA for cluster down - supports offline mode via --log_dir.
       Analyzes observer.log, coredump, dmesg for crash/abort patterns.
"""
import os
import re

from src.common.pack_discovery import discover_log_files
from src.common.tool import Util
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord


class ClusterDownScene(RcaScene):
    """Cluster down RCA - offline analysis from pack directory."""

    def __init__(self):
        super().__init__()
        self.log_dir = None
        self.log_files = []

    def init(self, context):
        super().init(context)
        self.log_dir = Util.get_option(context.options, "log_dir")
        if not self.log_dir or not os.path.isdir(self.log_dir):
            raise Exception("cluster_down requires --log_dir pointing to unpacked pack directory")
        self.log_files = discover_log_files(self.log_dir)
        self.record.add_record("log_dir: {0}, discovered {1} log files".format(self.log_dir, len(self.log_files)))
        if not self.observer_version:
            self.observer_version = "unknown"
        self.record.add_record("observer_version: {0} (offline mode)".format(self.observer_version))

    def verbose(self, info):
        self.stdio.verbose("[ClusterDownScene] {0}".format(info))

    def execute(self):
        try:
            self.record.add_record("Analyzing logs for crash/abort patterns...")
            crash_patterns = [
                (r"CRASH ERROR", "CRASH ERROR in log"),
                (r"segmentation fault|Segmentation fault", "Segmentation fault"),
                (r"abort\(\)|Fatal signal", "Process abort"),
                (r"ret=-4389", "OB_ERR_UNEXPECTED (-4389)"),
                (r"ret=-4138", "OB_ALLOCATE_MEMORY_FAILED (-4138)"),
                (r"ret=-4013", "OB_ALLOCATE_MEMORY_FAILED (-4013)"),
                (r"core dumped|Core dump", "Core dump"),
            ]
            found = []
            for log_path in self.log_files:
                try:
                    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                        for i, line in enumerate(f):
                            for pat, desc in crash_patterns:
                                if re.search(pat, line, re.IGNORECASE):
                                    found.append((os.path.basename(log_path), i + 1, desc, line.strip()[:120]))
                                    self.record.add_record("Found in {0}:{1} - {2}".format(os.path.basename(log_path), i + 1, desc))
                except Exception as e:
                    self.verbose("read {0} failed: {1}".format(log_path, e))
            if found:
                self.record.add_suggest("Cluster down likely due to crash. Check observer.log for trace_id, use gdb bt full on coredump.")
                self.record.add_suggest("Common causes: OOM (-4013/-4138), disk full, network partition. Review full logs in pack.")
            else:
                self.record.add_record("No crash patterns found in logs. Cluster may have stopped for other reasons.")
                self.record.add_suggest("Check dmesg, /var/log/messages for OOM killer or hardware issues.")
        except Exception as e:
            self.record.add_suggest("Analysis error: {0}. Please share pack with OceanBase support.".format(e))

    def get_scene_info(self):
        return {
            "name": "cluster_down",
            "info_en": "Cluster down RCA (offline). Use --log_dir=<pack_dir>",
            "info_cn": "集群宕机根因分析（离线模式）。需 --log_dir=<pack目录>",
            "example": "obdiag rca run --scene=cluster_down --log_dir=./obdiag_gather_pack_xxx/",
        }


cluster_down = ClusterDownScene()
