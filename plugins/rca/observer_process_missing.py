#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
@time: 2026/06/08
@file: observer_process_missing.py
@desc: RCA for missing or restarted observer process.
"""

import os
import re
import shlex

from src.common.command import get_observer_pid
from src.handler.rca.rca_exception import RCAExecuteException, RCAInitException
from src.handler.rca.rca_handler import RcaScene


class ObserverProcessMissingScene(RcaScene):
    DEFAULT_TAIL_LINES = 5000
    KEYWORD_PATTERN = "CRASH|signal|SIGTERM|SIGSEGV|SIGABRT|SIGKILL|abort|core|fatal"

    def __init__(self):
        super().__init__()
        self.tail_lines = self.DEFAULT_TAIL_LINES
        self.node_ip = None

    def verbose(self, info):
        self.stdio.verbose("[ObserverProcessMissingScene] {0}".format(info))

    def init(self, context):
        super().init(context)
        if not self.observer_nodes:
            raise RCAInitException("observer nodes are empty. Please check the NODES conf.")
        if not self.store_dir:
            raise RCAInitException("store_dir is empty.")
        if not os.path.exists(self.store_dir):
            os.makedirs(self.store_dir)

        self.node_ip = self.input_parameters.get("node_ip")
        tail_lines = self.input_parameters.get("tail_lines", self.DEFAULT_TAIL_LINES)
        try:
            self.tail_lines = int(tail_lines)
        except Exception:
            raise RCAInitException("tail_lines is not a valid number, please check the env")
        if self.tail_lines <= 0:
            raise RCAInitException("tail_lines must be greater than 0")
        self.record.add_record("start check observer_process_missing scene")
        self.record.add_record("input parameters: node_ip: {0}, tail_lines: {1}".format(self.node_ip or "all", self.tail_lines))

    def execute(self):
        try:
            matched_node = False
            missing_count = 0
            running_count = 0
            for node in self.observer_nodes:
                if self.node_ip and node.get("ip") != self.node_ip:
                    continue
                matched_node = True
                status = self._inspect_node(node)
                if status == "missing":
                    missing_count += 1
                elif status == "running":
                    running_count += 1

            if not matched_node:
                self.record.add_suggest("no observer node matched node_ip={0}".format(self.node_ip))
                return
            if missing_count > 0:
                self.record.add_suggest("observer process is missing on {0} node(s). Check collected observer logs, crash keywords, and core file list first.".format(missing_count))
            if running_count > 0:
                self.record.add_suggest("observer process is running on {0} node(s). If an unexpected restart is suspected, compare process start time with observer.log timestamps.".format(running_count))
            self.record.add_suggest("If core files exist, collect the core file, observer binary, and observer version for stack analysis.")
        except Exception as e:
            raise RCAExecuteException("ObserverProcessMissingScene execute error: {0}".format(e))

    def _inspect_node(self, node):
        node_name = self._node_name(node)
        home_path = node.get("home_path")
        ssh_client = node.get("ssher")
        if not home_path or ssh_client is None:
            self.record.add_record("node {0}: home_path or ssh client is empty, skip".format(node_name))
            return "skipped"

        local_dir = os.path.join(self.store_dir, "observer_process_missing", self._safe_name(node_name))
        os.makedirs(local_dir, exist_ok=True)

        pids = get_observer_pid(ssh_client, home_path, self.stdio)
        if pids:
            self.record.add_record("node {0}: observer process is running, pid(s): {1}".format(node_name, ",".join(pids)))
            self._record_process_start_time(ssh_client, node_name, pids)
            status = "running"
        else:
            self.record.add_record("node {0}: observer process is missing".format(node_name))
            status = "missing"

        evidence = self._collect_evidence(ssh_client, home_path, local_dir, node_name)
        self._record_evidence(node_name, evidence)
        if status == "missing":
            if evidence["core_files"]:
                self.record.add_suggest("node {0}: found core file candidates, active crash/core analysis is needed.".format(node_name))
            elif evidence["keyword_file"]:
                self.record.add_suggest("node {0}: no core file candidate was found, but crash/signal keywords exist in observer logs.".format(node_name))
            else:
                self.record.add_suggest("node {0}: no observer process/core/crash keyword was found. Check whether the process was stopped by external scripts or OS signals.".format(node_name))
        return status

    def _record_process_start_time(self, ssh_client, node_name, pids):
        cmd = "ps -p {0} -o pid= -o lstart= -o etime= 2>/dev/null".format(shlex.quote(",".join(pids)))
        try:
            output = ssh_client.exec_cmd(cmd).strip()
            if output:
                self.record.add_record("node {0}: process start time: {1}".format(node_name, output.replace("\n", "; ")))
        except Exception as e:
            self.record.add_record("node {0}: failed to get process start time: {1}".format(node_name, e))

    def _collect_evidence(self, ssh_client, home_path, local_dir, node_name):
        remote_dir = self._create_remote_dir(ssh_client)
        evidence = {"latest_log": "", "keyword_file": "", "core_files": "", "process_file": "", "version_file": ""}
        try:
            commands = self._build_evidence_commands(home_path, remote_dir)
            for cmd in commands:
                ssh_client.exec_cmd(cmd)
            remote_files = {
                "latest_log": os.path.join(remote_dir, "latest_observer_tail.log"),
                "keyword_file": os.path.join(remote_dir, "observer_crash_keywords.log"),
                "core_files": os.path.join(remote_dir, "core_files.txt"),
                "process_file": os.path.join(remote_dir, "observer_process.txt"),
                "version_file": os.path.join(remote_dir, "observer_version.txt"),
            }
            for evidence_name, remote_path in remote_files.items():
                if self._remote_file_has_content(ssh_client, remote_path):
                    local_path = os.path.join(local_dir, os.path.basename(remote_path))
                    ssh_client.download(remote_path, local_path)
                    evidence[evidence_name] = local_path
        except Exception as e:
            self.record.add_record("node {0}: failed to collect observer evidence: {1}".format(node_name, e))
        finally:
            self._cleanup_remote_dir(ssh_client, remote_dir)
        return evidence

    def _build_evidence_commands(self, home_path, remote_dir):
        quoted_home = shlex.quote(home_path)
        quoted_log_dir = shlex.quote(os.path.join(home_path, "log"))
        quoted_remote_dir = shlex.quote(remote_dir)
        quoted_observer = shlex.quote(os.path.join(home_path, "bin", "observer"))
        quoted_lib = shlex.quote(os.path.join(home_path, "lib"))
        quoted_keyword_pattern = shlex.quote(self.KEYWORD_PATTERN)
        return [
            "ps -ef | grep {0} | grep -v grep | grep -v obshell > {1}/observer_process.txt || true".format(quoted_observer, quoted_remote_dir),
            "export LD_LIBRARY_PATH={0}; {1} --version > {2}/observer_version.txt 2>&1 || true".format(quoted_lib, quoted_observer, quoted_remote_dir),
            "latest=$(ls -1t {0}/observer.log* 2>/dev/null | head -1); if [ -n \"$latest\" ]; then tail -n {1} \"$latest\" > {2}/latest_observer_tail.log; fi".format(quoted_log_dir, self.tail_lines, quoted_remote_dir),
            "grep -Eih {0} {1}/observer.log* 2>/dev/null | tail -n 1000 > {2}/observer_crash_keywords.log || true".format(quoted_keyword_pattern, quoted_log_dir, quoted_remote_dir),
            "find {0} -maxdepth 4 -type f \\( -name 'core*' -o -name '*.core' \\) -printf '%p\\t%TY-%Tm-%Td %TH:%TM:%TS\\t%s\\n' 2>/dev/null | sort | tail -100 > {1}/core_files.txt || true".format(quoted_home, quoted_remote_dir),
        ]

    def _create_remote_dir(self, ssh_client):
        remote_dir = ssh_client.exec_cmd("mktemp -d /tmp/obdiag_observer_process_missing.XXXXXX").strip().splitlines()
        if not remote_dir:
            raise RCAExecuteException("failed to create remote tmp dir")
        return remote_dir[-1]

    def _remote_file_has_content(self, ssh_client, remote_path):
        cmd = "test -s {0} && echo HAS_FILE || echo EMPTY_FILE".format(shlex.quote(remote_path))
        return "HAS_FILE" in ssh_client.exec_cmd(cmd)

    def _cleanup_remote_dir(self, ssh_client, remote_dir):
        try:
            if remote_dir.startswith("/tmp/obdiag_observer_process_missing."):
                ssh_client.exec_cmd("rm -rf {0}".format(shlex.quote(remote_dir)))
        except Exception as e:
            self.record.add_record("cleanup remote dir {0} failed: {1}".format(remote_dir, e))

    def _record_evidence(self, node_name, evidence):
        if evidence["version_file"]:
            self.record.add_record("node {0}: observer version info saved to {1}".format(node_name, evidence["version_file"]))
        if evidence["process_file"]:
            self.record.add_record("node {0}: process snapshot saved to {1}".format(node_name, evidence["process_file"]))
        if evidence["latest_log"]:
            self.record.add_record("node {0}: latest observer log tail saved to {1}".format(node_name, evidence["latest_log"]))
        if evidence["keyword_file"]:
            self.record.add_record("node {0}: observer crash/signal keyword log saved to {1}".format(node_name, evidence["keyword_file"]))
        if evidence["core_files"]:
            self.record.add_record("node {0}: core file candidate list saved to {1}".format(node_name, evidence["core_files"]))

    def _node_name(self, node):
        ssh_client = node.get("ssher")
        if ssh_client is not None:
            try:
                return ssh_client.get_name()
            except Exception:
                pass
        return "{0}:{1}".format(node.get("ip", "unknown"), node.get("ssh_port", ""))

    def _safe_name(self, name):
        return re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_") or "unknown"

    def get_scene_info(self):
        return {
            "name": "observer_process_missing",
            "info_en": "diagnose missing or restarted observer process",
            "info_cn": "诊断 observer 进程不存在或异常重启",
            "example": "obdiag rca run --scene=observer_process_missing",
        }


observer_process_missing = ObserverProcessMissingScene()
