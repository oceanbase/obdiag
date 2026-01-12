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
@time: 2025/01/08
@file: observer.py
@desc: Observer log gathering

OceanBase Observer Log Structure:
==================================

{home_path}/log/                              # Main log directory
├── observer.log                              # Current observer log
├── observer.log.20250108120000000            # Rotated observer log (17-digit timestamp)
├── observer.log.wf                           # Warning/Fatal level logs
├── rootservice.log                           # RootService logs
├── election.log                              # Election logs
│
└── alert/                                    # Alert log directory (OceanBase 4.x+)
    ├── alert.log                             # Current alert log
    └── alert.log.20250108120000000           # Rotated alert log

Alert Log Reference:
https://www.oceanbase.com/docs/common-oceanbase-database-cn-1000000001499552
"""

import os
import subprocess

from src.handler.gather.gather_log.base import BaseGatherLogOnNode


class ObserverGatherLogOnNode(BaseGatherLogOnNode):
    """Observer log gathering handler"""

    TARGET_NAME = "observer"
    LOG_SCOPES = {
        "observer": {"key": "*observer*"},
        "rootservice": {"key": "*rootservice*"},
        "election": {"key": "*election*"},
        "alert": {"key": "*alert*", "subdir": "alert"},
    }

    def __init__(self, context, node, config):
        super().__init__(context, node, config)
        # Store log path mapping for multi-directory structure
        self._log_path_mapping = {}

    def _get_log_path(self) -> str:
        """Get Observer log path"""
        return os.path.join(self.node.get("home_path"), "log")

    def _build_tmp_dir_name(self, from_ts, to_ts) -> str:
        """Override to add PID for local observer"""
        base_name = super()._build_tmp_dir_name(from_ts, to_ts)
        if self.ssh_client.get_name() == "local":
            pid = self._get_observer_pid()
            if pid:
                return "{0}_pid_{1}".format(base_name, pid)
        return base_name

    def _get_observer_pid(self):
        """Get observer PID from pid file or ps command (local only)"""
        # Note: This method only works for local observer
        # For remote nodes, PID info is not critical for log gathering
        home_path = self.node.get("home_path")
        if not home_path:
            return None

        pid_file_path = os.path.join(home_path, 'run', 'observer.pid')

        # Try to read from pid file first (local file access)
        try:
            with open(pid_file_path, 'r') as file:
                return file.readline().strip()
        except FileNotFoundError:
            self.stdio.verbose("The file {0} does not exist. Attempting to find the process using ps.".format(pid_file_path))
        except Exception as e:
            self.stdio.verbose("An error occurred while reading pid file: {0}".format(e))

        # Fallback to ps command (local execution)
        try:
            result = subprocess.run(['ps', '-ef'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if result.returncode != 0:
                return None
            processes = result.stdout.decode().splitlines()
            observer_bin = "{0}/bin/observer".format(home_path)
            observer_processes = [p for p in processes if observer_bin in p and 'grep' not in p]

            if observer_processes:
                return observer_processes[0].split()[1]
            else:
                self.stdio.verbose("No observer process found at the specified path.")
                return None
        except Exception as e:
            self.stdio.verbose("An error occurred while trying to find the observer process: {0}".format(e))
            return None

    def _find_logs_name(self):
        """
        Override to handle Observer multi-directory log structure.
        Alert logs are in {log_path}/alert/ subdirectory.
        """
        all_logs = []
        base_log_path = self.log_path

        # Determine which directories to search based on scope
        search_paths = self._get_search_paths(base_log_path)

        for search_path, scope_keys in search_paths:
            # Check if path exists
            check_cmd = "test -d {0} && echo 'exists'".format(search_path)
            result = self.ssh_client.exec_cmd(check_cmd)
            if "exists" not in result:
                self.stdio.verbose("Observer log path not exists: {0}".format(search_path))
                continue

            # Build find command for this path
            scope_pattern = self._build_find_pattern(scope_keys)
            if not scope_pattern:
                continue

            find_cmd = "find {0}/ -maxdepth 1 {1} 2>/dev/null | awk -F '/' ".format(search_path, scope_pattern) + "'{print $NF}' | sort"
            self.stdio.verbose("Observer find logs in {0}: {1}".format(search_path, find_cmd))
            logs_output = self.ssh_client.exec_cmd(find_cmd)

            if logs_output:
                for log_name in logs_output.split('\n'):
                    log_name = log_name.strip()
                    if log_name and log_name != "." and log_name != "..":
                        # Store as tuple (log_name, full_path)
                        all_logs.append((log_name, search_path))

        # Filter by time or recent_count
        if self.recent_count > 0:
            self.stdio.verbose("recent_count is {0}, skipping time filtering".format(self.recent_count))
            filtered_logs = self._filter_logs_by_recent_count(all_logs)
        else:
            filtered_logs = self._filter_logs_by_time(all_logs)

        # Store path mapping for grep phase
        self._log_path_mapping = {log[0]: log[1] for log in filtered_logs}

        self.stdio.verbose("Observer found {0} log files total".format(len(filtered_logs)))
        return [log[0] for log in filtered_logs]

    def _get_search_paths(self, base_path) -> list:
        """
        Get list of (search_path, scope_keys) based on configured scope.
        Handles subdirectories like alert/ for alert logs.
        """
        paths = []

        for scope_name, scope_config in self.scope.items():
            full_scope_config = self.LOG_SCOPES.get(scope_name, scope_config)
            subdir = full_scope_config.get("subdir", "")
            keys = full_scope_config.get("key", scope_config.get("key", []))

            if subdir:
                search_path = os.path.join(base_path, subdir)
            else:
                search_path = base_path

            self._add_to_paths(paths, search_path, keys)

        return paths

    def _add_to_paths(self, paths, search_path, keys):
        """Helper method to add or merge paths."""
        for i, (existing_path, existing_keys) in enumerate(paths):
            if existing_path == search_path:
                if isinstance(keys, list):
                    existing_keys.extend(keys)
                else:
                    existing_keys.append(keys)
                return

        if isinstance(keys, list):
            paths.append((search_path, list(keys)))
        else:
            paths.append((search_path, [keys]))

    def _build_find_pattern(self, keys) -> str:
        """Build find command pattern from key list."""
        if not keys:
            return ""

        # Handle both list and single key
        if isinstance(keys, str):
            keys = [keys]

        # Deduplicate
        keys = list(set(keys))
        pattern_parts = " -o ".join(['-name "{0}"'.format(k) for k in keys])
        return "\\( {0} \\)".format(pattern_parts)

    def _filter_logs_by_time(self, logs) -> list:
        """Filter logs by time range."""
        import datetime

        try:
            from_time_dt = datetime.datetime.strptime(self.from_time_str, "%Y-%m-%d %H:%M:%S")
            to_time_dt = datetime.datetime.strptime(self.to_time_str, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self.stdio.warn("Observer parse time failed: {0}, returning all logs".format(str(e)))
            return logs

        filtered = []

        for log_name, log_path in logs:
            file_time_dt = self._parse_timestamp_from_filename(log_name)

            if file_time_dt:
                # File has timestamp - check time range
                if from_time_dt <= file_time_dt <= to_time_dt:
                    filtered.append((log_name, log_path))
                    self.stdio.verbose("Observer log {0} in time range, include".format(log_name))
                else:
                    self.stdio.verbose("Observer log {0} out of time range, skip".format(log_name))
            else:
                # No timestamp - this is a current log file
                if self._is_current_log_file(log_name):
                    filtered.append((log_name, log_path))
                    self.stdio.verbose("Observer log {0} is current log, include".format(log_name))
                else:
                    self.stdio.verbose("Observer log {0} has no timestamp and is not a recognized log file, skip".format(log_name))

        self.stdio.verbose("Observer filtered {0} logs by time range".format(len(filtered)))
        return filtered

    def _filter_logs_by_recent_count(self, logs) -> list:
        """Filter logs by recent count."""
        import datetime

        # Group by log type and path
        log_groups = {}

        for log_name, log_path in logs:
            log_type = self._get_log_type(log_name)
            group_key = (log_type, log_path)

            if group_key not in log_groups:
                log_groups[group_key] = []

            file_time_dt = self._parse_timestamp_from_filename(log_name)
            log_groups[group_key].append((log_name, log_path, file_time_dt))

        # Filter each group to keep most recent
        filtered = []
        for group_key, group_logs in log_groups.items():
            # Separate: current logs (no timestamp), and historical logs
            current_logs = [l for l in group_logs if l[2] is None]
            historical_logs = [l for l in group_logs if l[2] is not None]

            # Sort historical by time (newest first)
            historical_logs.sort(key=lambda x: x[2], reverse=True)

            # Keep current logs + recent historical
            kept = [(l[0], l[1]) for l in current_logs]
            remaining = self.recent_count - len(kept)
            if remaining > 0:
                kept.extend([(l[0], l[1]) for l in historical_logs[:remaining]])

            filtered.extend(kept)
            self.stdio.verbose("Observer log type '{0}': kept {1} files".format(group_key[0], len(kept)))

        self.stdio.verbose("Observer filtered to {0} logs by recent_count".format(len(filtered)))
        return filtered

    def _grep_log_to_tmp(self, logs_name, tmp_log_dir):
        """Override to handle multi-directory log structure."""
        grep_options = self._build_grep_options()

        for log_name in logs_name:
            # Get the actual log path for this file
            log_path = self._log_path_mapping.get(log_name, self.log_path)

            source_log_name = os.path.join(log_path, log_name)
            target_log_name = os.path.join(tmp_log_dir, log_name)
            self.stdio.verbose("grep files, source = [{0}], target = [{1}]".format(source_log_name, target_log_name))

            # For compressed files, just copy
            if log_name.endswith(".gz") or log_name.endswith(".zst"):
                cmd = "cp -a '{0}' '{1}'".format(source_log_name, target_log_name)
                self.stdio.verbose("copy compressed file: {0}".format(cmd))
                self.ssh_client.exec_cmd(cmd)
                continue

            # For normal files
            if not grep_options:
                cmd = "cp -a '{0}' '{1}'".format(source_log_name, target_log_name)
            else:
                grep_pipeline = " | ".join(["grep -e '{0}'".format(opt) for opt in grep_options])
                cmd = "cat '{0}' | {1} > '{2}'".format(source_log_name, grep_pipeline, target_log_name)

            self.stdio.verbose("grep files: {0}".format(cmd))
            self.ssh_client.exec_cmd(cmd)
