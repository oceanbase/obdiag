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
@file: oms.py
@desc: OMS unified log gathering (includes OMS Console, CM, Ghana, Supervisor, CDC)

OMS Actual Log Structure (based on /data/1/oms/out/oms_logs):
=============================================================

{log_path}/                                    # e.g., /data/1/oms/out/oms_logs
├── docker_init.log.2026-01-04.10:42:11        # Docker init logs
├── oms_console_stdout.log                     # OMS Console stdout
├── oms_console_stderr.log                     # OMS Console stderr
├── oms_drc_cm_stdout.log                      # DRC CM stdout
├── oms_drc_supervisor_stdout.log              # DRC Supervisor stdout
├── supervisord.log                            # Main supervisord log
├── tengine-access.log                         # Web access log (large!)
├── tengine-error.log                          # Web error log
│
├── cm/log/                                    # CM (Configuration Manager)
│   ├── 20260108.cm-api.log                    # CM API log (daily prefix)
│   ├── cm-web.log                             # CM Web current log
│   └── cm-web.log.2026-01-06_17               # CM Web hourly rotation
│
├── ghana/                                     # Ghana (Main OMS Component)
│   ├── gc.log.0.current                       # JVM GC log
│   └── Ghana/                                 # Ghana application logs
│       ├── common-default.log                 # Default log
│       ├── common-default.log.2026-01-07.gz   # Compressed daily
│       ├── common-error.log                   # Error log
│       ├── database.log                       # Database operations
│       ├── meta-db.log                        # MetaDB log (large!)
│       ├── oms-alarm.log                      # Alarm log
│       ├── oms-api.log                        # API log
│       ├── oms-scheduler.log                  # Scheduler log (large!)
│       ├── oms-step.log                       # Step execution log
│       ├── oms-web.log                        # Web application log
│       └── oms-integration.log                # Integration log
│
├── supervisor/                                # Supervisor Component
│   ├── command.log                            # Command log
│   ├── error.log                              # Error log
│   ├── legacy.log                             # Legacy log
│   ├── monitor.log                            # Monitor log
│   ├── routine.log                            # Routine log (very large!)
│   └── supervisor.log                         # Supervisor log
│
{store_path}/store{obcdc_id}/log/              # CDC Logs (requires oms_component_id)
    ├── libobcdc.log                           # Current libobcdc log
    ├── libobcdc.log.20250108120000000         # Rotated libobcdc log (17-digit timestamp)
    ├── store.log                              # CDC store log
    └── *oblogproxy*                           # OBLogProxy related logs

Note: CDC logs are in {store_path}, NOT {log_path}!

Log Rotation Patterns:
- Daily: xxx.log.2026-01-07
- Daily with prefix: 20260108.cm-api.log
- Hourly: cm-web.log.2026-01-06_17
- Compressed: xxx.log.2026-01-07.gz
- CDC 17-digit: libobcdc.log.20250108120000000 (YYYYMMDDHHMMSSmmm)

Available Scopes:
=================
Root level:   console, nginx, supervisord, drc
Ghana:        ghana, scheduler, web, alarm, api, gc
CM:           cm
Supervisor:   supervisor
CDC:          cdc, libobcdc, store

Usage:
======
# Gather all OMS logs
obdiag gather oms_log --since 1h

# Gather specific component logs
obdiag gather oms_log --scope ghana
obdiag gather oms_log --scope supervisor
obdiag gather oms_log --scope cm

# Gather CDC logs (requires oms_component_id)
obdiag gather oms_log --scope cdc --oms_component_id "x.x.x.x-123"
obdiag gather oms_log --scope libobcdc --oms_component_id "x.x.x.x-123"
"""

import datetime
import os
import re

from src.handler.gather.gather_log.base import BaseGatherLogOnNode


class OmsGatherLogOnNode(BaseGatherLogOnNode):
    """
    OMS unified log gathering handler

    Supports gathering logs from all OMS components:
    - ghana: Main OMS application logs (oms-scheduler, oms-web, etc.)
    - cm: Configuration Manager logs
    - supervisor: Supervisor and process management logs
    - root: Root level logs (console, nginx, docker_init, etc.)
    - cdc: CDC/libobcdc logs (requires oms_component_id)
    """

    TARGET_NAME = "oms"

    # Log scopes based on actual OMS log structure
    LOG_SCOPES = {
        # === Root Level Logs ===
        "console": {"key": ["oms_console_*", "docker_init*"], "desc": "OMS Console and init logs"},
        "nginx": {"key": ["tengine-*", "oms_nginx_*"], "desc": "Nginx/Tengine access and error logs"},
        "supervisord": {"key": "supervisord.log*", "desc": "Main supervisord log"},
        "drc": {"key": "oms_drc_*", "desc": "DRC component logs"},
        # === Ghana Component Logs (main OMS application) ===
        "ghana": {"key": ["oms-*.log*", "common-*.log*", "meta-db.log*", "database.log*"], "desc": "Ghana application logs", "subdir": "ghana/Ghana"},
        "scheduler": {"key": "oms-scheduler.log*", "desc": "OMS scheduler logs", "subdir": "ghana/Ghana"},
        "web": {"key": "oms-web.log*", "desc": "OMS web application logs", "subdir": "ghana/Ghana"},
        "alarm": {"key": "oms-alarm.log*", "desc": "OMS alarm logs", "subdir": "ghana/Ghana"},
        "api": {"key": "oms-api.log*", "desc": "OMS API logs", "subdir": "ghana/Ghana"},
        "gc": {"key": "gc.log*", "desc": "JVM GC logs", "subdir": "ghana"},
        # === CM Component Logs ===
        "cm": {"key": ["*.cm-api.log*", "cm-web.log*"], "desc": "Configuration Manager logs", "subdir": "cm/log"},
        # === Supervisor Component Logs ===
        "supervisor": {"key": ["supervisor.log*", "command.log*", "error.log*", "monitor.log*", "routine.log*", "legacy.log*"], "desc": "Supervisor component logs", "subdir": "supervisor"},
        # === CDC Component Logs (requires oms_component_id, will be skipped if not provided) ===
        "cdc": {"key": ["*libobcdc.log*", "store.log*", "*oblogproxy*"], "desc": "CDC/libobcdc logs (requires oms_component_id)", "component": "cdc"},
        "libobcdc": {"key": "*libobcdc.log*", "desc": "libobcdc core logs (requires oms_component_id)", "component": "cdc"},
        "store": {"key": "store.log*", "desc": "CDC store logs (requires oms_component_id)", "component": "cdc"},
    }

    # Log timestamp patterns for OMS
    # Pattern 1: xxx.log.2026-01-07 (daily rotation)
    DAILY_TIMESTAMP_PATTERN = re.compile(r'\.(\d{4}-\d{2}-\d{2})$')
    # Pattern 2: cm-web.log.2026-01-06_17 (hourly rotation)
    HOURLY_TIMESTAMP_PATTERN = re.compile(r'\.(\d{4}-\d{2}-\d{2}_\d{2})$')
    # Pattern 3: 20260108.cm-api.log (date prefix)
    DATE_PREFIX_PATTERN = re.compile(r'^(\d{8})\.')
    # Pattern 4: xxx.log.2026-01-07.gz (compressed daily)
    COMPRESSED_DAILY_PATTERN = re.compile(r'\.(\d{4}-\d{2}-\d{2})\.gz$')
    # Pattern 5: docker_init.log.2026-01-04.10:42:11 (datetime suffix)
    DATETIME_TIMESTAMP_PATTERN = re.compile(r'\.(\d{4}-\d{2}-\d{2})\.(\d{2}:\d{2}:\d{2})$')
    # Pattern 6: libobcdc.log.20250108120000000 (CDC 17-digit timestamp, same as OceanBase)
    CDC_TIMESTAMP_PATTERN = re.compile(r'\.(\d{17})(?:\.|$)')
    # Pattern for extracting obcdc_id from component_id: x.x.x.x-{number}
    OBCDC_ID_PATTERN = re.compile(r"(\d+\.\d+\.\d+\.\d+)-(\d+)")

    def __init__(self, context, node, config):
        # Extract oms_component_id for CDC logs
        self.oms_component_id = config.get("oms_component_id")
        self.obcdc_id = self._extract_obcdc_id() if self.oms_component_id else ""
        # Get store_path for CDC logs (different from log_path)
        self.store_path = node.get("store_path", "")
        super().__init__(context, node, config)

    def _extract_obcdc_id(self) -> str:
        """Extract obcdc_id (numeric part) from oms_component_id for CDC log path."""
        if not self.oms_component_id:
            return ""
        match = self.OBCDC_ID_PATTERN.search(self.oms_component_id)
        if match:
            return match.group(2)  # Return the numeric part
        return ""

    def _get_log_path(self) -> str:
        """Get OMS log path from config"""
        # Try log_path first (recommended for OMS)
        log_path = self.node.get("log_path")
        if log_path:
            return log_path

        # Fallback to home_path/logs
        home_path = self.node.get("home_path")
        if home_path:
            return os.path.join(home_path, "logs")

        raise Exception("gather log on oms, but log_path is None. please check your config")

    def _find_logs_name(self):
        """
        Override to handle OMS multi-directory log structure.
        """
        all_logs = []
        base_log_path = self.log_path

        # Determine which subdirectories to search based on scope
        search_paths = self._get_search_paths(base_log_path)

        for search_path, scope_keys in search_paths:
            # Check if path exists
            check_cmd = "test -d {0} && echo 'exists'".format(search_path)
            result = self.ssh_client.exec_cmd(check_cmd)
            if "exists" not in result:
                self.stdio.verbose("OMS log path not exists: {0}".format(search_path))
                continue

            # Build find command for this path
            scope_pattern = self._build_find_pattern(scope_keys)
            if not scope_pattern:
                continue

            find_cmd = "find {0}/ -maxdepth 1 {1} 2>/dev/null | awk -F '/' ".format(search_path, scope_pattern) + "'{print $NF}' | sort"
            self.stdio.verbose("OMS find logs in {0}: {1}".format(search_path, find_cmd))
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
            filtered_logs = self._filter_by_recent_count_oms(all_logs)
        else:
            filtered_logs = self._filter_by_time_oms(all_logs)

        # Store path mapping for grep phase
        self._log_path_mapping = {log[0]: log[1] for log in filtered_logs}

        self.stdio.verbose("OMS found {0} log files total".format(len(filtered_logs)))
        return [log[0] for log in filtered_logs]

    def _get_search_paths(self, base_path) -> list:
        """
        Get list of (search_path, scope_keys) based on configured scope.

        For regular OMS logs, uses base_path (log_path).
        For CDC logs, uses store_path/store{obcdc_id}/log/ (different from log_path).
        """
        paths = []
        cdc_keys_collected = []  # Collect all CDC keys first

        for scope_name, scope_config in self.scope.items():
            full_scope_config = self.LOG_SCOPES.get(scope_name, scope_config)
            subdir = full_scope_config.get("subdir", "")
            keys = full_scope_config.get("key", scope_config.get("key", []))
            component = full_scope_config.get("component", "")

            # CDC logs have special path, collect keys first
            if component == "cdc":
                if isinstance(keys, list):
                    cdc_keys_collected.extend(keys)
                else:
                    cdc_keys_collected.append(keys)
            else:
                if subdir:
                    search_path = os.path.join(base_path, subdir)
                else:
                    search_path = base_path
                self._add_to_paths(paths, search_path, keys)

        # Handle CDC logs once with all collected keys
        # CDC logs are in store_path, not log_path
        if cdc_keys_collected:
            cdc_paths = self._get_cdc_log_paths(cdc_keys_collected)
            for cdc_path, cdc_keys in cdc_paths:
                self._add_to_paths(paths, cdc_path, cdc_keys)

        return paths

    def _get_cdc_log_paths(self, keys) -> list:
        """
        Get CDC log paths. CDC logs require oms_component_id to be specified.
        Path format: {store_path}/store{obcdc_id}/log/

        Note: CDC logs are stored in store_path (not log_path).

        Returns empty list if oms_component_id is not provided.
        """
        if not self.obcdc_id:
            self.stdio.warn("CDC logs require oms_component_id parameter, skipping CDC log collection")
            return []

        if not self.store_path:
            self.stdio.warn("store_path is not configured, skipping CDC log collection")
            return []

        cdc_log_path = os.path.join(self.store_path, "store{0}".format(self.obcdc_id), "log")
        self.stdio.verbose("CDC log path: {0}".format(cdc_log_path))
        return [(cdc_log_path, keys if isinstance(keys, list) else [keys])]

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

        # Deduplicate
        keys = list(set(keys))
        pattern_parts = " -o ".join(['-name "{0}"'.format(k) for k in keys])
        return "\\( {0} \\)".format(pattern_parts)

    def _parse_oms_log_timestamp(self, file_name):
        """
        Parse timestamp from OMS log filename.
        Returns (datetime, is_compressed) tuple or (None, False).

        Supported formats:
        1. xxx.log.2026-01-07.gz (compressed daily)
        2. xxx.log.2026-01-07 (daily rotation)
        3. cm-web.log.2026-01-06_17 (hourly rotation)
        4. 20260108.cm-api.log (date prefix)
        5. docker_init.log.2026-01-04.10:42:11 (datetime suffix)
        6. libobcdc.log.20250108120000000 (CDC 17-digit timestamp)
        """
        is_gz = file_name.endswith('.gz')

        # Pattern 1: xxx.log.2026-01-07.gz (compressed daily)
        match = self.COMPRESSED_DAILY_PATTERN.search(file_name)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1), "%Y-%m-%d"), True
            except ValueError:
                pass

        # Pattern 6: CDC 17-digit timestamp (libobcdc.log.20250108120000000)
        # Check this before daily pattern to avoid partial matches
        match = self.CDC_TIMESTAMP_PATTERN.search(file_name)
        if match:
            try:
                timestamp_str = match.group(1)
                # Format: YYYYMMDDHHMMSSmmm (17 digits)
                return datetime.datetime.strptime(timestamp_str[:14], "%Y%m%d%H%M%S"), is_gz
            except ValueError:
                pass

        # Pattern 2: xxx.log.2026-01-07 (daily rotation)
        match = self.DAILY_TIMESTAMP_PATTERN.search(file_name)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1), "%Y-%m-%d"), is_gz
            except ValueError:
                pass

        # Pattern 3: cm-web.log.2026-01-06_17 (hourly rotation)
        match = self.HOURLY_TIMESTAMP_PATTERN.search(file_name)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1), "%Y-%m-%d_%H"), is_gz
            except ValueError:
                pass

        # Pattern 4: 20260108.cm-api.log (date prefix)
        match = self.DATE_PREFIX_PATTERN.search(file_name)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1), "%Y%m%d"), is_gz
            except ValueError:
                pass

        # Pattern 5: docker_init.log.2026-01-04.10:42:11 (datetime suffix)
        match = self.DATETIME_TIMESTAMP_PATTERN.search(file_name)
        if match:
            try:
                date_str = match.group(1)
                time_str = match.group(2).replace(":", "")
                return datetime.datetime.strptime(date_str + time_str, "%Y-%m-%d%H%M%S"), is_gz
            except ValueError:
                pass

        return None, is_gz

    def _filter_by_time_oms(self, logs) -> list:
        """
        Filter OMS logs by time range.

        Special handling for .gz files:
        - .gz files are typically historical compressed logs
        - Only include .gz files if they fall within the time range
        - For non-.gz files without timestamp (current logs), always include
        """
        try:
            from_time_dt = datetime.datetime.strptime(self.from_time_str, "%Y-%m-%d %H:%M:%S")
            to_time_dt = datetime.datetime.strptime(self.to_time_str, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self.stdio.warn("OMS parse time failed: {0}, returning all logs".format(str(e)))
            return logs

        filtered = []
        today = datetime.datetime.now().date()
        current_hour = datetime.datetime.now().strftime("%Y-%m-%d_%H")
        from_date = from_time_dt.date()
        to_date = to_time_dt.date()

        for log_name, log_path in logs:
            is_gz = log_name.endswith('.gz')
            file_time, is_compressed = self._parse_oms_log_timestamp(log_name)

            if file_time:
                # File has timestamp - check time range
                file_date = file_time.date()

                if file_date >= from_date and file_date <= to_date:
                    filtered.append((log_name, log_path))
                    self.stdio.verbose("OMS log {0} (date {1}) in time range, include".format(log_name, file_date))
                else:
                    self.stdio.verbose("OMS log {0} (date {1}) out of time range, skip".format(log_name, file_date))
            else:
                # No timestamp - this is a current active log
                if is_gz:
                    # .gz file without parseable timestamp - only include if contains current hour
                    # This handles edge cases like hourly .gz files
                    if current_hour in log_name or today.strftime("%Y-%m-%d") in log_name:
                        filtered.append((log_name, log_path))
                        self.stdio.verbose("OMS .gz log {0} matches current time, include".format(log_name))
                    else:
                        self.stdio.verbose("OMS .gz log {0} does not match current time, skip".format(log_name))
                else:
                    # Non-.gz current log - always include
                    filtered.append((log_name, log_path))
                    self.stdio.verbose("OMS log {0} is current log, include".format(log_name))

        self.stdio.verbose("OMS filtered {0} logs by time range".format(len(filtered)))
        return filtered

    def _filter_by_recent_count_oms(self, logs) -> list:
        """
        Filter OMS logs by recent count.

        Special handling for .gz files:
        - .gz files are counted in the recent_count
        - Non-.gz current logs are always included first
        - Then most recent .gz files are included up to recent_count
        """
        # Group by log type
        log_groups = {}

        for log_name, log_path in logs:
            # Extract log type (e.g., "oms-scheduler.log" from "oms-scheduler.log.2026-01-07")
            log_type = self._get_log_type(log_name)
            group_key = (log_type, log_path)

            if group_key not in log_groups:
                log_groups[group_key] = []

            file_time, _ = self._parse_oms_log_timestamp(log_name)
            is_gz = log_name.endswith('.gz')
            log_groups[group_key].append((log_name, log_path, file_time, is_gz))

        # Filter each group to keep most recent
        filtered = []
        for group_key, group_logs in log_groups.items():
            # Separate: current logs (no timestamp, not .gz), and historical logs
            current_logs = [l for l in group_logs if l[2] is None and not l[3]]
            historical_logs = [l for l in group_logs if l[2] is not None or l[3]]

            # Sort historical by time (newest first), .gz files with no timestamp go last
            def sort_key(x):
                if x[2] is not None:
                    return (0, x[2])  # Has timestamp, sort by time
                else:
                    return (1, datetime.datetime.min)  # No timestamp, sort last

            historical_logs.sort(key=sort_key, reverse=True)

            # Keep current logs + recent historical
            kept = [(l[0], l[1]) for l in current_logs]
            remaining = self.recent_count - len(kept)
            if remaining > 0:
                kept.extend([(l[0], l[1]) for l in historical_logs[:remaining]])

            filtered.extend(kept)
            self.stdio.verbose("OMS log type '{0}': kept {1} files".format(group_key[0], len(kept)))

        self.stdio.verbose("OMS filtered to {0} logs by recent_count".format(len(filtered)))
        return filtered

    def _get_log_type(self, file_name) -> str:
        """Extract log type from filename, removing timestamp suffix."""
        # Remove known timestamp patterns
        result = file_name

        # Remove .gz suffix first
        if result.endswith('.gz'):
            result = result[:-3]

        # Remove date suffix patterns
        result = self.DAILY_TIMESTAMP_PATTERN.sub('', result)
        result = self.HOURLY_TIMESTAMP_PATTERN.sub('', result)
        result = self.DATETIME_TIMESTAMP_PATTERN.sub('', result)

        # Remove date prefix pattern
        result = self.DATE_PREFIX_PATTERN.sub('', result)

        return result

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

    def _is_current_log_file(self, file_name) -> bool:
        """Check if file is a current log file (no timestamp suffix)."""
        if file_name.endswith(".gz") or file_name.endswith(".zst"):
            return False

        # Check if it has any timestamp pattern
        if self.DAILY_TIMESTAMP_PATTERN.search(file_name):
            return False
        if self.HOURLY_TIMESTAMP_PATTERN.search(file_name):
            return False
        if self.DATE_PREFIX_PATTERN.search(file_name):
            return False
        if self.DATETIME_TIMESTAMP_PATTERN.search(file_name):
            return False

        return file_name.endswith(".log") or ".log." in file_name or file_name.endswith(".current")
