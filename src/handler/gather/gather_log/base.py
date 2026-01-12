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
@file: base.py
@desc: Base class for component log gathering
"""

from abc import ABC, abstractmethod
import datetime
import os
import re
import traceback
import uuid

from src.common.command import get_file_size, is_empty_dir
from src.common.ssh_client.ssh import SshClient
from src.common.tool import FileUtil, TimeUtils


class BaseGatherLogOnNode(ABC):
    """Base class for gathering logs on a single node"""

    # Subclass should override these
    TARGET_NAME = ""
    LOG_SCOPES = {}

    # Default configuration constants
    DEFAULT_FILE_NUMBER_LIMIT = 20
    DEFAULT_FILE_SIZE_LIMIT = 2 * 1024 * 1024 * 1024  # 2GB

    def __init__(self, context, node, config):
        self.context = context
        self.ssh_client = None
        self.stdio = context.stdio
        self.config = config
        self.node = node

        self.tmp_dir = config.get("tmp_dir")
        self.scope = config.get("scope")
        self.from_time_str = config.get("from_time")
        self.to_time_str = config.get("to_time")
        self.grep_option = config.get("grep")
        self.store_dir = config.get("store_dir")
        self.file_number_limit = config.get("file_number_limit", self.DEFAULT_FILE_NUMBER_LIMIT)
        self.file_size_limit = config.get("file_size_limit", self.DEFAULT_FILE_SIZE_LIMIT)
        self.recent_count = self._parse_recent_count(config.get("recent_count", 0))

        self.log_path = self._get_log_path()
        self.gather_tuple = {"node": "", "success": "Fail", "info": "", "file_size": 0, "file_path": ""}

    @abstractmethod
    def _get_log_path(self) -> str:
        """Get log path for this component - must be implemented by subclass"""
        pass

    def _parse_recent_count(self, value) -> int:
        """Parse recent_count value to int"""
        if value is None:
            return 0
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

    def get_result(self):
        """Get gather result"""
        return self.gather_tuple

    def handle(self):
        """Main handle logic - common for all components"""
        self.ssh_client = SshClient(self.context, self.node)
        self.gather_tuple["node"] = self.ssh_client.get_name()
        self.tmp_dir = os.path.join(self.tmp_dir, "obdiag_gather_{0}".format(str(uuid.uuid4())[:6]))
        self.ssh_client.exec_cmd("mkdir -p {0}".format(self.tmp_dir))

        from_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.from_time_str))
        to_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.to_time_str))

        tmp_dir = self._build_tmp_dir_name(from_datetime_timestamp, to_datetime_timestamp)
        tmp_log_dir = os.path.join(self.tmp_dir, tmp_dir)

        mkdir_response = self.ssh_client.exec_cmd("mkdir -p {0}".format(tmp_log_dir))
        if mkdir_response:
            self.stdio.error("gather_log_on_node {0} mkdir -p {1}: error:{2}".format(self.ssh_client.get_ip(), tmp_log_dir, mkdir_response))
            return

        self.stdio.verbose("gather_log_on_node {0} tmp_log_dir: {1}".format(self.ssh_client.get_ip(), tmp_log_dir))

        try:
            # Find logs
            logs_name = self._find_logs_name()
            if not self._validate_logs(logs_name):
                return

            # Grep logs to temp directory
            self._grep_log_to_tmp(logs_name, tmp_log_dir)

            # Package and download
            self._package_and_download(tmp_log_dir, tmp_dir)

        except Exception as e:
            self.stdio.verbose(traceback.format_exc())
            self.stdio.error("gather_log_on_node {0} failed: {1}".format(self.ssh_client.get_ip(), str(e)))
            self.gather_tuple["info"] = str(e)
        finally:
            self.stdio.verbose("clear tmp_log_dir: {0}".format(self.tmp_dir))
            self.ssh_client.exec_cmd("rm -rf {0}".format(self.tmp_dir))
            self.stdio.verbose("gather_log_on_node {0} finished".format(self.ssh_client.get_ip()))
            # Close SSH connection
            if self.ssh_client:
                try:
                    self.ssh_client.close()
                except Exception:
                    pass

    def _build_tmp_dir_name(self, from_ts, to_ts) -> str:
        """Build temporary directory name"""
        return "{0}_log_{1}_{2}_{3}_{4}".format(self.TARGET_NAME, self.ssh_client.get_name().replace(":", "_"), from_ts, to_ts, str(uuid.uuid4())[:6])

    def _validate_logs(self, logs_name) -> bool:
        """Validate found logs"""
        if not logs_name:
            self.stdio.warn("gather_log_on_node {0} failed: no log found".format(self.ssh_client.get_ip()))
            self.gather_tuple["info"] = "no log found"
            return False

        log_count = len(logs_name)
        if log_count > self.file_number_limit:
            self.stdio.warn('{0} The number of log files is {1}, out of range (0,{2}], Please adjust the query limit'.format(self.ssh_client.get_name(), log_count, self.file_number_limit))
            self.gather_tuple["info"] = "too many files {0} > {1}".format(log_count, self.file_number_limit)
            return False

        return True

    def _grep_log_to_tmp(self, logs_name, tmp_log_dir):
        """Grep logs to temp directory"""
        grep_options = self._build_grep_options()

        for log_name in logs_name:
            source_log_name = "{0}/{1}".format(self.log_path, log_name)
            target_log_name = "{0}/{1}".format(tmp_log_dir, log_name)
            self.stdio.verbose("grep files, source_log_name = [{0}], target_log_name = [{1}]".format(source_log_name, target_log_name))

            # For compressed files, just copy
            if log_name.endswith(".gz") or log_name.endswith(".zst"):
                log_grep_cmd = "cp -a {0} {1}".format(source_log_name, target_log_name)
                self.stdio.verbose("grep files, run cmd = [{0}]".format(log_grep_cmd))
                self.ssh_client.exec_cmd(log_grep_cmd)
                continue

            # For normal files
            if not grep_options:
                log_grep_cmd = "cp -a {0} {1}".format(source_log_name, target_log_name)
            else:
                # Build correct grep pipeline: cat file | grep -e 'p1' | grep -e 'p2' > target
                grep_pipeline = " | ".join(["grep -e '{0}'".format(opt) for opt in grep_options])
                log_grep_cmd = "cat {0} | {1} > {2}".format(source_log_name, grep_pipeline, target_log_name)

            self.stdio.verbose("grep files, run cmd = [{0}]".format(log_grep_cmd))
            self.ssh_client.exec_cmd(log_grep_cmd)

    def _build_grep_options(self) -> list:
        """Build grep options list"""
        if not self.grep_option:
            return []
        return list(self.grep_option)

    def _package_and_download(self, tmp_log_dir, tmp_dir):
        """Package and download logs to local"""
        # Check if temp dir is empty
        if is_empty_dir(self.ssh_client, tmp_log_dir, self.stdio):
            self.ssh_client.exec_cmd("rm -rf {0}".format(tmp_log_dir))
            self.stdio.warn("gather_log_on_node {0} failed: tmp_log_dir({1}) no log found".format(self.ssh_client.get_name(), tmp_log_dir))
            self.gather_tuple["info"] = "tmp_log_dir({0}) no log found".format(tmp_log_dir)
            return

        # Create tar file
        tar_file = "{0}.tar.gz".format(tmp_log_dir)
        tar_cmd = "cd {0} && tar -czf {1}.tar.gz {1}/*".format(self.tmp_dir, tmp_dir)
        self.stdio.verbose("gather_log_on_node {0} tar_cmd: {1}".format(self.ssh_client.get_ip(), tar_cmd))
        self.stdio.verbose("gather_log_on_node {0} tar request:{1}".format(self.ssh_client.get_ip(), self.ssh_client.exec_cmd(tar_cmd)))

        # Check tar file size
        tar_file_size = int(get_file_size(self.ssh_client, tar_file))
        self.stdio.verbose("gather_log_on_node {0} tar_file_size: {1}".format(self.ssh_client.get_ip(), tar_file_size))

        if tar_file_size == 0:
            self.stdio.warn("gather_log_on_node {0} failed: tar file size is 0".format(self.ssh_client.get_ip()))
            self.gather_tuple["info"] = "tar file size is 0"
            return

        if tar_file_size > self.file_size_limit:
            self.stdio.warn("gather_log_on_node {0} failed: File too large over gather.file_size_limit".format(self.ssh_client.get_ip()))
            self.gather_tuple["info"] = "File too large over gather.file_size_limit"
            return

        # Download tar file
        self.stdio.verbose("gather_log_on_node {0} download log to local store_dir: {1}".format(self.ssh_client.get_ip(), self.store_dir))
        self.ssh_client.exec_cmd("chmod -R a+rx {0}".format(self.tmp_dir))
        self.ssh_client.download(tar_file, os.path.join(self.store_dir, os.path.basename("{0}".format(tar_file))))
        self.stdio.verbose("download success. gather_log_on_node {0} delete tar file: {1}".format(self.ssh_client.get_ip(), tar_file))
        self.ssh_client.exec_cmd("rm -rf {0}".format(tar_file))

        # Update gather result
        tar_file_name = os.path.basename("{0}".format(tar_file))
        local_tar_file_path = os.path.join(self.store_dir, tar_file_name)
        self.stdio.verbose("local_tar_file_path: {0}".format(local_tar_file_path))

        self.gather_tuple["file_size"] = FileUtil.size_format(num=int(os.path.getsize(local_tar_file_path) or 0), output_str=True)
        self.gather_tuple["info"] = "file save in {0}".format(local_tar_file_path)
        self.gather_tuple["success"] = "Success"
        self.gather_tuple["file_path"] = local_tar_file_path

    # ========== Log file finding methods ==========

    def _find_logs_name(self):
        """Find log files"""
        logs_scope = self._build_logs_scope()
        if not logs_scope:
            self.stdio.warn("gather_log_on_node {0} find logs scope is null".format(self.ssh_client.get_ip()))
            return []

        self.stdio.verbose("gather_log_on_node {0} find logs scope: {1}".format(self.ssh_client.get_ip(), logs_scope))
        find_cmd = "find {0}/ {1} | awk -F '/' ".format(self.log_path, logs_scope) + "'{print $NF}' | sort"
        self.stdio.verbose("gather_log_on_node {0} find logs cmd: {1}".format(self.ssh_client.get_ip(), find_cmd))
        logs_name = self.ssh_client.exec_cmd(find_cmd)

        if not logs_name or len(logs_name) == 0:
            self.stdio.warn("gather_log_on_node {0} failed: no log found".format(self.ssh_client.get_ip()))
            return []

        # If recent_count > 0, skip time filtering
        if self.recent_count > 0:
            self.stdio.verbose("recent_count is {0}, skipping time filtering".format(self.recent_count))
            log_name_list = self._get_all_logfile_names(logs_name)
            if len(log_name_list) > self.recent_count:
                log_name_list = self._filter_by_recent_count(log_name_list)
            return log_name_list
        else:
            return self._get_logfile_names_by_time(logs_name)

    def _build_logs_scope(self) -> str:
        """Build find command scope"""
        name_patterns = []
        for scope_config in self.scope.values():
            target_scopes = scope_config["key"]
            if isinstance(target_scopes, list):
                name_patterns.extend(target_scopes)
            else:
                name_patterns.append(target_scopes)

        if not name_patterns:
            return ""

        # Build: \( -name "pattern1" -o -name "pattern2" \)
        # Parentheses are needed for correct -o behavior in find command
        pattern_parts = " -o ".join(['-name "{0}"'.format(p) for p in name_patterns])
        return "\\( {0} \\)".format(pattern_parts)

    def _get_all_logfile_names(self, log_files) -> list:
        """Get all log file names without time filtering"""
        log_name_list = []
        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            if file_name == "":
                self.stdio.verbose("existing file name is empty")
                continue
            log_name_list.append(file_name)

        self.stdio.verbose("get all log file name list (no time filtering), found {0} files".format(len(log_name_list)))
        return log_name_list

    def _get_logfile_names_by_time(self, log_files) -> list:
        """
        Get log files filtered by time range.
        Default implementation for components with standard timestamp format (17 digits).
        Subclass can override for special handling.
        """
        self.stdio.verbose("get log file name list, from time {0}, to time {1}, log dir {2}".format(self.from_time_str, self.to_time_str, self.log_path))
        log_name_list = []

        try:
            from_time_dt = datetime.datetime.strptime(self.from_time_str, "%Y-%m-%d %H:%M:%S")
            to_time_dt = datetime.datetime.strptime(self.to_time_str, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self.stdio.warn("gather_log_on_node {0} parse time failed: {1}".format(self.ssh_client.get_ip(), str(e)))
            return log_name_list

        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            if not file_name:
                self.stdio.verbose("existing file name is empty")
                continue

            try:
                # Parse timestamp from filename
                file_time_dt = self._parse_timestamp_from_filename(file_name)

                if file_time_dt:
                    # File has timestamp in filename
                    self.stdio.verbose("node: {0}, file_name: {1}, file_time: {2}".format(self.ssh_client.get_name(), file_name, file_time_dt.strftime("%Y-%m-%d %H:%M:%S.%f")))

                    if self._is_file_in_time_range(file_time_dt, from_time_dt, to_time_dt):
                        log_name_list.append(file_name)
                        self.stdio.verbose("node: {0}, file {1} is in range [{2}, {3}], include it".format(self.ssh_client.get_name(), file_name, self.from_time_str, self.to_time_str))
                    else:
                        self.stdio.verbose("node: {0}, file {1} is out of range [{2}, {3}], exclude it".format(self.ssh_client.get_name(), file_name, self.from_time_str, self.to_time_str))
                else:
                    # File has no timestamp - check if it's a current log file
                    if self._is_current_log_file(file_name):
                        log_name_list.append(file_name)
                        self.stdio.verbose("node: {0}, file {1} has no timestamp, is current log file, include it".format(self.ssh_client.get_name(), file_name))
                    else:
                        self.stdio.verbose("node: {0}, file {1} has no timestamp and is not a recognized log file, skip".format(self.ssh_client.get_name(), file_name))

            except Exception as e:
                self.stdio.warn("gather_log_on_node {0} get log file: {2} name failed, Skip it: {1}".format(self.ssh_client.get_ip(), str(e), file_name))
                continue

        if log_name_list:
            self.stdio.verbose("Find the qualified log file {0} on Server [{1}]".format(log_name_list, self.ssh_client.get_ip()))
        else:
            self.stdio.warn("No found the qualified log file on Server [{0}]".format(self.ssh_client.get_name()))

        return log_name_list

    def _is_current_log_file(self, file_name) -> bool:
        """
        Check if file is a current log file (no timestamp suffix).
        Override in subclass for component-specific logic.
        """
        return file_name.endswith(".log") or file_name.endswith(".wf")

    def _get_log_type(self, file_name) -> str:
        """
        Extract log type from file name by removing timestamp suffix.
        Examples:
            observer.log -> observer.log
            observer.log.20250101120000000 -> observer.log
            observer.log.wf -> observer.log.wf
            observer.log.wf.20250101120000000 -> observer.log.wf
        """
        log_type = re.sub(r'\.\d{17}$', '', file_name)
        return log_type

    def _filter_by_recent_count(self, log_name_list) -> list:
        """
        Filter log files to keep only the most recent N files for EACH log type.
        Files without timestamp (current log files) are treated as newest.
        """
        if self.recent_count <= 0:
            return log_name_list

        self.stdio.verbose("recent_count is {0}, filtering to keep only the most recent {0} files per log type from {1} files".format(self.recent_count, len(log_name_list)))

        # Group files by log type
        log_type_groups = {}  # log_type -> list of (file_name, timestamp_datetime or None)

        for file_name in log_name_list:
            log_type = self._get_log_type(file_name)
            if log_type not in log_type_groups:
                log_type_groups[log_type] = []

            # Reuse existing timestamp parser
            file_time_dt = self._parse_timestamp_from_filename(file_name)
            log_type_groups[log_type].append((file_name, file_time_dt))

        # Filter each log type group
        filtered_list = []
        for log_type, files in log_type_groups.items():
            current_files = [f for f, ts in files if ts is None]
            timestamped_files = [(f, ts) for f, ts in files if ts is not None]

            # Sort timestamped files by time (newest first)
            timestamped_files.sort(key=lambda x: x[1], reverse=True)

            # Build list: current files first, then most recent timestamped files
            type_filtered = list(current_files)
            remaining_slots = self.recent_count - len(current_files)
            if remaining_slots > 0:
                type_filtered.extend([f for f, _ in timestamped_files[:remaining_slots]])
            else:
                type_filtered = current_files[: self.recent_count]

            self.stdio.verbose("Log type '{0}': kept {1} files from {2}".format(log_type, len(type_filtered), len(files)))
            filtered_list.extend(type_filtered)

        self.stdio.verbose("After filtering by recent_count={0}, kept {1} files total".format(self.recent_count, len(filtered_list)))
        return filtered_list

    def _parse_timestamp_from_filename(self, file_name):
        """
        Parse timestamp from filename.
        Filename format: filename.20251201131651918 or filename.20251015031702026.zst
        Timestamp format: YYYYMMDDHHMMSSmmm (17 digits)
        Returns datetime object or None if no valid timestamp found.
        """
        timestamp_match = re.search(r'\.(\d{17})(?:\.|$)', file_name)
        if not timestamp_match:
            return None

        timestamp_str = timestamp_match.group(1)
        try:
            year = int(timestamp_str[0:4])
            month = int(timestamp_str[4:6])
            day = int(timestamp_str[6:8])
            hour = int(timestamp_str[8:10])
            minute = int(timestamp_str[10:12])
            second = int(timestamp_str[12:14])
            microsecond = int(timestamp_str[14:17]) * 1000
            return datetime.datetime(year, month, day, hour, minute, second, microsecond)
        except (ValueError, IndexError):
            return None

    def _is_file_in_time_range(self, file_time_dt, from_time_dt, to_time_dt) -> bool:
        """Check if file time is within the query range"""
        return from_time_dt <= file_time_dt <= to_time_dt
