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
@time: 2025/07/09
@file: gather_core.py
@desc: Gather observer core files when observer process crashes
"""
import os
import time
import datetime
import re

# Removed tabulate import - now using BaseHandler._generate_summary_table
from src.common.base_handler import BaseHandler
from src.common.tool import TimeUtils
from src.common.command import download_file
from src.common.constant import const
from src.common.ssh_client.ssh import SshClient
from src.common.tool import Util
from src.common.tool import DirectoryUtil
from src.common.tool import FileUtil
from src.common.result_type import ObdiagResult


class GatherCoreHandler(BaseHandler):
    def _init(self, gather_pack_dir='./', is_scene=False, **kwargs):
        """Subclass initialization"""
        self.local_stored_path = gather_pack_dir
        self.is_scene = is_scene
        self.config_path = const.DEFAULT_CONFIG_PATH

        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

        # Initialize config
        self.nodes = self.context.cluster_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.nodes, self.stdio)
        if new_nodes:
            self.nodes = new_nodes

        # Use ConfigAccessor if available (file_number_limit is from BaseHandler @property)
        if self.config:
            self.config_path = self.config.basic_config_path
        else:
            # Fallback to direct config access
            if self.context.inner_config is None:
                pass  # file_number_limit uses BaseHandler default
            else:
                basic_config = self.context.inner_config['obdiag']['basic']
                self.config_path = basic_config['config_path']

        # Initialize options
        store_dir_option = self._get_option('store_dir')
        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self._log_warn(f'args --store_dir [{os.path.abspath(store_dir_option)}] incorrect: No such directory, Now create it')
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_path = os.path.abspath(store_dir_option)

        # Get time range options
        self.from_time = self._get_option('from')
        self.to_time = self._get_option('to')
        self.since = self._get_option('since')

        # Handle --since parameter
        if self.since and not self.from_time:
            # Calculate time range based on since (e.g. '30m' -> 30 minutes ago to now)
            now_time = datetime.datetime.now()
            self.to_time = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(self.since))).strftime('%Y-%m-%d %H:%M:%S')

    def handle(self) -> ObdiagResult:
        """Main handle logic"""
        self._validate_initialized()

        try:
            if self.is_scene:
                pack_dir_this_command = self.local_stored_path
            else:
                pack_dir_this_command = os.path.join(self.local_stored_path, f"obdiag_gather_{TimeUtils.timestamp_to_filename_time(self.gather_timestamp)}")
            self._log_verbose(f"Use {pack_dir_this_command} as pack dir.")
            gather_tuples = []

            def handle_from_node(node):
                st = time.time()
                resp = self.__handle_from_node(pack_dir_this_command, node)
                file_size = ""
                if len(resp["error"]) == 0:
                    file_size = os.path.getsize(resp["gather_pack_path"])
                gather_tuples.append((node.get("ip"), False, resp["error"], file_size, int(time.time() - st), resp["gather_pack_path"]))

            exec_tag = False
            for node in self.nodes:
                if node.get("ssh_type") == "docker" or node.get("ssh_type") == "kubernetes":
                    self._log_warn(f"Skip gather from node {node.get('ip')} because it is a docker or kubernetes node")
                    continue
                handle_from_node(node)
                exec_tag = True

            if not exec_tag:
                self._log_verbose("No node to gather from, skip")
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

            summary_tuples = self.__get_overall_summary(gather_tuples)
            self._log_info(summary_tuples)
            # Persist the summary results to a file
            FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)
            last_info = f"For result details, please run cmd \033[32m' cat {os.path.join(pack_dir_this_command, 'result_summary.txt')} '\033[0m\n"
            self._log_info(last_info)
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

        except Exception as e:
            return self._handle_error(e)

    def __handle_from_node(self, local_stored_path, node):
        resp = {"skip": False, "error": "", "gather_pack_path": ""}
        remote_ip = node.get("ip")
        remote_user = node.get("ssh_username")
        self._log_verbose(f"Sending Collect Core Command to node {remote_ip} ...")
        DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)

        ssh_client = None
        try:
            ssh_client = SshClient(self.context, node)
        except Exception as e:
            self.stdio.exception(f"ssh {remote_user}@{remote_ip}: failed, Please check the node conf.")
            resp["skip"] = True
            resp["error"] = "Please check the node conf."
            raise Exception("Please check the node conf.")

        # Check if observer process exists
        observer_process_count = self.__check_observer_process(ssh_client)
        if observer_process_count > 0:
            self._log_info(f"Observer process is running on {remote_ip}")
        else:
            self._log_warn(f"No running observer process found on {remote_ip}")

        # Check if observer.log contains CRASH ERROR
        crash_error_found = self.__check_crash_error_in_log(ssh_client, node.get("home_path"))
        if not crash_error_found:
            self._log_warn("No CRASH ERROR found in logs, continuing with subsequent collection steps")

        # Get core file pattern and search for core files with time filter
        core_files_info = self.__find_core_files_with_info(ssh_client, node.get("home_path"))
        if not core_files_info:
            resp["error"] = "No core files found within time range: {0} to {1}".format(self.from_time, self.to_time)
            resp["gather_pack_path"] = "{0}".format(local_stored_path)
            return resp

        # Show core files info and get user confirmation
        if not self.__show_core_files_and_confirm(core_files_info, remote_ip):
            resp["error"] = "User cancelled the download"
            resp["gather_pack_path"] = "{0}".format(local_stored_path)
            return resp

        # Download core files directly
        downloaded_files = self.__download_core_files(ssh_client, core_files_info, local_stored_path, remote_ip)
        if not downloaded_files:
            resp["error"] = "Failed to download core files from {0}".format(remote_ip)
            resp["gather_pack_path"] = "{0}".format(local_stored_path)
            return resp

        resp["error"] = ""
        resp["gather_pack_path"] = local_stored_path
        return resp

    def __check_observer_process(self, ssh_client):
        """Check if observer process is running"""
        cmd = "ps -ef | grep observer | grep -v grep | grep -v obshell | wc -l"
        result = ssh_client.exec_cmd(cmd)
        process_count = int(result.strip())
        self.stdio.verbose("Observer process count: {0}".format(process_count))
        return process_count

    def __check_crash_error_in_log(self, ssh_client, home_path):
        """Check if observer.log (including archives) contains CRASH ERROR within time range"""
        log_dir = "{0}/log".format(home_path)
        from_time = self.from_time
        to_time = self.to_time

        # Parse from and to times into datetime objects
        try:
            from_datetime = datetime.datetime.strptime(from_time, '%Y-%m-%d %H:%M:%S')
            to_datetime = datetime.datetime.strptime(to_time, '%Y-%m-%d %H:%M:%S')
        except Exception as e:
            self.stdio.warn("Invalid time format: {0}".format(e))
            return False

        # Find all observer.log* files
        find_cmd = "find {0} -maxdepth 1 -type f -name 'observer.log*' | sort".format(log_dir)
        try:
            result = ssh_client.exec_cmd(find_cmd)
            log_files = result.strip().split('\n') if result.strip() else []
        except Exception as e:
            self.stdio.warn("Failed to find log files: {0}".format(e))
            return False

        relevant_files = []
        for file_path in log_files:
            filename = os.path.basename(file_path)

            # Always include the main observer.log
            if filename == 'observer.log':
                relevant_files.append(file_path)
                continue

            # Check archived logs with timestamps (e.g., observer.log.20250710155331286)
            parts = filename.split('.')
            if len(parts) >= 3 and parts[0] == 'observer' and parts[1] == 'log':
                timestamp_str = parts[2]
                try:
                    # Parse timestamp (format: YYYYMMDDHHMMSSsss)
                    log_time = datetime.datetime.strptime(timestamp_str, '%Y%m%d%H%M%S%f')
                    # Check if log_time is within the target range
                    if from_datetime <= log_time <= to_datetime:
                        relevant_files.append(file_path)
                except ValueError:
                    # Invalid timestamp format, skip
                    self.stdio.verbose("Skipping invalid log file: {0}".format(filename))
                    continue

        if not relevant_files:
            self.stdio.verbose("No relevant log files found in time range")
            return False

        # Search for CRASH ERROR in all relevant files
        total_crash_count = 0
        for log_file in relevant_files:
            # Handle compressed logs
            cmd = "grep -i 'CRASH ERROR' {0} | wc -l".format(log_file)
            try:
                result = ssh_client.exec_cmd(cmd)
                crash_count = int(result.strip())
                total_crash_count += crash_count
                self.stdio.print("CRASH ERROR count in {0}: {1}".format(log_file, crash_count))
            except Exception as e:
                self.stdio.warn("Failed to check CRASH ERROR in {0}: {1}".format(log_file, e))
                continue

        return total_crash_count > 0

    def __get_core_pattern(self, ssh_client):
        """Get core pattern from /etc/sysctl.conf"""
        cmd = "grep 'kernel.core_pattern' /etc/sysctl.conf | awk -F '=' '{print $2}' | xargs echo -n | xargs dirname"
        try:
            result = ssh_client.exec_cmd(cmd)
            if result.strip():
                return result.strip()
            return None
        except Exception as e:
            self.stdio.warn("Failed to get core pattern from /etc/sysctl.conf: {0}".format(e))
            return None

    def __find_core_files_with_info(self, ssh_client, home_path):
        """Find core files with detailed info including size and modification time"""
        core_files_info = []

        # Get core pattern
        core_pattern = self.__get_core_pattern(ssh_client)

        # Search for core files based on pattern
        if core_pattern:
            cmd = "find {0} -name 'core.*' -type f -ls 2>/dev/null".format(core_pattern)
            result = ssh_client.exec_cmd(cmd)
            if result.strip():
                for line in result.strip().split('\n'):
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 8:
                            inode = parts[0]
                            size = int(parts[6])
                            mtime = ' '.join(parts[7:10])
                            file_path = parts[10]
                            core_files_info.append({'path': file_path, 'size': size, 'mtime': mtime, 'inode': inode})

        # Also check default location in home_path
        cmd = "find {0} -name 'core.*' -type f -ls 2>/dev/null".format(home_path)
        result = ssh_client.exec_cmd(cmd)
        if result.strip():
            for line in result.strip().split('\n'):
                if line.strip():
                    parts = line.split()
                    if len(parts) >= 8:
                        inode = parts[0]
                        size = int(parts[6])
                        mtime = ' '.join(parts[7:10])
                        file_path = parts[10]
                        core_files_info.append({'path': file_path, 'size': size, 'mtime': mtime, 'inode': inode})

        # Filter by time range if specified
        if self.from_time or self.to_time:
            core_files_info = self.__filter_core_files_by_time(core_files_info)

        # Remove duplicates based on inode
        seen_inodes = set()
        unique_core_files = []
        for file_info in core_files_info:
            if file_info['inode'] not in seen_inodes:
                seen_inodes.add(file_info['inode'])
                unique_core_files.append(file_info)

        self._log_verbose(f"Found {len(unique_core_files)} core files")
        return unique_core_files

    def __filter_core_files_by_time(self, core_files_info):
        """Filter core files by time range"""
        if not self.from_time and not self.to_time:
            return core_files_info

        filtered_files = []
        for file_info in core_files_info:
            # Parse modification time
            try:
                # Convert mtime to timestamp for comparison
                mtime_str = file_info['mtime']
                # Parse the time format from ls output
                # Format: "Dec 19 10:30" or "Dec 19 2023"
                current_year = datetime.datetime.now().year
                if ':' in mtime_str:
                    # Same year format: "Dec 19 10:30"
                    mtime_parsed = datetime.datetime.strptime(f"{mtime_str} {current_year}", "%b %d %H:%M %Y")
                else:
                    # Different year format: "Dec 19 2023"
                    mtime_parsed = datetime.datetime.strptime(mtime_str, "%b %d %Y")

                file_timestamp = mtime_parsed.timestamp()

                # Check time range
                include_file = True
                if self.from_time:
                    from_datetime = TimeUtils.parse_time_str(self.from_time, self.stdio)
                    from_timestamp = from_datetime.timestamp()
                    if file_timestamp < from_timestamp:
                        include_file = False

                if self.to_time and include_file:
                    to_datetime = TimeUtils.parse_time_str(self.to_time, self.stdio)
                    to_timestamp = to_datetime.timestamp()
                    if file_timestamp > to_timestamp:
                        include_file = False

                if include_file:
                    filtered_files.append(file_info)
                    self._log_info(f"Selected core file: {file_info}")
                else:
                    self._log_info(f"Unselected core file: {file_info}")

            except Exception as e:
                self._log_warn(f"Failed to parse time for file {file_info['path']}: {e}")
                # Include file if time parsing fails
                filtered_files.append(file_info)

        return filtered_files

    def __show_core_files_and_confirm(self, core_files_info, remote_ip):
        """Show core files info and get user confirmation"""
        if not core_files_info:
            return False

        total_size = sum(file_info['size'] for file_info in core_files_info)
        total_size_gb = total_size / (1024 * 1024 * 1024)

        self._log_info(f"\n=== Core Files Found on {remote_ip} ===")
        self._log_info(f"Total files: {len(core_files_info)}, Total size: {total_size_gb:.2f} GB")
        self._log_info("\nFile details:")

        # Create table for display
        table_data = []
        for i, file_info in enumerate(core_files_info, 1):
            size_mb = file_info['size'] / (1024 * 1024)
            table_data.append([i, os.path.basename(file_info['path']), f"{size_mb:.2f} MB", file_info['mtime'], file_info['path']])

        headers = ["#", "Filename", "Size", "Modified Time", "Full Path"]
        # Use BaseHandler template method for table generation
        table_str = self._generate_summary_table(headers, table_data, "Core Files Found")
        # Note: _generate_summary_table already logs the table, so we don't need to print again

        # Ask for user confirmation
        confirm = input("\nDo you want to download these core files? (y/n): ").strip().lower()
        if confirm in ['y', 'yes']:
            return True
        else:
            # 当输入不是'y'或'yes'时，结束收集
            self._log_info("User declined to download core files, terminating collection.")
            return False

    def __download_core_files(self, ssh_client, core_files_info, local_stored_path, remote_ip):
        """Download core files directly from remote to local"""
        downloaded_files = []

        for i, file_info in enumerate(core_files_info, 1):
            remote_path = file_info['path']
            filename = os.path.basename(remote_path)
            local_path = os.path.join(local_stored_path, f"{remote_ip}_{filename}")

            self._log_info(f"Downloading file {i}/{len(core_files_info)}: {filename} ({file_info['size'] / (1024 * 1024):.2f} MB)")

            try:
                download_file(ssh_client, remote_path, local_path, self.stdio)
                downloaded_files.append(local_path)
                self._log_info(f"✓ Downloaded: {local_path}")
            except Exception as e:
                self._log_error(f"Failed to download {remote_path}: {e}")

        return downloaded_files

    def __get_overall_summary(self, node_summary_tuple):
        """
        Generate overall summary from gather tuples using BaseHandler template method.
        :param node_summary_tuple: List of tuples (node, is_err, error_msg, file_size, consume_time, pack_path)
        :return: Formatted summary table string
        """
        summary_tab = []
        field_names = ["Node", "Status", "Size", "Time", "PackPath"]
        for tup in node_summary_tuple:
            node = tup[0]
            is_err = tup[2]
            file_size = tup[3]
            consume_time = tup[4]
            pack_path = tup[5]
            try:
                format_file_size = FileUtil.size_format(num=file_size, output_str=True)
            except Exception as e:
                self._log_verbose("Failed to format file size {0}: {1}".format(file_size, e))
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        # Use BaseHandler template method
        return self._generate_summary_table(field_names, summary_tab, "Gather Core Summary")
