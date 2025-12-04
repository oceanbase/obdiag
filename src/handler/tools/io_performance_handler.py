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
@time: 2025/12/04
@file: io_performance_handler.py
@desc: IO performance inspection using tsar
"""
from src.common.ssh_client.ssh import SshClient
from src.common.tool import Util
from src.common.result_type import ObdiagResult


class IoPerformanceHandler:
    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.options = self.context.options
        self.ob_cluster = self.context.cluster_config
        self.observer_nodes = []
        self._init_observer_nodes()

    def _init_observer_nodes(self):
        """Initialize observer nodes with SSH clients"""
        observer_nodes = self.ob_cluster.get("servers")
        if observer_nodes:
            for node in observer_nodes:
                try:
                    ssh = SshClient(self.context, node)
                    node["ssher"] = ssh
                    self.observer_nodes.append(node)
                except Exception as e:
                    self.stdio.warn("Failed to create SSH client for node {0}: {1}".format(node.get("ip"), e))

    def _get_disk_device(self, node, disk_type):
        """
        Get disk device name for clog or data directory
        :param node: node configuration dict
        :param disk_type: 'clog' or 'data'
        :return: disk device name (e.g., 'sda')
        """
        try:
            ssh_client = node.get("ssher")
            if not ssh_client:
                return None

            if disk_type == "clog":
                redo_dir = node.get("redo_dir")
                if not redo_dir:
                    self.stdio.error("redo_dir is not configured for node {0}".format(node.get("ip")))
                    return None
                # Find clog directory
                cmd = 'find {0}/ -name "clog" -type d 2>/dev/null | head -1'.format(redo_dir)
                self.stdio.verbose("Executing: {0}".format(cmd))
                log_dir_path = ssh_client.exec_cmd(cmd).strip()
                if not log_dir_path:
                    self.stdio.error("Cannot find clog directory in {0}".format(redo_dir))
                    return None
            elif disk_type == "data":
                data_dir = node.get("data_dir")
                if not data_dir:
                    self.stdio.error("data_dir is not configured for node {0}".format(node.get("ip")))
                    return None
                # Find sstable directory
                cmd = 'find {0}/ -name "sstable" -type d 2>/dev/null | head -1'.format(data_dir)
                self.stdio.verbose("Executing: {0}".format(cmd))
                log_dir_path = ssh_client.exec_cmd(cmd).strip()
                if not log_dir_path:
                    self.stdio.error("Cannot find sstable directory in {0}".format(data_dir))
                    return None
            else:
                return None

            # Get the disk device name
            cmd = 'device=$(df -P {0} | awk \'NR==2{{print $1}}\') && while [[ -n $device ]]; do parent=$(lsblk -no PKNAME $device 2>/dev/null); if [[ -n $parent ]]; then device=$parent; else echo $device; break; fi; done'.format(log_dir_path)
            self.stdio.verbose("Executing: {0}".format(cmd))
            disk_device = ssh_client.exec_cmd(cmd).strip()
            # Extract device name (e.g., /dev/sda -> sda)
            if disk_device.startswith('/dev/'):
                disk_device = disk_device.replace('/dev/', '')
            return disk_device
        except Exception as e:
            self.stdio.error("Failed to get disk device: {0}".format(e))
            return None

    def _check_tsar_installed(self, ssh_client):
        """Check if tsar is installed"""
        try:
            cmd = "which tsar"
            result = ssh_client.exec_cmd(cmd).strip()
            if result and "tsar" in result:
                return True
            return False
        except Exception:
            return False

    def _execute_tsar(self, ssh_client, disk_device, date=None, duration=25):
        """
        Execute tsar command to collect IO performance data
        :param ssh_client: SSH client
        :param disk_device: disk device name (e.g., 'sda')
        :param date: date for historical data (format: YYYYMMDD), None for real-time
        :param duration: collection duration in seconds (default: 25)
        :return: tsar output
        """
        try:
            if date:
                # Historical data collection
                cmd = "timeout {0} tsar --io -d {1} -I {2} -i 1 2>&1".format(duration, date, disk_device)
            else:
                # Real-time collection
                cmd = "timeout {0} tsar --io -I {1} -i 1 2>&1".format(duration, disk_device)

            self.stdio.verbose("Executing tsar command: {0}".format(cmd))
            result = ssh_client.exec_cmd(cmd)
            return result
        except Exception as e:
            self.stdio.error("Failed to execute tsar: {0}".format(e))
            return None

    def _parse_tsar_output(self, output):
        """
        Parse tsar output and extract await values
        :param output: tsar command output
        :return: list of await values in milliseconds
        """
        await_values = []
        if not output:
            return await_values

        lines = output.split('\n')
        await_column_index = None

        for line in lines:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            # Find the header line to determine column index
            if 'await' in line.lower() or 'Time' in line:
                parts = line.split()
                try:
                    await_column_index = [p.lower() for p in parts].index('await')
                except ValueError:
                    # Try to find it by position (usually second column after Time)
                    if len(parts) > 1:
                        await_column_index = 1
                continue

            # Parse data lines
            if await_column_index is not None:
                parts = line.split()
                if len(parts) > await_column_index:
                    try:
                        await_val = float(parts[await_column_index])
                        await_values.append(await_val)
                    except (ValueError, IndexError):
                        continue
            else:
                # Fallback: try to parse as second column (after time)
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        # Skip time column (first), try second column
                        await_val = float(parts[1])
                        await_values.append(await_val)
                    except (ValueError, IndexError):
                        continue

        return await_values

    def _analyze_await_values(self, await_values, threshold=10.0):
        """
        Analyze await values and determine if there are performance issues
        :param await_values: list of await values in milliseconds
        :param threshold: threshold in milliseconds (default: 10ms)
        :return: (has_issue, high_await_count, total_count, max_await)
        """
        if not await_values:
            return False, 0, 0, 0.0

        total_count = len(await_values)
        high_await_count = sum(1 for val in await_values if val > threshold)
        max_await = max(await_values) if await_values else 0.0

        # If more than 50% of values exceed threshold, consider it a frequent issue
        has_issue = (high_await_count / total_count) > 0.5 if total_count > 0 else False

        return has_issue, high_await_count, total_count, max_await

    def handle(self):
        """Main handler method"""
        try:
            disk = Util.get_option(self.options, 'disk')
            date = Util.get_option(self.options, 'date')

            if not disk:
                self.stdio.error("--disk parameter is required. Use 'clog', 'data', or device name (e.g., sda)")
                return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="--disk parameter is required")

            if not self.observer_nodes:
                self.stdio.error("No observer nodes available. Please check your cluster configuration.")
                return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="No observer nodes available")

            self.stdio.print("Starting IO performance inspection...")
            self.stdio.print("Disk: {0}, Date: {1}".format(disk, date if date else "real-time"))

            all_results = []
            for node in self.observer_nodes:
                ip = node.get("ip")
                ssh_client = node.get("ssher")
                if not ssh_client:
                    self.stdio.warn("Node {0} has no SSH client, skipping".format(ip))
                    continue

                self.stdio.print("\nChecking node: {0}".format(ip))

                # Check if tsar is installed
                if not self._check_tsar_installed(ssh_client):
                    self.stdio.error("tsar is not installed on node {0}. Please install tsar first.".format(ip))
                    all_results.append({"node": ip, "status": "failed", "error": "tsar not installed"})
                    continue

                # Get disk device
                if disk in ["clog", "data"]:
                    disk_device = self._get_disk_device(node, disk)
                    if not disk_device:
                        self.stdio.error("Failed to get disk device for {0} on node {1}".format(disk, ip))
                        all_results.append({"node": ip, "status": "failed", "error": "Failed to get disk device"})
                        continue
                    self.stdio.verbose("Detected disk device: {0}".format(disk_device))
                else:
                    disk_device = disk

                # Execute tsar command
                self.stdio.print("Collecting IO performance data for {0} seconds...".format(25))
                tsar_output = self._execute_tsar(ssh_client, disk_device, date, duration=25)

                if not tsar_output:
                    self.stdio.error("Failed to collect tsar data on node {0}".format(ip))
                    all_results.append({"node": ip, "status": "failed", "error": "Failed to collect tsar data"})
                    continue

                # Parse await values
                await_values = self._parse_tsar_output(tsar_output)
                if not await_values:
                    self.stdio.warn("No await values found in tsar output on node {0}".format(ip))
                    all_results.append({"node": ip, "status": "warning", "error": "No await values found"})
                    continue

                # Analyze await values
                has_issue, high_count, total_count, max_await = self._analyze_await_values(await_values, threshold=10.0)

                result = {
                    "node": ip,
                    "disk_device": disk_device,
                    "status": "issue" if has_issue else "normal",
                    "total_samples": total_count,
                    "high_await_count": high_count,
                    "max_await_ms": round(max_await, 2),
                    "await_values": await_values[:10],  # Show first 10 values
                }
                all_results.append(result)

                # Print results
                self.stdio.print("Results for node {0}:".format(ip))
                self.stdio.print("  Disk device: {0}".format(disk_device))
                self.stdio.print("  Total samples: {0}".format(total_count))
                self.stdio.print("  Samples with await > 10ms: {0} ({1}%)".format(high_count, round(high_count * 100.0 / total_count, 1) if total_count > 0 else 0))
                self.stdio.print("  Max await: {0} ms".format(round(max_await, 2)))
                if has_issue:
                    self.stdio.print("  Status: CRITICAL - Disk IO performance issue detected (await frequently > 10ms)")
                else:
                    self.stdio.print("  Status: NORMAL - Disk IO performance is acceptable")

            # Summary
            self.stdio.print("\n" + "=" * 60)
            self.stdio.print("Summary:")
            issue_nodes = [r for r in all_results if r.get("status") == "issue"]
            if issue_nodes:
                self.stdio.print("  CRITICAL: {0} node(s) have disk IO performance issues".format(len(issue_nodes)))
                for r in issue_nodes:
                    self.stdio.print("    - {0} (disk: {1}, max await: {2}ms)".format(r["node"], r["disk_device"], r["max_await_ms"]))
            else:
                self.stdio.print("  All nodes have normal disk IO performance")

            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"results": all_results})

        except Exception as e:
            self.stdio.error("IO performance inspection failed: {0}".format(e))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="IO performance inspection failed: {0}".format(e))
