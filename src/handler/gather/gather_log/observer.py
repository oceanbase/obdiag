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
    }

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

    # _get_logfile_names_by_time is inherited from BaseGatherLogOnNode
