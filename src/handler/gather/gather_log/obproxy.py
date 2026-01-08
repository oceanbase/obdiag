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
@file: obproxy.py
@desc: OBProxy log gathering
"""

import datetime
import os
import traceback

from src.handler.gather.gather_log.base import BaseGatherLogOnNode


class ObproxyGatherLogOnNode(BaseGatherLogOnNode):
    """OBProxy log gathering handler"""

    TARGET_NAME = "obproxy"
    LOG_SCOPES = {
        "obproxy": {"key": "*obproxy*"},
        "obproxy_diagnosis": {"key": "*obproxy_diagnosis*"},
        "obproxy_digest": {"key": "*obproxy_digest*"},
        "obproxy_stat": {"key": "*obproxy_stat*"},
        "obproxy_slow": {"key": "*obproxy_slow*"},
        "obproxy_limit": {"key": "*obproxy_limit*"},
    }

    def _get_log_path(self) -> str:
        """Get OBProxy log path"""
        return os.path.join(self.node.get("home_path"), "log")

    def _get_logfile_names_by_time(self, log_files) -> list:
        """
        Get log file names filtered by time range.
        Parse timestamp from filename for efficiency.
        Same logic as Observer.
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
            try:
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue

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
                    # File has no timestamp (e.g., obproxy.log, obproxy.log.wf)
                    # These are current log files being written, should be included
                    if file_name.endswith(".log") or file_name.endswith(".wf"):
                        log_name_list.append(file_name)
                        self.stdio.verbose("node: {0}, file {1} has no timestamp, is current log file, include it".format(self.ssh_client.get_name(), file_name))
                    else:
                        self.stdio.verbose("node: {0}, file {1} has no timestamp and is not a .log or .wf file, skip".format(self.ssh_client.get_name(), file_name))

            except Exception as e:
                self.stdio.warn("gather_log_on_node {0} get log file: {2} name failed, Skip it: {1}".format(self.ssh_client.get_ip(), str(e), file_name))
                self.stdio.verbose(traceback.format_exc())
                continue

        if len(log_name_list) > 0:
            self.stdio.verbose("Find the qualified log file {0} on Server [{1}]".format(log_name_list, self.ssh_client.get_ip()))
        else:
            self.stdio.warn("No found the qualified log file on Server [{0}]".format(self.ssh_client.get_name()))

        return log_name_list
