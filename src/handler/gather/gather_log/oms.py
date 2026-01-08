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
@desc: OMS log gathering
"""

import datetime
import os

from src.handler.gather.gather_log.base import BaseGatherLogOnNode


class OmsGatherLogOnNode(BaseGatherLogOnNode):
    """OMS log gathering handler"""

    TARGET_NAME = "oms"
    LOG_SCOPES = {
        "connector": {"key": "*connector.*"},
        "error": {"key": "error"},
        "trace.log": {"key": "trace.log"},
        "metrics": {"key": "metrics*"},
    }

    def __init__(self, context, node, config):
        self.oms_component_id = config.get("oms_component_id")
        if self.oms_component_id is None:
            raise Exception("gather log on oms, but oms_component_id is None. please check your config")
        super().__init__(context, node, config)

    def _get_log_path(self) -> str:
        """Get OMS log path"""
        run_path = self.node.get("run_path")
        if run_path is None:
            raise Exception("gather log on oms, but run_path is None. please check your config")
        return os.path.join(run_path, self.oms_component_id, "logs")

    def _get_all_logfile_names(self, log_files) -> list:
        """
        Override to handle OMS special case.
        Filter out .gz files except current hour's.
        """
        log_name_list = []
        formatted_time = datetime.datetime.now().strftime("%Y-%m-%d_%H")

        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            if file_name == "":
                self.stdio.verbose("existing file name is empty")
                continue
            # Include non-gz files, or gz files from current hour
            if "log.gz" not in file_name or formatted_time in file_name:
                log_name_list.append(file_name)

        self.stdio.verbose("OMS get all log file name list, found {0} files".format(len(log_name_list)))
        return log_name_list

    def _get_logfile_names_by_time(self, log_files) -> list:
        """
        OMS: get all logs (small size), filter out old .gz files.
        OMS logs are generally small, so we collect all of them.
        """
        log_name_list = []
        formatted_time = datetime.datetime.now().strftime("%Y-%m-%d_%H")

        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            if file_name == "":
                self.stdio.verbose("existing file name is empty")
                continue
            # Include non-gz files, or gz files from current hour
            if "log.gz" not in file_name or formatted_time in file_name:
                log_name_list.append(file_name)
                continue

        self.stdio.verbose("OMS get log file name list, found {0} files".format(len(log_name_list)))
        return log_name_list
