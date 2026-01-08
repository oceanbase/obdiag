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
@file: oms_cdc.py
@desc: OMS CDC log gathering
"""

import os
import re

from src.handler.gather.gather_log.base import BaseGatherLogOnNode


class OmsCdcGatherLogOnNode(BaseGatherLogOnNode):
    """OMS CDC log gathering handler"""

    TARGET_NAME = "oms_cdc"
    LOG_SCOPES = {
        "libobcdc": {"key": "*libobcdc.log*"},
        "removed_log_files": {"key": "removed_log_files"},
        "store": {"key": "store.log"},
    }

    def __init__(self, context, node, config):
        self.oms_component_id = config.get("oms_component_id")
        if self.oms_component_id is None:
            raise Exception("gather log on oms_cdc, but oms_component_id is None. please check your config")
        self.obcdc_id = self._extract_obcdc_id(context)
        super().__init__(context, node, config)

    def _extract_obcdc_id(self, context) -> str:
        """Extract obcdc_id from oms_component_id"""
        # Pattern: x.x.x.x-{number}
        match = re.search(r"\d+\.\d+\.\d+\.\d+-(\d+)", self.oms_component_id)
        if match:
            return match.group(1)
        else:
            context.stdio.warn("can not get obcdc_id by component_id. please check component_id.")
            return ""

    def _get_log_path(self) -> str:
        """Get OMS CDC log path"""
        store_path = self.node.get("store_path")
        if store_path is None:
            raise Exception("gather log on oms_cdc, but store_path is None. please check your config")
        return os.path.join(store_path, "store" + self.obcdc_id, "log")

    def _get_all_logfile_names(self, log_files) -> list:
        """
        Override to handle OMS CDC special case.
        Only collect libobcdc.log related files.
        """
        log_name_list = []

        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            if file_name == "":
                self.stdio.verbose("existing file name is empty")
                continue
            if "libobcdc.log" in file_name:
                log_name_list.append(file_name)

        self.stdio.verbose("OMS CDC get all log file name list, found {0} files".format(len(log_name_list)))
        return log_name_list

    def _get_logfile_names_by_time(self, log_files) -> list:
        """
        OMS CDC: not support time filtering, return all 'libobcdc.log*' files.
        """
        self.stdio.warn("oms_cdc not support get log file name list by time, return all 'libobcdc.log*' log file name list")
        log_name_list = []

        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            if file_name == "":
                self.stdio.verbose("existing file name is empty")
                continue
            if "libobcdc.log" in file_name:
                log_name_list.append(file_name)
                continue

        self.stdio.verbose("OMS CDC get log file name list, found {0} files".format(len(log_name_list)))
        return log_name_list
