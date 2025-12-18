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
@time: 2025/12/18
@file: enable_ob_protocol_v2_with_client.py
@desc: Check obproxy enable_ob_protocol_v2_with_client parameter, alert if enabled
"""
from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class EnableObProtocolV2WithClientTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not self.obproxy_version:
                self.stdio.verbose("obproxy version not available, skip check")
                return None

            # Check if obproxy version is >= 4.0.0
            if StringUtils.compare_versions_lower(self.obproxy_version, "4.0.0"):
                self.stdio.verbose("obproxy version {0} is less than 4.0.0 skip check".format(self.obproxy_version))
                return None

            # Get obproxy enable_ob_protocol_v2_with_client parameter
            results = super().get_obproxy_parameter("enable_ob_protocol_v2_with_client")

            if not results:
                self.stdio.verbose("enable_ob_protocol_v2_with_client parameter not found, skip check")
                return

            for param in results:
                value = param.get('value', '')
                self.stdio.verbose("obproxy enable_ob_protocol_v2_with_client parameter value: {0}".format(value))

                # Check if the parameter is enabled (value is True or "True" or "true" or "1")
                if str(value).lower() in ('true', '1', 'on', 'yes'):
                    self.report.add_warning(
                        "obproxy enable_ob_protocol_v2_with_client is enabled (value: {0}). "
                        "This parameter enables OceanBase Protocol V2 between client and obproxy, "
                        "which may cause compatibility issues with some clients. "
                        "Please ensure your client supports this protocol before enabling it.".format(value)
                    )
                    self.stdio.warn("enable_ob_protocol_v2_with_client is enabled: {0}".format(value))
                else:
                    self.stdio.verbose("enable_ob_protocol_v2_with_client check passed, parameter is disabled")

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "enable_ob_protocol_v2_with_client",
            "info": "Check obproxy enable_ob_protocol_v2_with_client parameter, alert if enabled. issue #1139",
        }


enable_ob_protocol_v2_with_client = EnableObProtocolV2WithClientTask()
