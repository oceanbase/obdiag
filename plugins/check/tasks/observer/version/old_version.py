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
@time: 2024/12/29
@file: old_version.py
@desc: Check observer version - Some old versions are not recommended
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils
import re


class OldVersionTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            version = self.observer_version
            if not version:
                self.stdio.verbose("Cannot get observer version")
                return

            # Check 3.1.x CE version
            if version.startswith("3.1."):
                self.report.add_warning("Starting from June 30, 2024, OceanBase Database Community Edition V3.1.x will no longer receive any bug fixes or version updates. Please upgrade as soon as possible")
                return

            # Check 4.x versions
            if super().check_ob_version_min("4.0.0.0"):
                # Not recommended versions: 4.0.*, 4.1.*, 4.2.2.*, 4.3.0.*, 4.3.1.*
                bad_prefixes = ["4.0.", "4.1.", "4.2.2.", "4.3.0.", "4.3.1."]

                for prefix in bad_prefixes:
                    if version.startswith(prefix):
                        self.report.add_warning("{0} is not recommended, please upgrade to the obcluster".format(version))
                        return

                # Check 4.2.1.x where x < 5
                if version.startswith("4.2.1."):
                    parts = version.split(".")
                    if len(parts) >= 4:
                        try:
                            minor = int(parts[3])
                            if minor < 5:
                                self.report.add_warning("{0} is not recommended, please upgrade to the obcluster".format(version))
                        except ValueError:
                            pass

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "old_version", "info": "Check observer version - Some old versions are not recommended."}


old_version = OldVersionTask()
