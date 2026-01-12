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
@time: 2025/01/23
@file: old_version.py
@desc: Check obproxy version. Some versions of obproxy are not recommended
"""

from src.handler.check.check_task import TaskBase


class OldVersionTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check if obproxy_version is available
            if not self.obproxy_version:
                self.report.add_fail("Failed to get obproxy version")
                return
            self.stdio.verbose("obproxy full version: {0}".format(self.obproxy_full_version))
            self.stdio.verbose("obproxy version: {0}".format(self.obproxy_version))

            # Deprecated version patterns
            deprecated_patterns = [
                "4.0",
                "4.1",
                "4.2",
                "4.3.0",
                "3",
                "4.3.1",
                "4.3.2",
            ]

            self.stdio.verbose("obproxy version: {0}".format(self.obproxy_version))

            # Check if version matches any deprecated pattern
            is_deprecated = False
            for pattern in deprecated_patterns:
                if self.obproxy_version.startswith(pattern):
                    is_deprecated = True
                    break

            if is_deprecated:
                self.report.add_warning("obproxy version {0} is not recommended, please upgrade to the obproxy".format(self.obproxy_version))
                # check obproxy_full_version
            full_version_list = ["4.3.2.0 42", "4.3.2.0 26", "4.3.1.0 4"]
            for full_version in full_version_list:
                if full_version in self.obproxy_full_version:
                    is_deprecated = True
                    break
            if is_deprecated:
                self.report.add_warning("obproxy full version {0} is not recommended, please upgrade to the obproxy".format(self.obproxy_full_version))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "old_version",
            "info": "Check obproxy version. Some versions of obproxy are not recommended",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1103",
        }


old_version = OldVersionTask()
