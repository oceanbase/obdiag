#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
@time: 2025/07/16
@file: ob_query_timeout.py
@desc: Check ob_query_timeout global variable value
"""

from src.handler.checker.check_task import TaskBase
import re


class ObQueryTimeoutTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                self.report.add_fail("Database connection is not available")
                return

            # Query ob_query_timeout global variable
            sql = "SHOW GLOBAL VARIABLES LIKE 'ob_query_timeout'"

            try:
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
                results = cursor.fetchall()

                if not results:
                    self.report.add_fail("Failed to get ob_query_timeout value")
                    return

                for row in results:
                    variable_name = row.get('Variable_name', '')
                    value = row.get('Value', '')

                    self.stdio.verbose("ob_query_timeout value: {0}".format(value))

                    # Parse timeout value (convert microseconds to seconds)
                    timeout_seconds = self._parse_timeout_value(value)

                    if timeout_seconds is not None:
                        # 24 hours = 24 * 3600 = 86400 seconds
                        if timeout_seconds > 86400:
                            self.report.add_warning(
                                "ob_query_timeout is set to {0} microseconds ({1} seconds), which exceeds 24 hours. "
                                "This may cause threads to hang indefinitely when query timeout occurs. "
                                "Consider reducing the timeout value to prevent thread exhaustion issues.".format(value, timeout_seconds)
                            )
                            self.stdio.warn("ob_query_timeout exceeds 24 hours: {0} microseconds ({1} seconds)".format(value, timeout_seconds))
                        else:
                            self.stdio.verbose("ob_query_timeout is within acceptable range: {0} microseconds ({1} seconds)".format(value, timeout_seconds))
                    else:
                        self.report.add_fail("Failed to parse ob_query_timeout value: {0}".format(value))

            except Exception as e:
                self.report.add_fail("Failed to query ob_query_timeout: {0}".format(e))
                self.stdio.warn("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def _parse_timeout_value(self, value):
        """Parse timeout value and convert microseconds to seconds"""
        try:
            # Remove any whitespace and convert to integer
            value = value.strip()
            microseconds = int(value)

            # Convert microseconds to seconds
            seconds = microseconds / 1000000

            return seconds
        except (ValueError, AttributeError):
            return None

    def get_task_info(self):
        return {
            "name": "ob_query_timeout",
            "info": "Check ob_query_timeout global variable for potential thread hang issues. issue #978",
        }


ob_query_timeout = ObQueryTimeoutTask()