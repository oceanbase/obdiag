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
@file: time_jump.py
@desc: Detect time jump anomalies in log files
"""

import datetime
import os
from typing import List, Dict, Optional, Any
from src.common.tool import TimeUtils


class time_jump:
    def __init__(self, stdio=None):
        self.stdio = stdio
        self.max_backward_threshold = datetime.timedelta(seconds=1)  # 1 second backward is considered anomaly
        self.max_forward_threshold = datetime.timedelta(hours=1)  # 1 hour forward jump is considered anomaly
        self.warn_count = 0
        self.max_warn_count = 10  # Limit WARN logs to 10
        self.output_file_path = None  # Store output file path for writing time_jump_error.txt
        self.anomaly_lines = []  # Store log lines with time jump anomalies

    def redact(self, text, output_file_path=None):
        """
        Redact method that detects time jump anomalies in the text.
        For time_jump, we don't modify the text, just detect anomalies.
        If time jump is detected, the log lines are written to time_jump_error.txt.

        :param text: Log content to process
        :param output_file_path: Optional output file path, used to determine where to write time_jump_error.txt
        :return: Original text (unchanged)
        """
        # Store output file path if provided
        if output_file_path:
            self.output_file_path = output_file_path

        # Reset anomaly lines for each file
        self.anomaly_lines = []

        try:
            prev_timestamp = None
            prev_line = None
            lines = text.split('\n')

            for line_idx, line in enumerate(lines):
                line_stripped = line.strip()
                if not line_stripped:
                    continue

                # Extract timestamp from log line
                timestamp_str = TimeUtils.extract_time_from_log_file_text(line_stripped, self.stdio)

                if not timestamp_str:
                    continue

                try:
                    current_timestamp = datetime.datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

                    if prev_timestamp is not None:
                        time_delta = current_timestamp - prev_timestamp

                        # Check for backward time jump
                        if time_delta < -self.max_backward_threshold:
                            # Record the log line with time jump
                            self.anomaly_lines.append(line)
                            # Limit WARN logs to max_warn_count
                            if self.warn_count < self.max_warn_count:
                                if self.stdio:
                                    self.stdio.warn(
                                        "Time jump detector: time backward jump detected at line {0}: {1} -> {2} (delta: {3:.2f}s)".format(
                                            line_idx + 1, prev_timestamp.strftime("%Y-%m-%d %H:%M:%S"), current_timestamp.strftime("%Y-%m-%d %H:%M:%S"), time_delta.total_seconds()
                                        )
                                    )
                                self.warn_count += 1

                        # Check for large forward time jump
                        elif time_delta > self.max_forward_threshold:
                            # Record the log line with time jump
                            self.anomaly_lines.append(line)
                            # Limit WARN logs to max_warn_count
                            if self.warn_count < self.max_warn_count:
                                if self.stdio:
                                    self.stdio.warn(
                                        "Time jump detector: large time forward jump detected at line {0}: {1} -> {2} (delta: {3:.2f}s)".format(
                                            line_idx + 1, prev_timestamp.strftime("%Y-%m-%d %H:%M:%S"), current_timestamp.strftime("%Y-%m-%d %H:%M:%S"), time_delta.total_seconds()
                                        )
                                    )
                                self.warn_count += 1

                    prev_timestamp = current_timestamp
                    prev_line = line

                except ValueError as e:
                    if self.stdio:
                        self.stdio.verbose("Time jump detector: failed to parse timestamp from line {0}: {1}".format(line_idx + 1, str(e)))
                    continue

            # Write anomaly lines to time_jump_error.txt if any detected
            if self.anomaly_lines and self.output_file_path:
                self.__write_anomaly_lines()

        except Exception as e:
            # Don't raise exception, just log it
            if self.stdio:
                self.stdio.verbose("Time jump detector: error detecting time jump: {0}".format(str(e)))

        return text

    def __write_anomaly_lines(self):
        """Write anomaly log lines to time_jump_error.txt"""
        try:
            # Determine the directory where time_jump_error.txt should be written
            if self.output_file_path:
                output_dir = os.path.dirname(self.output_file_path)
                error_file = os.path.join(output_dir, "time_jump_error.txt")
            else:
                return

            # Append to the file (in case multiple files are processed in the same directory)
            with open(error_file, 'a', encoding='utf-8') as f:
                for line in self.anomaly_lines:
                    f.write(line + '\n')

            # Clear the anomaly lines after writing
            self.anomaly_lines = []

        except Exception as e:
            if self.stdio:
                self.stdio.verbose("Time jump detector: error writing to time_jump_error.txt: {0}".format(str(e)))


# Plugin instance - will be initialized with stdio when used
time_jump = time_jump()
