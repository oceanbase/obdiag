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
@time: 2025/12/10
@file: archive_continuous_error.py
@desc: Check for archive continuous errors by searching "pay ATTENTION!! archive continuous encounter error more than 15" in observer logs.
       Reference: https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000003272139
       issue #991
"""

import threading
from src.handler.check.check_task import TaskBase


class ArchiveContinuousError(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def _check_node(self, node, error_keyword, results, lock):
        """Check a single node for archive errors in logs"""
        try:
            ssh_client = node.get("ssher")
            if ssh_client is None:
                with lock:
                    results["failures"].append({"node": node.get("ip", "unknown"), "error": "ssh client is None"})
                return

            node_name = ssh_client.get_name()
            self.stdio.verbose("Checking archive error logs on node: {0}".format(node_name))

            # Get observer log directory from node info
            home_path = node.get("home_path", "")
            if not home_path:
                self.stdio.warn("Node {0} home_path is empty, skip log check".format(node_name))
                return

            log_dir = "{0}/log".format(home_path)

            # Search for the error keyword in observer.log and observer.log.wf files
            grep_cmd = "grep -l '{0}' {1}/observer.log* 2>/dev/null | head -5".format(error_keyword, log_dir)

            result = ssh_client.exec_cmd(grep_cmd).strip()
            if result:
                # Found the error keyword in log files
                matched_files = result.split('\n')
                self.stdio.verbose("Found archive error in {0} log files on node {1}".format(len(matched_files), node_name))

                with lock:
                    results["found_errors"].append({"node": node_name, "files": matched_files})
            else:
                self.stdio.verbose("No archive error found on node {0}".format(node_name))

        except Exception as e:
            self.stdio.warn("Failed to search logs on node {0}: {1}".format(node.get("ip", "unknown"), e))
            with lock:
                results["failures"].append({"node": node.get("ip", "unknown"), "error": str(e)})

    def execute(self):
        try:
            # This feature is only available in OceanBase 4.x
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Archive check is only supported in OceanBase 4.x and later")
                return

            self.stdio.verbose("Starting archive continuous error check in observer logs...")

            error_keyword = "pay ATTENTION!! archive continuous encounter error more than 15"

            # Shared results container with thread lock
            results = {"found_errors": [], "failures": []}
            lock = threading.Lock()

            # Create threads for each node
            threads = []
            for node in self.observer_nodes:
                thread = threading.Thread(target=self._check_node, args=(node, error_keyword, results, lock))
                thread.daemon = True
                threads.append(thread)

            # Start all threads
            for thread in threads:
                thread.start()

            # Wait for all threads to complete with timeout
            timeout_per_node = 60  # 60 seconds timeout per node
            for thread in threads:
                thread.join(timeout=timeout_per_node)

            # Check for threads that didn't complete
            for i, thread in enumerate(threads):
                if thread.is_alive():
                    self.stdio.warn("Thread for node check timed out")

            # Report failures
            for failure in results["failures"]:
                self.report.add_fail("Node {0}: {1}".format(failure["node"], failure["error"]))

            # Report findings
            if results["found_errors"]:
                for error in results["found_errors"]:
                    self.report.add_critical(
                        "Node {0}: Found 'pay ATTENTION!! archive continuous encounter error more than 15' in observer logs. "
                        "This indicates archive has encountered continuous errors. "
                        "Please check archive destination connectivity and storage. "
                        "Reference: https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000003272139. "
                        "Log files: {1}".format(error["node"], ", ".join(error["files"][:3]))  # Show at most 3 files
                    )
                self.stdio.verbose("Archive continuous error check completed, found issues on {0} nodes".format(len(results["found_errors"])))
            else:
                self.stdio.verbose("Archive continuous error check completed, no issues found")

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "archive_continuous_error",
            "info": "Check for 'pay ATTENTION!! archive continuous encounter error more than 15' in observer logs. "
            "This error indicates archive has encountered continuous errors more than 15 times. "
            "Reference: https://www.oceanbase.com/knowledge-base/oceanbase-database-1000000003272139.",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/991",
        }


archive_continuous_error = ArchiveContinuousError()
