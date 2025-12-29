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
@file: sysbench_test_cluster_datafile_size.py
@desc: Check cluster info about datafile_size and log_disk_size for sysbench
"""

from src.handler.check.check_task import TaskBase
import re


class SysbenchTestClusterDatafileSizeTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if not super().check_ob_version_min("4.0.0.0"):
                self.stdio.verbose("Version not supported, skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if ssh_client is None:
                    continue

                node_name = ssh_client.get_name()
                remote_ip = node.get("ip")
                home_path = node.get("home_path", "")

                if not home_path:
                    continue

                try:
                    # Get datafile_size
                    sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where Name='datafile_size' and SVR_IP='{0}'".format(remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    datafile_size = 0
                    if result:
                        val = result[0].get('VALUE', '0')
                        match = re.search(r'\d+', str(val))
                        if match:
                            datafile_size = int(match.group())

                    # Get datafile_disk_percentage
                    sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where Name='datafile_disk_percentage' and SVR_IP='{0}'".format(remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    datafile_disk_percentage = int(result[0].get('VALUE', 0)) if result else 0

                    # Get data disk size
                    data_disk_cmd = "df {0}/store/sstable | awk 'NR==2{{print int($2/1024/1024)}}'".format(home_path)
                    data_path_os_disk_size = int(ssh_client.exec_cmd(data_disk_cmd).strip() or 0)

                    if data_path_os_disk_size > 0:
                        actual_datafile_size = datafile_size if datafile_size > 0 else (datafile_disk_percentage * data_path_os_disk_size // 100)
                        datafile_size_percentage = actual_datafile_size * 100 // data_path_os_disk_size

                    # Get log_disk_size
                    sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where Name='log_disk_size' and SVR_IP='{0}'".format(remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    logfile_size = 0
                    if result:
                        val = result[0].get('VALUE', '0')
                        match = re.search(r'\d+', str(val))
                        if match:
                            logfile_size = int(match.group())

                    # Get log_disk_percentage
                    sql = "select VALUE from oceanbase.GV$OB_PARAMETERS where Name='log_disk_percentage' and SVR_IP='{0}'".format(remote_ip)
                    result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    logfile_disk_percentage = int(result[0].get('VALUE', 0)) if result else 0

                    # Get log disk size
                    log_disk_cmd = "df {0}/store/clog | awk 'NR==2{{print int($2/1024/1024)}}'".format(home_path)
                    log_path_os_disk_size = int(ssh_client.exec_cmd(log_disk_cmd).strip() or 0)

                    if log_path_os_disk_size > 0:
                        actual_logfile_size = logfile_size if logfile_size > 0 else (logfile_disk_percentage * log_path_os_disk_size // 100)
                        logfile_size_percentage = actual_logfile_size * 100 // log_path_os_disk_size

                    # Check if data and log are on the same disk
                    data_dir = ssh_client.exec_cmd("df -h {0}/store/sstable | grep '/' | awk '{{print $6}}'".format(home_path)).strip()
                    log_dir = ssh_client.exec_cmd("df -h {0}/store/clog | grep '/' | awk '{{print $6}}'".format(home_path)).strip()

                    if data_dir == log_dir and data_dir:
                        if data_path_os_disk_size > 0 and datafile_size_percentage <= 60:
                            self.report.add_warning("On {0}: When the data disk and log disk are on the same disk, the data disk needs to occupy more than 60% of the disk space. now it is {1}%".format(node_name, datafile_size_percentage))
                        if log_path_os_disk_size > 0 and logfile_size_percentage <= 30:
                            self.report.add_warning("On {0}: When the data disk and log disk are on the same disk, the log disk needs to occupy more than 30% of the disk space. now it is {1}%".format(node_name, logfile_size_percentage))

                except Exception as e:
                    self.stdio.error("Failed to check datafile size on {0}: {1}".format(node_name, e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {"name": "sysbench_test_cluster_datafile_size", "info": "Check cluster info about datafile_size and log_disk_size for sysbench."}


sysbench_test_cluster_datafile_size = SysbenchTestClusterDatafileSizeTask()
