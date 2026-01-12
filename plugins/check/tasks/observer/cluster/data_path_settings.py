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
@time: 2025/08/18
@file: data_path_settings.py
@desc:
"""

from src.handler.check.check_task import TaskBase


class DataPathSettings(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        for node in self.observer_nodes:
            ip = node.get("ip")
            ssher = node.get("ssher")
            if not ssher:
                self.stdio.warn(f"Node {ip} has no ssher, skip check.")
                continue
            try:
                # Check if sstable files exist in the data directory
                data_dir = node.get("data_dir")
                cmd = f'find {data_dir}/ -name "sstable"'
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                data_dir_path = ssher.exec_cmd(cmd).strip()
                if not data_dir_path:
                    self.report.add_warning(f"ip:{ip}, data_dir_path is null. Please check your nodes.data_dir need absolute Path")
                    return

                # Get the disk where the data directory is located
                cmd = f"device=$(df -P {data_dir_path} | awk 'NR==2{{print $1}}') && while [[ -n $device ]]; do parent=$(lsblk -no PKNAME $device 2>/dev/null); if [[ -n $parent ]]; then device=$parent; else echo $device; break; fi; done"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                data_dir_disk = ssher.exec_cmd(cmd).strip()

                # Check if clog files exist in the log directory
                redo_dir = node.get("redo_dir")
                cmd = f'find {redo_dir}/ -name "clog"'
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                log_dir_path = ssher.exec_cmd(cmd).strip()
                if not log_dir_path:
                    self.report.add_critical(f"ip:{ip}, log_dir_path is null. Please check your nodes.redo_dir need absolute Path")
                    return

                # Get the disk where the log directory is located
                cmd = f"device=$(df -P {log_dir_path} | awk 'NR==2{{print $1}}') && while [[ -n $device ]]; do parent=$(lsblk -no PKNAME $device 2>/dev/null); if [[ -n $parent ]]; then device=$parent; else echo $device; break; fi; done"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                log_dir_disk = ssher.exec_cmd(cmd).strip()

                # Check if the obtained disk device is empty
                if data_dir_disk == "" or data_dir_disk == None:
                    self.stdio.warn(f"ip:{ip}, The obtained data_ir_disk disk device is null, please manually check.")
                if log_dir_disk == "" or log_dir_disk == None:
                    self.stdio.warn(f"ip:{ip}, The obtained log_dir_disk disk device is null, please manually check.")

                # Check if data directory and log directory are on different disks
                if data_dir_disk == log_dir_disk:
                    self.report.add_critical(f"ip:{ip}, data_dir and log_dir_disk are on the same disk.")

                # Check the file system type of the data directory (must be xfs if disk size exceeds 16TB)
                cmd = f"df -T {data_dir_path} | grep '/' | awk '{{if ($3 > 17179869184 && $2 != \"xfs\") print \"1\"; else print \"0\"}}'"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                file_system_check = ssher.exec_cmd(cmd).strip()
                if file_system_check == "1":
                    self.report.add_critical(f"ip:{ip}, the data_dir_path of disk size over 16TB, the type must be xfs")

                # Check that the file system type of the log directory must be xfs or ext4
                cmd = f"df -Th {log_dir_path} | grep '/' | awk '{{print $2}}'"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                file_system = ssher.exec_cmd(cmd).strip()
                if file_system not in ["xfs", "ext4"]:
                    self.report.add_critical(f"ip:{ip}, log_dir_path: {log_dir_path} file_system is not xfs or ext4.")

                # Check the file system type of the log directory (must be xfs if disk size exceeds 16TB)
                cmd = f"df -T {log_dir_path} | grep '/' | awk '{{if ($3 > 17179869184 && $2 != \"xfs\") print \"1\"; else print \"0\"}}'"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                file_system_check = ssher.exec_cmd(cmd).strip()
                if file_system_check == "1":
                    self.report.add_critical(f"ip:{ip}, the log_dir_path of disk size over 16TB, the type must be xfs")

                # Check that the file system type of the data directory must be xfs or ext4
                cmd = f"df -Th {data_dir_path} | grep '/' | awk '{{print $2}}'"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                file_system = ssher.exec_cmd(cmd).strip()
                if file_system not in ["xfs", "ext4"]:
                    self.report.add_critical(f"ip:{ip}, data_dir_path: {data_dir_path} file_system is not xfs or ext4.")

            except Exception as e:
                self.stdio.error(f"Check data path settings failed on {ip}: {str(e)}")
                self.report.add_fail(f"ip:{ip}, data_path_settings, Exception: {str(e)}")

    def get_task_info(self):
        return {
            "name": "data_path_settings",
            "info": "Check whether data_dir and log_dir_disk are on the same disk",
        }


data_path_settings = DataPathSettings()
