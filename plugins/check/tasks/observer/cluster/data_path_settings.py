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

from src.handler.checker.check_task import TaskBase
from src.handler.checker.check_exception import StepResultFailException


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
                # 检查数据目录是否存在 sstable 文件
                data_dir = node.get("data_dir")
                cmd = f'find {data_dir}/ -name "sstable"'
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                data_dir_path = ssher.exec_cmd(cmd).strip()
                if not data_dir_path:
                    raise StepResultFailException(f"ip:{ip}, data_dir_path is null. Please check your nodes.data_dir need absolute Path")

                # 获取数据目录所在磁盘
                # cmd = f"df -h {data_dir_path} | grep '/' | awk '{{print $6}}'"
                cmd = f"device=$(df -P {data_dir_path} | awk 'NR==2{{print $1}}') && while [[ -n $device ]]; do parent=$(lsblk -no PKNAME $device 2>/dev/null); if [[ -n $parent ]]; then device=$parent; else echo $device; break; fi; done"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                data_dir_disk = ssher.exec_cmd(cmd).strip()

                # 检查日志目录是否存在 clog 文件
                redo_dir = node.get("redo_dir")
                cmd = f'find {redo_dir}/ -name "clog"'
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                log_dir_path = ssher.exec_cmd(cmd).strip()
                if not log_dir_path:
                    raise StepResultFailException(f"ip:{ip}, log_dir_path is null. Please check your nodes.redo_dir need absolute Path")

                # 获取日志目录所在磁盘
                # cmd = f"df -h {log_dir_path} | grep '/' | awk '{{print $6}}'"
                cmd = f"device=$(df -P {log_dir_path} | awk 'NR==2{{print $1}}') && while [[ -n $device ]]; do parent=$(lsblk -no PKNAME $device 2>/dev/null); if [[ -n $parent ]]; then device=$parent; else echo $device; break; fi; done"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                log_dir_disk = ssher.exec_cmd(cmd).strip()

                # 检查数据目录和日志目录是否在不同磁盘
                if data_dir_disk == log_dir_disk:
                    raise StepResultFailException(f"ip:{ip}, data_dir and log_dir_disk are on the same disk.")

                # 检查数据目录的文件系统类型 (如果磁盘大小超过16TB，必须是xfs)
                cmd = f"df -T {data_dir_path} | grep '/' | awk '{{if ($3 > 17179869184 && $2 != \"xfs\") print \"1\"; else print \"0\"}}'"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                file_system_check = ssher.exec_cmd(cmd).strip()
                if file_system_check == "1":
                    raise StepResultFailException(f"ip:{ip}, the data_dir_path of disk size over 16TB, the type must be xfs")

                # 检查日志目录的文件系统类型必须是xfs或ext4
                cmd = f"df -Th {log_dir_path} | grep '/' | awk '{{print $2}}'"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                file_system = ssher.exec_cmd(cmd).strip()
                if file_system not in ["xfs", "ext4"]:
                    raise StepResultFailException(f"ip:{ip}, log_dir_path: {log_dir_path} file_system is not xfs or ext4.")

                # 检查日志目录的文件系统类型 (如果磁盘大小超过16TB，必须是xfs)
                cmd = f"df -T {log_dir_path} | grep '/' | awk '{{if ($3 > 17179869184 && $2 != \"xfs\") print \"1\"; else print \"0\"}}'"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                file_system_check = ssher.exec_cmd(cmd).strip()
                if file_system_check == "1":
                    raise StepResultFailException(f"ip:{ip}, the log_dir_path of disk size over 16TB, the type must be xfs")

                # 检查数据目录的文件系统类型必须是xfs或ext4
                cmd = f"df -Th {data_dir_path} | grep '/' | awk '{{print $2}}'"
                self.stdio.verbose(f"Executing on {ip}: {cmd}")
                file_system = ssher.exec_cmd(cmd).strip()
                if file_system not in ["xfs", "ext4"]:
                    raise StepResultFailException(f"ip:{ip}, data_dir_path: {data_dir_path} file_system is not xfs or ext4.")

                # 所有检查通过
                self.report.add_success_result(
                    f"ip:{ip}",
                    "data_path_settings",
                    "Check data path settings passed.",
                    {"data_dir": data_dir, "log_dir": redo_dir}
                )

            except StepResultFailException as e:
                self.stdio.warn(str(e))
                self.report.add_fail(f"ip:{ip}, data_path_settings, {str(e)}")
            except Exception as e:
                self.stdio.error(f"Check data path settings failed on {ip}: {str(e)}")
                self.report.add_critical(f"ip:{ip}, data_path_settings, Exception: {str(e)}")

    def get_task_info(self):
        return {
            "name": "data_path_settings",
            "info": "Check whether data_dir and log_dir_disk are on the same disk",
        }
    

data_path_settings = DataPathSettings()
