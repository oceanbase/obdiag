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
@time: 2025/04/23
@file: dmesg_log.py
@desc:
"""
import re
from src.handler.checker.check_task import TaskBase


class DmesgLog(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # check dmesg is exist
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if not super().check_command_exist(ssh_client, "dmesg"):
                    self.report.add_warning("node:{0}. dmesg command does not exist.".format(ssh_client.get_name()))
                    continue
                # check dmesg log
                # download dmesg log
                dmesg_log_file_name = "dmesg.log." + ssh_client.get_name()
                ssh_client.exec_cmd("dmesg > {}".format(dmesg_log_file_name)).strip()
                ssh_client.download("{0}".format(dmesg_log_file_name), "./{}".format(dmesg_log_file_name))
                ssh_client.exec_cmd("rm -rf {0}".format(dmesg_log_file_name))
                with open(dmesg_log_file_name, "r", encoding="utf-8", errors="ignore") as f:
                    dmesg_log = f.read()
                    if not dmesg_log:
                        self.report.add_warning("node:{0}. dmesg log is empty.".format(ssh_client.get_name()))
                        continue
                    # check "Hardware Error" is existed
                    if re.search(r"Hardware Error", dmesg_log):
                        dmesg_log_lines = dmesg_log.splitlines("\n")
                        for line in dmesg_log_lines:
                            if "Hardware Error" in line:
                                self.report.add_warning("node:{0}. dmesg log has Hardware Error. log:{1}".format(ssh_client.get_name(), line))
                                break

        except Exception as e:
            return self.report.add_fail(f"Execute error: {e}")

    def get_task_info(self):
        return {"name": "dmesg_log", "info": "Confirm whether there is \"Hardware Error\" in dmesg. issue #885 "}


dmesg_log = DmesgLog()
