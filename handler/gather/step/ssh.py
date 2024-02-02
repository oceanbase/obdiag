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
@time: 2024/01/04
@file: ssh.py
@desc:
"""
import os
from utils.shell_utils import SshHelper
from common.logger import logger
from utils.utils import build_str_on_expr_by_dict_2


class SshHandler:
    def __init__(self, step, node, report_path, task_variable_dict):
        self.ssh_report_value = None
        self.parameters = None
        self.step = step
        self.node = node
        self.report_path = report_path
        try:
            is_ssh = True
            self.ssh_helper = SshHelper(is_ssh, node.get("ip"), node.get("user"), node.get("password"), node.get("port"), node.get("private_key"), node)
        except Exception as e:
            logger.error("SshHandler init fail. Please check the NODES conf. node: {0}. Exception : {1} .".format(node, e))
        self.task_variable_dict = task_variable_dict
        self.parameter = []
        self.report_file_path = os.path.join(self.report_path, "shell_result.txt")

    def execute(self):
        try:
            if "ssh" not in self.step:
                logger.error("SshHandler execute ssh is not set")
                return
            ssh_cmd = build_str_on_expr_by_dict_2(self.step["ssh"], self.task_variable_dict)
            logger.info("step SshHandler execute :{0} ".format(ssh_cmd))
            ssh_report_value = self.ssh_helper.ssh_exec_cmd(ssh_cmd)
            if ssh_report_value is None:
                ssh_report_value = ""
            if len(ssh_report_value) > 0:
                ssh_report_value = ssh_report_value.strip()
                self.report(ssh_cmd, ssh_report_value)
        except Exception as e:
            logger.error("ssh execute Exception:{0}".format(e).strip())
        finally:
            self.ssh_helper.ssh_close()
        logger.debug("gather step SshHandler ssh_report_value:{0}".format(ssh_report_value))

    def update_step_variable_dict(self):
        return self.task_variable_dict

    def report(self, command, data):
        try:
            with open(self.report_file_path, 'a', encoding='utf-8') as f:
                f.write('\n\n' + 'shell > ' + command + '\n')
                f.write(data + '\n')
        except Exception as e:
            logger.error("report sql result to file: {0} failed, error: ".format(self.report_file_path))
