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
@time: 2023/9/26
@file: ssh.py
@desc:
"""

from handler.checker.check_exception import StepExecuteFailException
from handler.checker.check_report import TaskReport
from utils.shell_utils import SshHelper
from common.logger import logger
from utils.utils import convert_to_number, build_str_on_expr_by_dict


class SshHandler:
    def __init__(self, step, node, task_variable_dict):
        self.ssh_report_value = None
        self.parameters = None
        self.step = step
        self.node = node
        try:
            self.ssh_helper = SshHelper(True, self.node["ip"], self.node["user"], self.node["password"],
                                        self.node["port"],
                                        self.node["private_key"])
        except Exception as e:
            logger.error("SshHandler init fail Exception : {0} .".format(e))
            raise Exception("SshHandler init fail Exception : {0} .".format(e))
        self.task_variable_dict = task_variable_dict
        self.parameter = []
        self.report = TaskReport

    def execute(self):
        try:
            if "ssh" not in self.step:
                raise StepExecuteFailException("SshHandler execute ssh is not set")
            ssh_cmd = build_str_on_expr_by_dict(self.step["ssh"], self.task_variable_dict)
            logger.info("step SshHandler execute :{0} ".format(ssh_cmd))
            ssh_report_value = self.ssh_helper.ssh_exec_cmd(ssh_cmd)
            if ssh_report_value == None:
                ssh_report_value = ""
            logger.info("ssh result:{0}".format(convert_to_number(ssh_report_value[:-1])))
            if "result" in self.step and "set_value" in self.step["result"]:
                logger.debug("ssh result set {0}".format(self.step["result"]["set_value"],
                                                         convert_to_number(ssh_report_value[:-1])))
                self.task_variable_dict[self.step["result"]["set_value"]] = convert_to_number(ssh_report_value[:-1])
        except Exception as e:
            logger.error("ssh execute Exception:{0}".format(e.msg))
            raise StepExecuteFailException(e)
        logger.info("step SshHandler ssh_report_value:{0}".format(ssh_report_value[:-1]))

    def update_step_variable_dict(self):
        return self.task_variable_dict
