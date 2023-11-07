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
@file: get_system_parameter.py
@desc:
"""


from handler.checker.check_exception import StepExecuteFailException
from utils.shell_utils import SshHelper
from handler.checker.check_report import TaskReport
from common.logger import logger
from utils.utils import convert_to_number


class GetSystemParameterHandler:
    def __init__(self, step, node, task_variable_dict):
        # super(GetSystemParameterHandler, self).__init__(nodes)
        logger.info("init GetSystemParameterHandler")
        self.ssh_helper = None
        self.parameters = None
        self.step = step
        self.node = node
        self.task_variable_dict = task_variable_dict

        try:
            self.ssh_helper = SshHelper(True, self.node["ip"], self.node["user"], self.node["password"],
                                        self.node["port"],
                                        self.node["private_key"])
        except Exception as e:
            logger.error("GetSystemParameterHandler ssh init fail Exception : {0} .".format(e))
            raise Exception("GetSystemParameterHandler ssh init fail Exception : {0} .".format(e))

        # step report
        self.parameter = []
        self.report = TaskReport

    def get_parameter(self, parameter_name):
        parameter_value = self.ssh_helper.ssh_exec_cmd("sysctl -n " + parameter_name)
        self.ssh_helper.ssh_close()
        return parameter_value

    def execute(self):

        try:
            if "parameter" not in self.step:
                raise StepExecuteFailException("GetSystemParameterHandler execute parameter is not set")
            logger.info("GetSystemParameterHandler execute: {0}".format(self.step["parameter"]))
            parameter_value = self.get_parameter(self.step["parameter"])
            logger.info("GetSystemParameterHandler get value : {0}".format(parameter_value))
            if "result" in self.step and "set_value" in self.step["result"]:
                self.task_variable_dict[self.step["result"]["set_value"]] = convert_to_number(parameter_value[:-1])
        except Exception as e:
            logger.error("get_parameter execute: {0}".format(e))
            raise StepExecuteFailException("get_parameter execute: {0}".format(e))

    def get_report(self):
        return self.report

    def update_step_variable_dict(self):
        return self.task_variable_dict

