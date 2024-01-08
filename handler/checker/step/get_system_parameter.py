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
from utils.utils import convert_to_number, get_localhost_inner_ip


class GetSystemParameterHandler:
    def __init__(self, step, node, task_variable_dict):
        logger.debug("init GetSystemParameterHandler")
        self.ssh_helper = None
        self.parameters = None
        self.step = step
        self.node = node
        self.task_variable_dict = task_variable_dict

        try:
            is_ssh = True
            self.ssh_helper = SshHelper(is_ssh, node.get("ip"),
                                        node.get("user"),
                                        node.get("password"),
                                        node.get("port"),
                                        node.get("private_key"),
                                        node)
        except Exception as e:
            logger.error(
                "GetSystemParameterHandler ssh init fail  . Please check the NODES conf Exception : {0} .".format(e))
            raise Exception(
                "GetSystemParameterHandler ssh init fail . Please check the NODES conf  Exception : {0} .".format(e))

        # step report
        self.parameter = []
        self.report = TaskReport

    def get_parameter(self, parameter_name):
        try:
            parameter_value = self.ssh_helper.ssh_exec_cmd("sysctl -n " + parameter_name)
            self.ssh_helper.ssh_close()
        except Exception as e:
            logger.warning(
                "get {0} fail:{1} .please check，the parameter_value will be set -1".format(parameter_name, e))
            parameter_value = str("-1")
        return parameter_value

    def execute(self):

        try:
            if "parameter" not in self.step:
                raise StepExecuteFailException("GetSystemParameterHandler execute parameter is not set")
            logger.info("GetSystemParameterHandler execute: {0}".format(self.step["parameter"]))
            s = ""
            if '.'  in self.step["parameter"]:
                last_substring = s.rsplit('.', 1)[1]
                s=last_substring
            # SystemParameter exist?
            if self.ssh_helper.ssh_exec_cmd('find /proc/sys/ -name "{0}"'.format(s))=="" :
                if "result" in self.step and "set_value" in self.step["result"]:
                    self.task_variable_dict[self.step["result"]["set_value"]]=""
                return
            parameter_value = self.get_parameter(self.step["parameter"])

            if "result" in self.step and "set_value" in self.step["result"]:
                if len(parameter_value) > 0:
                    parameter_value = parameter_value.strip()
                logger.info("GetSystemParameterHandler get value : {0}".format(parameter_value))
                self.task_variable_dict[self.step["result"]["set_value"]] = convert_to_number(parameter_value)
        except Exception as e:
            logger.error("get_parameter execute: {0}".format(e).strip())
            raise StepExecuteFailException("get_parameter execute: {0}".format(e).strip())

    def get_report(self):
        return self.report

    def update_step_variable_dict(self):
        return self.task_variable_dict
