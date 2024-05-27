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
from common.ssh import SshHelper
from handler.checker.check_report import TaskReport
from common.tool import Util


class GetSystemParameterHandler:
    def __init__(self, context, step, node, task_variable_dict):
        self.context = context
        self.stdio = context.stdio
        self.stdio.verbose("init GetSystemParameterHandler")
        self.ssh_helper = None
        self.parameters = None
        self.step = step
        self.node = node
        self.task_variable_dict = task_variable_dict

        try:
            self.ssh_helper = self.node["ssher"]
            if self.ssh_helper is None:
                raise Exception("self.ssh_helper is None.")
        except Exception as e:
            self.stdio.error("GetSystemParameterHandler ssh init fail  . Please check the NODES conf Exception : {0} .".format(e))
            raise Exception("GetSystemParameterHandler ssh init fail . Please check the NODES conf  Exception : {0} .".format(e))

        # step report
        self.parameter = []
        self.report = TaskReport

    def get_parameter(self, parameter_name):
        try:
            parameter_name = parameter_name.replace(".", "/")
            parameter_value = self.ssh_helper.ssh_exec_cmd("cat /proc/sys/" + parameter_name).strip()
            self.ssh_helper.ssh_close()
        except Exception as e:
            self.stdio.warn("get {0} fail:{1} .please check, the parameter_value will be set -1".format(parameter_name, e))
            parameter_value = str("-1")
        return parameter_value

    def execute(self):

        try:
            if "parameter" not in self.step:
                raise StepExecuteFailException("GetSystemParameterHandler execute parameter is not set")
            self.stdio.verbose("GetSystemParameterHandler execute: {0}".format(self.step["parameter"]))
            s = self.step["parameter"]
            if '.' in s:
                last_substring = s.rsplit('.', 1)
                s = last_substring[len(last_substring) - 1]
            else:
                s = self.step["parameter"]
            # SystemParameter exist?
            if self.ssh_helper.ssh_exec_cmd('find /proc/sys/ -name "{0}"'.format(s)) == "":
                self.stdio.warn("{0} is not exist".format(self.step["parameter"]))
                if "result" in self.step and "set_value" in self.step["result"]:
                    self.task_variable_dict[self.step["result"]["set_value"]] = ""
                return
            parameter_value = self.get_parameter(self.step["parameter"])

            if "result" in self.step and "set_value" in self.step["result"]:
                if len(parameter_value) > 0:
                    parameter_value = parameter_value.strip()
                self.stdio.verbose("GetSystemParameterHandler get value : {0}".format(parameter_value))
                self.task_variable_dict[self.step["result"]["set_value"]] = Util.convert_to_number(parameter_value)
        except Exception as e:
            self.stdio.error("get_parameter execute: {0}".format(e).strip())
            raise StepExecuteFailException("get_parameter execute: {0}".format(e).strip())

    def get_report(self):
        return self.report

    def update_step_variable_dict(self):
        return self.task_variable_dict
