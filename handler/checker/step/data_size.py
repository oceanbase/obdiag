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
@time: 2024/4/2
@file: data_size.py
@desc:
"""
from common.types import Capacity
from handler.checker.check_exception import StepExecuteFailException
from common.ssh import SshHelper
from handler.checker.check_report import TaskReport
from common.tool import Util


class DataSizeHandler:
    def __init__(self,context, step, node, task_variable_dict):
        self.context = context
        self.stdio = context.stdio
        self.stdio.verbose("init DataSizeHandler")
        self.ssh_helper = None
        self.parameters = None
        self.step = step
        self.node = node
        self.task_variable_dict = task_variable_dict

        try:
            is_ssh = True
            self.ssh_helper = SshHelper(is_ssh, node.get("ip"),
                                        node.get("ssh_username"),
                                        node.get("ssh_password"),
                                        node.get("ssh_port"),
                                        node.get("ssh_key_file"),
                                        node)
        except Exception as e:
            self.stdio.error(
                "GetSystemParameterHandler ssh init fail  . Please check the NODES conf Exception : {0} .".format(e))
            raise Exception(
                "GetSystemParameterHandler ssh init fail . Please check the NODES conf  Exception : {0} .".format(e))

        # step report
        self.parameter = []
        self.report = TaskReport

    def execute(self):

        try:
            if "key" not in self.step:
                raise StepExecuteFailException("DataSizeHandler execute parameter's 'key' is not set")
            self.stdio.verbose("DataSizeHandler execute: {0}".format(self.step["key"]))
            s = self.step["key"]
            value = self.task_variable_dict[s]
            self.task_variable_dict[s]=Capacity(value).btyes()
            self.stdio.verbose("DataSizeHandler set {0} = {1}".format(s,self.task_variable_dict[s]))
        except Exception as e:
            self.stdio.error("DataSizeHandler execute Exception: {0}".format(e).strip())
            raise StepExecuteFailException("DataSizeHandler execute Exception: {0}".format(e).strip())

    def get_report(self):
        return self.report

    def update_step_variable_dict(self):
        return self.task_variable_dict
