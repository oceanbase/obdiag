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
from common.tool import StringUtils
from common.tool import Util


class SshHandler:
    def __init__(self, context, step, node, task_variable_dict):
        self.context = context
        self.stdio = context.stdio
        self.ssh_report_value = None
        self.parameters = None
        self.step = step
        self.node = node
        try:
            self.ssh_helper = self.node["ssher"]
            if self.ssh_helper is None:
                raise Exception("self.ssh_helper is None.")
        except Exception as e:
            self.stdio.error("SshHandler init fail. Please check the NODES conf. node: {0}. Exception : {1} .".format(node, e))
            raise Exception("SshHandler init fail. Please check the NODES conf node: {0}  Exception : {1} .".format(node, e))
        self.task_variable_dict = task_variable_dict
        self.parameter = []
        self.report = TaskReport

    def execute(self):
        try:
            if "ssh" not in self.step:
                raise StepExecuteFailException("SshHandler execute ssh is not set")
            ssh_cmd = StringUtils.build_str_on_expr_by_dict(self.step["ssh"], self.task_variable_dict)
            self.stdio.verbose("step SshHandler execute :{0} ".format(ssh_cmd))
            ssh_report_value = self.ssh_helper.ssh_exec_cmd(ssh_cmd)
            if ssh_report_value is None:
                ssh_report_value = ""
            if len(ssh_report_value) > 0:
                ssh_report_value = ssh_report_value.strip()
            self.stdio.verbose("ssh result:{0}".format(Util.convert_to_number(ssh_report_value)))
            if "result" in self.step and "set_value" in self.step["result"]:
                self.stdio.verbose("ssh result set {0}".format(self.step["result"]["set_value"], Util.convert_to_number(ssh_report_value)))
                self.task_variable_dict[self.step["result"]["set_value"]] = Util.convert_to_number(ssh_report_value)
        except Exception as e:
            self.stdio.error("ssh execute Exception:{0}".format(e).strip())
            raise StepExecuteFailException("ssh execute Exception:{0}".format(e).strip())
        finally:
            self.ssh_helper.ssh_close()
        self.stdio.verbose("step SshHandler ssh_report_value:{0}".format(ssh_report_value))

    def update_step_variable_dict(self):
        return self.task_variable_dict
