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
@file: result.py
@desc:
"""
from handler.checker.check_exception import ResultFalseException, ResultFailException, VerifyFailException
from handler.checker.result.verify import VerifyResult
import re


# There are three types of validation results: pass; VerifyFailException (if an exception occurs during the
# validation process, handle it as fail); VerifyException (verification failed, report needs to be combined with
# report_type)


class CheckResult:
    def __init__(self, context, step_result_info, variable_dict):
        self.context = context
        self.stdio = context.stdio
        self.step_result_info = step_result_info
        self.variable_dict = variable_dict
        self.result = False

    def execute(self):
        verify_type = None
        self.result = False
        if "verify_type" in self.step_result_info:
            verify_type = self.step_result_info["verify_type"]
            self.stdio.verbose("verify_type is {0}".format(verify_type))

        # if verify in step.result[]
        if "verify" in self.step_result_info:
            try:
                verify = VerifyResult(self.context, self.step_result_info["verify"], self.variable_dict, self.step_result_info["set_value"], verify_type)
                result = verify.execute()
                self.stdio.verbose("verify.execute end. and result is {0}".format(result))

            except Exception as e:
                self.stdio.error("check_result execute VerifyFailException :{0}".format(e))
                raise ResultFailException(e)
            if not result:
                err_msg = self.build_msg()
                self.stdio.verbose("verify.execute end. and result is false return ResultFalseException err_msg:{0}".format(err_msg))
                raise ResultFalseException(err_msg)

    def build_msg(self):
        s = "the step is not pass"
        if 'err_msg' in self.step_result_info:
            s = self.step_result_info["err_msg"]
        d = self.variable_dict

        def replacer(match):
            key = match.group(1)
            return str(d.get(key, match.group(0)))

        return re.sub(r'#\{(\w+)\}', replacer, s)
