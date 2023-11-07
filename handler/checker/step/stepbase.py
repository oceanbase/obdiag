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
@file: step_base.py
@desc:
"""

from handler.checker.check_exception import StepResultFailException, StepExecuteFailException, \
    ResultFalseException, ResultFailException, StepResultFalseException
from handler.checker.step.get_system_parameter import GetSystemParameterHandler
from handler.checker.result.result import CheckResult
from handler.checker.step.ssh import SshHandler
from handler.checker.step.sql import StepSQLHandler
from common.logger import logger


class StepBase(object):
    def __init__(self, step, node, cluster, task_variable_dict):
        self.step = step
        self.node = node
        self.cluster = cluster
        self.task_variable_dict = {}
        self.task_variable_dict = task_variable_dict

    def execute(self, report):
        # execute and result
        try:
            self.task_variable_dict["remote_ip"] = self.node["ip"]
            self.task_variable_dict["remote_home_path"] = self.node["home_path"]

            if "type" not in self.step:
                raise StepExecuteFailException("Missing field :type")
            if self.step["type"] == "get_system_parameter":
                handler = GetSystemParameterHandler(self.step, self.node, self.task_variable_dict)
                # self.task_variable_dict = handler.update_step_variable_dict()
            elif self.step["type"] == "ssh":
                handler = SshHandler(self.step, self.node, self.task_variable_dict)
            elif self.step["type"] == "sql":
                handler = StepSQLHandler(self.step, self.cluster, self.task_variable_dict)
            else:
                raise StepExecuteFailException("the type not support: {0}" .format(self.step["type"]))
            logger.info("task execute and result")
            handler.execute()
        except Exception as e:
            logger.error("StepBase handler.execute fail".format(e))
            report.add(e.msg, "fail")
            raise StepExecuteFailException("StepBase handler.execute fail".format(e))

        try:
            self.task_variable_dict = handler.update_step_variable_dict()
            logger.debug("self.task_variable_dict: {0}".format(self.task_variable_dict))
            if "result" in self.step:
                logger.info("result execute ")
                result = CheckResult(self.step["result"], self.task_variable_dict)
                result.execute()
                if "report_type" in self.step["result"] and self.step["result"]["report_type"] == "execution":
                    logger.info("report_type stop this step")
                    return

        except ResultFalseException as resultException:
            # The result false. Determine whether to stop the subsequent process based on the result. type tag If
            # the result fails, determine whether to stop subsequent processes based on the result.type flag.
            # When result.type is execution, if this step is executed successfully, subsequent steps will not be
            # executed.

            logger.warning("step_base ResultFalseException:{0}".format(resultException.msg))
            level = "critical"
            logger.debug("step_base ResultFalseException self.step.result:{0}".format(self.step["result"]))
            if "result" in self.step:
                if "report_type" in self.step["result"]:
                    logger.info("report_type use is  {0}".format(self.step["result"]["report_type"]))
                    level = self.step["result"]["report_type"]

            if level == "execution":
                level = "warning"

            report.add(resultException.msg, level)
            if level == "critical":
                raise StepResultFailException(resultException.msg)
            raise StepResultFalseException(resultException.msg)

        except ResultFailException as resultFailException:
            # 验证失败，属于fail类型，一般是verify阶段出现异常,需要马上修正
            logger.error("step_base ResultFailException:{0}".format(resultFailException.msg))
            report.add(resultFailException.msg, "fail")
            raise StepResultFailException(resultFailException)

        except Exception as e:
            logger.error("step_base Exception {0}".format(e))
            raise StepExecuteFailException(e)

    def update_task_variable_dict(self):
        return self.task_variable_dict
