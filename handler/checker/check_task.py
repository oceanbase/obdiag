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
@file: check_task.py
@desc:
"""

from common.logger import logger
from handler.checker.check_exception import StepResultFailException, \
    StepExecuteFailException, StepResultFalseException, TaskException
from handler.checker.step.stepbase import StepBase
from utils.utils import node_cut_passwd_for_log
from common.scene import filter_by_version


class TaskBase(object):
    def __init__(self, task, nodes, cluster, report, task_variable_dict=None):
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.task = task
        self.cluster = cluster
        self.nodes = nodes
        self.report = report

    def execute(self):
        logger.info("task_base execute")
        steps_nu = filter_by_version(self.task, self.cluster)
        if steps_nu < 0:
            logger.warning("Unadapted by version. SKIP")
            self.report.add("Unadapted by version. SKIP", "warning")
            return "Unadapted by version.SKIP"
        logger.info("filter_by_version is return {0}".format(steps_nu))
        if len(self.nodes)==0:
            raise Exception("node is not exist")
        for node in self.nodes:
            logger.info("run task in node: {0}".format(node_cut_passwd_for_log(node)))
            steps = self.task[steps_nu]
            nu = 1
            for step in steps["steps"]:
                try:
                    logger.debug("step nu: {0}".format(nu))
                    if len(self.cluster)==0:
                        raise Exception("cluster is not exist")
                    step_run = StepBase(step, node, self.cluster, self.task_variable_dict)
                    logger.debug("step nu: {0} initted, to execute".format(nu))
                    step_run.execute(self.report)
                    self.task_variable_dict = step_run.update_task_variable_dict()
                    if "report_type" in step["result"] and step["result"]["report_type"] == "execution":
                        logger.info("report_type stop this step")
                        return
                except StepExecuteFailException as e:
                    logger.error("TaskBase execute CheckStepFailException: {0} . Do Next Task".format(e))
                    return
                except StepResultFalseException as e:
                    logger.warning("TaskBase execute StepResultFalseException: {0} .".format(e))
                    continue
                except StepResultFailException as e:
                    logger.error("TaskBase execute StepResultFailException: {0}".format(e))
                    return
                except Exception as e:
                    logger.error("TaskBase execute Exception: {0}".format(e))
                    raise TaskException("TaskBase execute Exception:  {0}".format(e))

                logger.debug("step nu: {0} execute end ".format(nu))
                nu = nu + 1
        logger.info("task execute end")