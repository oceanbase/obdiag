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
import docker


class StepBase(object):
    def __init__(self, step, node, cluster, task_variable_dict):
        self.step = step
        self.node = node
        self.cluster = cluster
        self.task_variable_dict = {}
        self.task_variable_dict = task_variable_dict

    def execute(self, report):
        no_cluster_name_msg = "(Please set ob_cluster_name or obproxy_cluster_name)"
        # execute and result
        try:
            # init task_variable_dict
            ## set remote_ip
            if "ip" in self.node:
                self.task_variable_dict["remote_ip"] = self.node["ip"]
            elif "ssh_type" in self.node and self.node["ssh_type"] == "docker":
                logger.debug("execute ssh_type is docker")
                self.task_variable_dict["remote_ip"] = \
                docker.from_env().containers.get(self.node["container_name"]).attrs['NetworkSettings']['Networks'][
                    'bridge']["IPAddress"]
            self.task_variable_dict["remote_home_path"] = self.node["home_path"]

            if "type" not in self.step:
                raise StepExecuteFailException("Missing field :type")
            if self.step["type"] == "get_system_parameter":
                handler = GetSystemParameterHandler(self.step, self.node, self.task_variable_dict)
            elif self.step["type"] == "ssh":
                handler = SshHandler(self.step, self.node, self.task_variable_dict)
            elif self.step["type"] == "sql":
                handler = StepSQLHandler(self.step, self.cluster, self.task_variable_dict)
            else:
                raise StepExecuteFailException("the type not support: {0}".format(self.step["type"]))
            logger.debug("task execute and result")
            handler.execute()
        except Exception as e:
            logger.error("StepBase handler.execute fail {0}".format(e))
            if self.step["type"] == "sql":
                report.add("[cluster:{0}] {1}".format(self.cluster.get("ob_cluster_name") or self.cluster.get(
                    "obproxy_cluster_name") or no_cluster_name_msg, e), "fail")
            else:
                report.add("[{0}:{1}] {2}".format(self.node.get("ssh_type") or "",
                                                  self.node.get("container_name") or self.task_variable_dict.get(
                                                      "remote_ip") or "", e), "fail")
            raise StepExecuteFailException("StepBase handler.execute fail {0}".format(e))

        try:
            self.task_variable_dict = handler.update_step_variable_dict()
            logger.debug("self.task_variable_dict: {0}".format(self.task_variable_dict))
            if self.step["type"] == "get_system_parameter" and "result" in self.step and "set_value" in self.step[
                "result"] and self.task_variable_dict[self.step["result"]["set_value"]] == "":
                return

            if "result" in self.step:
                logger.debug("result execute ")
                result = CheckResult(self.step["result"], self.task_variable_dict)
                result.execute()
                if "report_type" in self.step["result"] and self.step["result"]["report_type"] == "execution":
                    logger.debug("report_type stop this step")
                    return

        except ResultFalseException as resultException:
            # The result false. Determine whether to stop the subsequent process based on the result. type tag If
            # the result fails, determine whether to stop subsequent processes based on the result.type flag.
            # When result.type is execution, if this step is executed successfully, subsequent steps will not be
            # executed.

            logger.warning("step_base ResultFalseException:{0}".format(resultException))
            level = "critical"
            logger.debug("step_base ResultFalseException self.step.result:{0}".format(self.step["result"]))
            if "result" in self.step:
                if "report_type" in self.step["result"]:
                    logger.info("report_type use is  {0}".format(self.step["result"]["report_type"]))
                    level = self.step["result"]["report_type"]

            if level == "execution":
                level = "warning"
            if self.step["type"] == "sql":
                report.add("[cluster:{0}] {1}".format(self.cluster.get("ob_cluster_name") or self.cluster.get(
                    "obproxy_cluster_name") or no_cluster_name_msg, resultException), level)
            else:
                report.add("[{0}:{1}] {2}".format(self.node.get("ssh_type") or "",
                                                  self.node.get("container_name") or self.task_variable_dict.get(
                                                      "remote_ip") or "", resultException), level)
            if level == "critical":
                raise StepResultFailException(resultException)
            raise StepResultFalseException(resultException)

        except ResultFailException as resultFailException:
            # 验证失败，属于fail类型，一般是verify阶段出现异常,需要马上修正
            logger.error("step_base ResultFailException:{0}".format(resultFailException))
            if self.step["type"] == "sql":
                report.add("[cluster:{0}] {1}".format(self.cluster.get("ob_cluster_name") or self.cluster.get(
                    "obproxy_cluster_name") or no_cluster_name_msg, resultFailException), "fail")
            else:
                report.add("[{0}:{1}] {2}".format(self.node.get("ssh_type") or "",
                                                  self.node.get("container_name") or self.task_variable_dict.get(
                                                      "remote_ip") or "", resultFailException), "fail")
            raise StepResultFailException(resultFailException)

        except Exception as e:
            logger.error("step_base Exception {0}".format(e))
            raise StepExecuteFailException(e)

    def update_task_variable_dict(self):
        return self.task_variable_dict
