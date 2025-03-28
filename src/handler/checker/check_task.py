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
import threading

from src.handler.checker.check_exception import StepResultFailException, StepExecuteFailException, StepResultFalseException, TaskException
from src.handler.checker.step.stepbase import StepBase
from src.common.tool import StringUtils
from src.common.scene import filter_by_version


class TaskBase:
    def __init__(self):
        self.work_path = None
        self.record = None
        self.gather_log = None
        self.stdio = None
        self.input_parameters = None
        self.ob_cluster = None
        self.ob_connector = None
        self.store_dir = None
        self.obproxy_version = None
        self.observer_version = None
        self.report = None
        self.obproxy_nodes = None
        self.observer_nodes = None
        self.oms_nodes = None
        self.context = None
        self.name = type(self).__name__
        self.Result = None

    def init(self, context):
        self.context = context
        self.stdio = context.stdio
        self.observer_nodes = context.get_variable("observer_nodes")
        self.obproxy_nodes = context.get_variable("obproxy_nodes")
        self.oms_nodes = context.get_variable("oms_nodes")
        self.report = context.get_variable("report")
        self.obproxy_version = context.get_variable("obproxy_version", default="")
        self.observer_version = context.get_variable("observer_version", default="")
        self.ob_connector = context.get_variable("ob_connector", default=None)
        self.store_dir = context.get_variable("store_dir")
        self.ob_cluster = context.get_variable("ob_cluster")
        self.input_parameters = context.get_variable("input_parameters") or {}
        self.gather_log = context.get_variable("gather_log")
        self.work_path = self.store_dir


class Task(TaskBase):
    def __init__(self, context, task, nodes, cluster, report, task_variable_dict=None):
        self.context = context
        self.stdio = context.stdio
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.task = task
        self.cluster = cluster
        self.nodes = nodes
        self.report = report

    def execute(self):
        self.stdio.verbose("task_base execute")
        if len(self.task) > 0 and self.task[0].get("task_type") and self.task[0]["task_type"] == "py":
            module = self.task[0]["module"]
            module.init(self.context, self.report, self.nodes)
            module.execute()
            return
        steps_nu = filter_by_version(self.task, self.cluster, self.stdio)
        if steps_nu < 0:
            self.stdio.verbose("Unadapted by version. SKIP")
            self.report.add("Unadapted by version. SKIP", "warning")
            return "Unadapted by version.SKIP"
        self.stdio.verbose("filter_by_version is return {0}".format(steps_nu))
        if len(self.nodes) == 0:
            raise Exception("node is not exist")
        work_threads = []
        for node in self.nodes:
            t = threading.Thread(target=self.execute_one_node, args=(steps_nu, node))
            work_threads.append(t)
            t.start()
        for t in work_threads:
            t.join()

        self.stdio.verbose("task execute end")

    def execute_one_node(self, steps_nu, node):
        try:
            self.stdio.verbose("run task in node: {0}".format(StringUtils.node_cut_passwd_for_log(node)))
            steps = self.task[steps_nu]
            nu = 1
            task_variable_dict = {}
            for step in steps["steps"]:
                try:
                    self.stdio.verbose("step nu: {0}".format(nu))
                    if len(self.cluster) == 0:
                        raise Exception("cluster is not exist")
                    step_run = StepBase(self.context, step, node, self.cluster, task_variable_dict)
                    self.stdio.verbose("step nu: {0} initted, to execute".format(nu))
                    step_run.execute(self.report)
                    task_variable_dict = step_run.update_task_variable_dict()
                    if "report_type" in step["result"] and step["result"]["report_type"] == "execution":
                        self.stdio.verbose("report_type stop this step")
                        return
                except StepExecuteFailException as e:
                    self.stdio.error("Task execute CheckStepFailException: {0} . Do Next Task".format(e))
                    return
                except StepResultFalseException as e:
                    self.stdio.warn("Task execute StepResultFalseException: {0} .".format(e))
                    continue
                except StepResultFailException as e:
                    self.stdio.warn("Task execute StepResultFailException: {0}".format(e))
                    return
                except Exception as e:
                    self.stdio.error("Task execute Exception: {0}".format(e))
                    raise TaskException("Task execute Exception:  {0}".format(e))

                self.stdio.verbose("step nu: {0} execute end ".format(nu))
                nu = nu + 1
        except Exception as e:
            self.stdio.error("Task execute Exception: {0}".format(e))
            raise e
