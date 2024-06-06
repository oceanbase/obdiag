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
@time: 2024/05/28
@file: sleep.py
@desc:
"""
import time

from handler.checker.check_exception import StepExecuteFailException
from common.ob_connector import OBConnector
from common.tool import StringUtils
from common.tool import Util


class StepSleepHandler:
    def __init__(self, context, step, task_variable_dict):
        self.sleep_time = None
        try:
            self.context = context
            self.stdio = context.stdio
            self.ob_cluster = self.context.cluster_config
            self.ob_cluster_name = self.ob_cluster.get("cluster_name")
            self.tenant_mode = None
            self.sys_database = None
            self.database = None
            self.ob_connector_pool = self.context.get_variable('check_obConnector_pool', None)
            if self.ob_connector_pool is not None:
                self.ob_connector = self.ob_connector_pool.get_connection()
            if self.ob_connector is None:
                raise Exception("self.ob_connector is None.")
        except Exception as e:
            self.stdio.error("StepSleepHandler init fail. Please check the OBCLUSTER conf. Exception : {0} .".format(e))
            raise Exception("StepSleepHandler init fail. Please check the OBCLUSTER conf. Exception : {0} .".format(e))
        self.task_variable_dict = task_variable_dict
        self.enable_dump_db = False
        self.trace_id = None
        self.STAT_NAME = {}
        self.report_file_path = ""
        self.enable_fast_dump = False
        self.ob_major_version = None
        self.step = step

    def execute(self):
        try:
            self.sleep_time = self.step.get("sleep", None)
            if self.sleep_time == None:
                raise StepExecuteFailException("StepSleepHandler execute sleep is not set")
            if type(self.sleep_time) != int:
                raise StepExecuteFailException("StepSleepHandler execute sleep type must be int")
            time.sleep(int(self.sleep_time))
            self.stdio.verbose("StepSleepHandler execute: {0}".format(self.sleep_time))
        except Exception as e:
            self.stdio.error("StepSleepHandler execute Exception: {0}".format(e))
            raise StepExecuteFailException("StepSleepHandler execute Exception: {0}".format(e))
        finally:
            self.ob_connector_pool.release_connection(self.ob_connector)

    def update_step_variable_dict(self):
        return self.task_variable_dict
