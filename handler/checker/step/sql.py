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
@time: 2023/10/12
@file: sql.py
@desc:
"""

from handler.checker.check_exception import StepExecuteFailException
from common.logger import logger
from common.ob_connector import OBConnector
from utils.utils import build_str_on_expr_by_dict, convert_to_number


class StepSQLHandler:
    def __init__(self, step, ob_cluster, task_variable_dict):
        self.ob_cluster = ob_cluster
        self.ob_cluster_name = ob_cluster["cluster_name"]
        self.tenant_mode = None
        self.sys_database = None
        self.database = None
        try:
            self.ob_connector = OBConnector(ip=ob_cluster["host"],
                                        port=ob_cluster["port"],
                                        username=ob_cluster["user"],
                                        password=ob_cluster["password"],
                                        timeout=100)
        except Exception as e:
            logger.error("StepSQLHandler init fail Exception : {0} .".format(e))
            raise Exception("StepSQLHandler init fail Exception : {0} .".format(e))
        self.task_variable_dict = task_variable_dict
        self.enable_dump_db = False
        self.trace_id = None
        self.STAT_NAME = {}
        self.report_file_path = ""
        self.enable_fast_dump = False
        self.ob_major_version = None
        self.sql_audit_name = "gv$sql_audit"
        self.plan_explain_name = "gv$plan_cache_plan_explain"
        self.step = step

    def execute(self):
        try:
            if "sql" not in self.step:
                raise StepExecuteFailException("StepSQLHandler execute sql is not set")
            sql = build_str_on_expr_by_dict(self.step["sql"], self.task_variable_dict)
            logger.info("StepSQLHandler execute: {0}".format(sql))
            data = self.ob_connector.execute_sql(sql)
            if data is None:
                logger.warning("sql result is None: {0}".format(self.step["sql"]))
            logger.info("execute_sql result:{0}".format(data))
            if len(data) == 0:
                logger.warning("sql result is None: {0}".format(self.step["sql"]))
            else:
                data = data[0][0]
            if data is None:
                data = ""
            logger.info("sql result:{0}".format(data))
            if "result" in self.step and "set_value" in self.step["result"]:
                logger.info("sql execute update task_variable_dict: {0} = {1}".format(self.step["result"]["set_value"], data))
                self.task_variable_dict[self.step["result"]["set_value"]] = data
        except Exception as e:
            logger.error("StepSQLHandler execute Exception: {0}".format(e))
            raise StepExecuteFailException(e)

    def update_step_variable_dict(self):
        return self.task_variable_dict
