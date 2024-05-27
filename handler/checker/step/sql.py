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
from common.ob_connector import OBConnector
from common.tool import StringUtils
from common.tool import Util


class StepSQLHandler:
    def __init__(self, context, step, task_variable_dict):
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
            self.stdio.error("StepSQLHandler init fail. Please check the OBCLUSTER conf. Exception : {0} .".format(e))
            raise Exception("StepSQLHandler init fail. Please check the OBCLUSTER conf. Exception : {0} .".format(e))
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
            sql = StringUtils.build_str_on_expr_by_dict(self.step["sql"], self.task_variable_dict)
            self.stdio.verbose("StepSQLHandler execute: {0}".format(sql))
            data = self.ob_connector.execute_sql(sql)
            self.stdio.verbose("execute_sql result:{0}".format(data))
            if data is None or len(data) == 0:
                data = ""
            else:
                data = data[0][0]
            if data is None:
                data = ""
            self.stdio.verbose("sql result:{0}".format(Util.convert_to_number(str(data))))
            if "result" in self.step and "set_value" in self.step["result"]:
                self.stdio.verbose("sql execute update task_variable_dict: {0} = {1}".format(self.step["result"]["set_value"], Util.convert_to_number(data)))
                self.task_variable_dict[self.step["result"]["set_value"]] = Util.convert_to_number(data)
        except Exception as e:
            self.stdio.error("StepSQLHandler execute Exception: {0}".format(e))
            raise StepExecuteFailException("StepSQLHandler execute Exception: {0}".format(e))
        finally:
            self.ob_connector_pool.release_connection(self.ob_connector)

    def update_step_variable_dict(self):
        return self.task_variable_dict
