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
    def __init__(self,context, step, ob_cluster, task_variable_dict):
        try:
            self.context = context
            self.stdio = context.stdio
            self.ob_cluster = ob_cluster
            self.ob_cluster_name = ob_cluster.get("cluster_name")
            self.tenant_mode = None
            self.sys_database = None
            self.database = None
            self.ob_connector = OBConnector(ip=ob_cluster.get("db_host"),
                                        port=ob_cluster.get("db_port"),
                                        username=ob_cluster.get("tenant_sys").get("user"),
                                        password=ob_cluster.get("tenant_sys").get("password"),
                                        stdio=self.stdio,
                                        timeout=10000)
        except Exception as e:
            self.stdio.error("StepSQLHandler init fail. Please check the OBCLUSTER conf. OBCLUSTER: {0} Exception : {1} .".format(ob_cluster,e))
            raise Exception("StepSQLHandler init fail. Please check the OBCLUSTER conf. OBCLUSTER: {0} Exception : {1} .".format(ob_cluster,e))
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
            if data is None:
                self.stdio.warn("sql result is None: {0}".format(self.step["sql"]))
            self.stdio.verbose("execute_sql result:{0}".format(data))
            if len(data) == 0:
                self.stdio.warn("sql result is None: {0}".format(self.step["sql"]))
            else:
                data = data[0][0]
            if data is None:
                data = ""
            self.stdio.verbose("sql result:{0}".format(Util.convert_to_number(str(data))))
            if "result" in self.step and "set_value" in self.step["result"]:
                self.stdio.verbose("sql execute update task_variable_dict: {0} = {1}".format(self.step["result"]["set_value"], Util.convert_to_number(data)))
                self.task_variable_dict[self.step["result"]["set_value"]] = Util.convert_to_number(data)
        except Exception as e:
            self.stdio.error("StepSQLHandler execute Exception: {0}".format(e).strip())
            raise StepExecuteFailException("StepSQLHandler execute Exception: {0}".format(e).strip())

    def update_step_variable_dict(self):
        return self.task_variable_dict
