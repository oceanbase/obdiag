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
@file: get_system_parameter.py
@desc:
"""

from src.handler.checker.check_exception import StepExecuteFailException
from src.handler.checker.check_report import TaskReport
from src.common.tool import Util, StringUtils


class GetObproxyParameterHandler:
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
            self.stdio.error("GetObproxyParameterHandler init fail. Please check the OBCLUSTER conf. Exception : {0} .".format(e))
            raise Exception("GetObproxyParameterHandler init fail. Please check the OBCLUSTER conf. Exception : {0} .".format(e))
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
        # check the ob_connector is based on the obproxy
        try:
            self.ob_connector.execute_sql("show proxyconfig")
            self.enable_fast_dump = True
        except Exception as e:
            raise StepExecuteFailException("ob_connector is not based on the obproxy. Please check the OBCLUSTER conf, the db_host, db_port must belong to obproxy. Exception : {0} .".format(e))
        try:
            if "parameter" not in self.step:
                raise StepExecuteFailException("GetObproxyParameterHandler parameter is not set")
            parameter = StringUtils.build_str_on_expr_by_dict(self.step["parameter"], self.task_variable_dict)

            sql = "show proxyconfig like '{0}';".format(parameter)
            self.stdio.verbose("GetObproxyParameterHandler execute: {0}".format(sql))
            data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.stdio.verbose("parameter result:{0}".format(data))
            if data is None or len(data) == 0:
                data = ""
            else:
                data = data[0]["value"]
            if data is None:
                data = ""
            self.stdio.verbose("parameter result:{0}".format(Util.convert_to_number(str(data))))
            if "result" in self.step and "set_value" in self.step["result"]:
                self.stdio.verbose("sql execute update task_variable_dict: {0} = {1}".format(self.step["result"]["set_value"], Util.convert_to_number(data)))
                self.task_variable_dict[self.step["result"]["set_value"]] = Util.convert_to_number(data)
        except Exception as e:
            self.stdio.error("GetObproxyParameterHandler execute Exception: {0}".format(e))
            raise StepExecuteFailException("GetObproxyParameterHandler execute Exception: {0}".format(e))
        finally:
            self.ob_connector_pool.release_connection(self.ob_connector)

    def update_step_variable_dict(self):
        return self.task_variable_dict
