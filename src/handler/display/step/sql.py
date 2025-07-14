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
@time: 2024/08/31
@file: sql.py
@desc:
"""
from src.common.stdio import SafeStdio
from src.common.tool import StringUtils
from prettytable import PrettyTable


class StepSQLHandler(SafeStdio):
    def __init__(self, context, step, ob_cluster, task_variable_dict, env, db_connector):
        self.context = context
        self.stdio = context.stdio
        try:
            self.ob_cluster = ob_cluster
            self.ob_cluster_name = ob_cluster.get("cluster_name")
            self.tenant_mode = None
            self.sys_database = None
            self.database = None
            self.env = env
            self.db_connector = db_connector
        except Exception as e:
            self.stdio.error("StepSQLHandler init fail. Please check the OBCLUSTER conf. OBCLUSTER: {0} Exception : {1} .".format(ob_cluster, e))
        self.task_variable_dict = task_variable_dict
        self.enable_dump_db = False
        self.enable_fast_dump = False
        self.ob_major_version = None
        self.step = step

    def execute(self):
        try:
            data = ""
            if "sql" not in self.step:
                self.stdio.error("StepSQLHandler execute sql is not set")
                return
            sql = StringUtils.build_sql_on_expr_by_dict(self.step["sql"], self.task_variable_dict)
            params = StringUtils.extract_parameters(sql)
            for param in params:
                values = self.env.get(param)
                if values is None or len(values) == 0:
                    self.stdio.print("the values of param %s is None", param)
                    return
            sql = StringUtils.replace_parameters(sql, self.env)
            self.stdio.verbose("StepSQLHandler execute: {0}".format(sql))
            columns, data = self.db_connector.execute_sql_return_columns_and_data(sql)
            if data is None or len(data) == 0:
                self.stdio.verbose("excute sql: {0},  result is None".format(sql))
            table = PrettyTable(columns)
            for row in data:
                table.add_row(row)
            for column in columns:
                table.align[column] = 'l'
            title = self.step.get("tittle")
            if title is not None:
                title = StringUtils.replace_parameters(title, self.env)
                formatted_title = f"\n[obdiag display]: {title} "
                self.stdio.print(formatted_title)
                data = data + "\n" + formatted_title
            self.stdio.print(table)
            data = data + "\n" + table.get_string()
            return data
        except Exception as e:
            self.stdio.error("StepSQLHandler execute Exception: {0}".format(e).strip())

    def update_step_variable_dict(self):
        return self.task_variable_dict
