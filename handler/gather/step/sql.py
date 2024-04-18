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
@time: 2024/01/04
@file: sql.py
@desc:
"""
import os
from stdio import SafeStdio
from common.ob_connector import OBConnector
from tabulate import tabulate
from common.tool import StringUtils


class StepSQLHandler(SafeStdio):
    def __init__(self, context, step, ob_cluster, report_path, task_variable_dict):
        self.context = context
        self.stdio=context.stdio
        try:
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
        self.task_variable_dict = task_variable_dict
        self.enable_dump_db = False
        self.enable_fast_dump = False
        self.ob_major_version = None
        self.step = step
        self.report_path = report_path
        self.report_file_path = os.path.join(self.report_path, "sql_result.txt")

    def execute(self):
        try:
            if "sql" not in self.step:
                self.stdio.error("StepSQLHandler execute sql is not set")
                return
            sql = StringUtils.build_sql_on_expr_by_dict(self.step["sql"], self.task_variable_dict)
            self.stdio.verbose("StepSQLHandler execute: {0}".format(sql))
            columns, data = self.ob_connector.execute_sql_return_columns_and_data(sql)
            if data is None or len(data) == 0:
                self.stdio.verbose("excute sql: {0},  result is None".format(sql))
            self.report(sql, columns, data)
        except Exception as e:
            self.stdio.error("StepSQLHandler execute Exception: {0}".format(e).strip())

    def update_step_variable_dict(self):
        return self.task_variable_dict

    def report(self, sql, column_names, data):
        try:
            table_data = [list(row) for row in data]
            formatted_table = tabulate(table_data, headers=column_names, tablefmt="grid")

            # Check file size and rename if necessary
            while True:
                if not os.path.exists(self.report_file_path):
                    break

                file_size = os.path.getsize(self.report_file_path)
                if file_size < 200 * 1024 * 1024:  # 200 MB
                    break

                # Increment file suffix and update self.report_file_path
                base_name, ext = os.path.splitext(self.report_file_path)
                parts = base_name.split('_')
                if len(parts) > 1 and parts[-1].isdigit():  # Check if the last part is a digit
                    suffix = int(parts[-1]) + 1
                    new_base_name = '_'.join(parts[:-1]) + '_{}'.format(suffix)
                else:
                    new_base_name = base_name + '_1'
                self.report_file_path = '{}{}'.format(new_base_name, ext)

            with open(self.report_file_path, 'a', encoding='utf-8') as f:
                f.write('\n\n' + 'obclient > ' + sql + '\n')
                f.write(formatted_table)
        except Exception as e:
            self.stdio.error("report sql result to file: {0} failed, error: {1}".format(self.report_file_path, str(e)))