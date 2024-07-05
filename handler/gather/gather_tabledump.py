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
@time: 2024/04/15
@file: gather_tabledump_handler.py
@desc:
"""

import os
import re
from stdio import SafeStdio
import datetime
from common.ob_connector import OBConnector
from handler.gather.scenes.base import SceneBase
from common.obdiag_exception import OBDIAGFormatException
from handler.gather.scenes.list import GatherScenesListHandler
from common.tool import DirectoryUtil
from common.tool import StringUtils
from common.command import get_observer_version
from colorama import Fore, Style
from common.tool import Util
from common.tool import TimeUtils
from tabulate import tabulate
from handler.checker.check_exception import CheckException
from colorama import Fore, Style


class GatherTableDumpHandler(SafeStdio):

    def __init__(self, context, task_type="observer", export_report_path="./gather_report"):
        self.context = context
        self.stdio = context.stdio
        self.is_ssh = True
        self.report = None
        self.report_path = None
        self.yaml_tasks = {}
        self.code_tasks = []
        self.env = {}
        self.scene = None
        self.task_type = task_type
        self.variables = {}
        self.ob_cluster = {}
        self.ob_connector = {}
        self.tenant_connector = {}
        self.database = None
        self.table = None
        self.result_list = []
        self.export_report_path = export_report_path
        try:
            if not os.path.exists(export_report_path):
                os.makedirs(export_report_path)
        except Exception as e:
            self.stdio.error("init gather_report {0}".format(e))
            raise CheckrReportException("int gather_report {0}".format(e))
        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        try:
            self.ob_cluster = self.context.cluster_config
            self.obproxy_nodes = self.context.obproxy_config['servers']
            self.ob_nodes = self.context.cluster_config['servers']
            new_nodes = Util.get_nodes_list(self.context, self.ob_nodes, self.stdio)
            if new_nodes:
                self.nodes = new_nodes
            options = self.context.options
            self.database = Util.get_option(options, 'database')
            self.table = Util.get_option(options, 'table')
            user = Util.get_option(options, 'user')
            password = Util.get_option(options, 'password')
            ## 获取租户名
            self.tenant_name = self.__extract_string(user)
            self.ob_connector = OBConnector(
                ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.ob_cluster.get("tenant_sys").get("user"), password=self.ob_cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=100
            )
            self.tenant_connector = OBConnector(ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=user, password=password, stdio=self.stdio, timeout=100)
            self.file_name = "{0}/obdiag_tabledump_result_{1}.txt".format(self.export_report_path, self.gather_timestamp)
            return True
        except Exception as e:
            return False

    def handle(self):
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        self.execute()
        self.stdio.print("get table info finished. For more details, please run cmd '" + Fore.YELLOW + " cat {0} ".format(self.file_name) + Style.RESET_ALL + "'")

    def execute(self):
        try:
            self.version = get_observer_version(self.context)
            ## 获取建表语句
            self.__get_table_schema()

            ## 获取表信息
            if self.version == "4.0.0.0" or StringUtils.compare_versions_greater(self.version, "4.0.0.0"):
                self.__get_table_info()
            else:
                self.__get_table_info_v3()
        except Exception as e:
            self.stdio.error("report sql result to file: {0} failed, error: ".format(self.file_name))
            self.stdio.error("StepSQLHandler execute Exception: {0}".format(e).strip())

    def __get_table_schema(self):
        sql = "show create table " + self.database + "." + self.table
        columns, result = self.tenant_connector.execute_sql_return_columns_and_data(sql)
        if result is None or len(result) == 0:
            self.stdio.verbose("excute sql: {0},  result is None".format(sql))
        self.__report(sql, columns, result)

    def __get_table_info(self):
        try:
            tenant_data = self.ob_connector.execute_sql_return_cursor_dictionary("select tenant_id from oceanbase.__all_tenant where tenant_name='{0}'".format(self.tenant_name))
            if tenant_data is None:
                self.stdio.error("tenant is None")
                return
            self.tenant_id = tenant_data.fetchall()[0].get("tenant_id")

            database_data = self.ob_connector.execute_sql_return_cursor_dictionary(
                "SELECT con_id as tenant_id, object_id as database_id, object_name as database_name FROM  oceanbase.cdb_objects where OBJECT_TYPE = 'DATABASE' and con_id = '{0}' and object_name='{1}' ".format(self.tenant_id, self.database)
            )
            if database_data is None:
                self.stdio.error("database is None")
                return
            self.database_id = database_data.fetchall()[0].get("database_id")
            table_data = self.ob_connector.execute_sql_return_cursor_dictionary(
                "select /*+read_consistency(weak) QUERY_TIMEOUT(60000000) */ t.table_id from oceanbase.__all_virtual_table t where t.tenant_id = '{0}' and t.database_id = '{1}' and table_name = '{2}' limit 1 ".format(
                    self.tenant_id, self.database_id, self.table
                )
            )
            if table_data is None:
                self.stdio.error("table is None")
                return
            self.table_id = table_data.fetchall()[0].get("table_id")

            ## 查询行数
            query_count = "select /*+read_consistency(weak) QUERY_TIMEOUT(60000000) */ table_name as 'Table' , ifnull(num_rows,0) as num_rows from oceanbase.cdb_tables where con_id = '{0}' and owner = '{1}' and table_name = '{2}' order by num_rows desc limit 1".format(
                self.tenant_id, self.database, self.table
            )
            columns, result = self.ob_connector.execute_sql_return_columns_and_data(query_count)
            if result is None:
                self.stdio.error("line Count is None")
                return
            self.stdio.print("table count {0}".format(result))

            self.__report(query_count, columns, result)
            ## 查询数据量
            query_data = '''select /*+read_consistency(weak) QUERY_TIMEOUT(60000000) */ t1.SVR_IP,t1.role,ifnull(t2.data_size,0) as total_data_size from (select SVR_IP,tenant_id, database_name, role, table_id, tablet_id from oceanbase.cdb_ob_table_locations) t1 left join (select tenant_id, tablet_id,data_size from oceanbase.cdb_ob_tablet_replicas) t2 on t1.tenant_id = t2.tenant_id and t1.tablet_id = t2.tablet_id where  t1.tenant_id = '{0}' and t1.table_id = '{1}' order by total_data_size desc limit 1'''.format(
                self.tenant_id, self.table_id
            )

            columns, result = self.ob_connector.execute_sql_return_columns_and_data(query_data)
            if result is None:
                self.stdio.error("dataSize is None")
                return
            self.stdio.print("data size {0}".format(result))
            self.__report(query_data, columns, result)

        except Exception as e:
            self.stdio.error("getTableInfo execute Exception: {0}".format(e).strip())

    def __get_table_info_v3(self):
        try:
            tenant_data = self.ob_connector.execute_sql_return_cursor_dictionary("select tenant_id from oceanbase.__all_tenant where tenant_name='{0}'".format(self.tenant_name))
            if tenant_data is None:
                self.stdio.error("tenant is None")
                return
            self.tenant_id = tenant_data.fetchall()[0].get("tenant_id")
            database_data = self.ob_connector.execute_sql_return_cursor_dictionary("select tenant_id,database_id,database_name from oceanbase.gv$database where tenant_name = '{0}' and database_name = '{1}' ".format(self.tenant_name, self.database))
            if database_data is None:
                self.stdio.error("database is None")
                return
            self.database_id = database_data.fetchall()[0].get("database_id")
            table_data = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.__all_virtual_table where table_name='{0}' and database_id='{1}' and tenant_id='{2}'".format(self.table, self.database_id, self.tenant_id))
            if table_data is None:
                self.stdio.error("table is None")
                return
            self.table_id = table_data.fetchall()[0].get("table_id")

            ## 查询行数 和 数据量
            query_count = '''select /*+read_consistency(weak) QUERY_TIMEOUT(60000000) */ m.svr_ip,m.role,m.data_size total_data_size, m.row_count as total_rows_count from oceanbase.__all_virtual_meta_table m, oceanbase.__all_virtual_table t 
                            where m.table_id = t.table_id and m.tenant_id = '{0}' and m.table_id = '{1}' and t.table_name = '{2}' order by total_rows_count desc limit 1'''.format(
                self.tenant_id, self.table_id, self.table
            )
            columns, result = self.ob_connector.execute_sql_return_columns_and_data(query_count)
            if result is None:
                self.stdio.error("dataSize and line count is None")
                return
            self.stdio.print("table count {0}".format(result))
            self.__report(query_count, columns, result)

        except Exception as e:
            self.stdio.error("getTableInfo execute Exception: {0}".format(e).strip())

    def __report(self, sql, column_names, data):
        try:
            table_data = [list(row) for row in data]
            formatted_table = tabulate(table_data, headers=column_names, tablefmt="grid")
            with open(self.file_name, 'a', encoding='utf-8') as f:
                f.write('\n\n' + 'obclient > ' + sql + '\n')
                f.write(formatted_table)
        except Exception as e:
            self.stdio.error("report sql result to file: {0} failed, error: ".format(self.file_name))

    def __extract_string(self, s):
        # 检查字符串是否包含'@'
        if '@' in s:
            # 查找'@'和'#'的索引
            at_index = s.index('@')
            # 检查是否包含'#'
            if '#' in s:
                hash_index = s.index('#')
                # 确保'#'在'@'之后
                if hash_index > at_index:
                    # 截取'@'和'#'之间的内容
                    return s[at_index + 1 : hash_index]
                else:
                    # 如果'#'在'@'之前，仅截取'@'之后的内容
                    return s[at_index + 1 :]
            else:
                # 如果不包含'#'，截取'@'之后的内容
                return s[at_index + 1 :]
        else:
            # 如果不包含'@'，则返回原字符串或空字符串，根据需要决定
            return s  # 或者 return ''


class CheckrReportException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(CheckrReportException, self).__init__(msg, obj)
