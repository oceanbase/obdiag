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
@time: 2024/8/19
@file: analyze_index_space.py
@desc:
"""


import sys
from prettytable import PrettyTable
from common.tool import StringUtils, Util
from common.ob_connector import OBConnector
from common.command import get_observer_version
from result_type import ObdiagResult


def translate_byte(B):
    if B < 0:
        B = -B
        return '-' + translate_byte(B)
    if B == 0:
        return '0B'
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    k = 1024
    i = 0
    while B >= k and i < len(units) - 1:
        B /= k
        i += 1
    return f"{B:.2f} {units[i]}"


class AnalyzeIndexSpaceHandler(object):
    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.ob_version = get_observer_version(self.context)
        self.sys_connector = None
        self.tenant_id = None
        self.table_id = None
        self.index_id = None
        self.column_names = []
        self.estimated_table_data = None
        self.result_map_list = []

    def init_option(self):
        options = self.context.options
        ob_cluster = self.context.cluster_config
        self.stdio.verbose('cluster config: {0}'.format(StringUtils.mask_passwords(ob_cluster)))
        self.ob_cluster = ob_cluster
        self.sys_connector = OBConnector(ip=ob_cluster.get("db_host"), port=ob_cluster.get("db_port"), username=ob_cluster.get("tenant_sys").get("user"), password=ob_cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=100)
        tenant_name = Util.get_option(options, 'tenant_name')
        table_name = Util.get_option(options, 'table_name')
        index_name = Util.get_option(options, 'index_name')
        column_names = Util.get_option(options, 'column_names')
        # get tenant id
        tenant_data = self.sys_connector.execute_sql("select tenant_id from oceanbase.__all_tenant where tenant_name = '{0}';".format(tenant_name))
        if len(tenant_data) == 0:
            raise Exception("can not find tenant id by tenant name: {0}. Please check the tenant name.".format(tenant_name))
        self.tenant_id = tenant_data[0][0]
        if self.tenant_id is None:
            raise Exception("can not find tenant id by tenant name: {0}. Please check the tenant name.".format(tenant_name))
        # get table id
        table_id_data = self.sys_connector.execute_sql("select table_id from oceanbase.__all_virtual_table where table_name = '{0}' and tenant_id = '{1}';".format(table_name, self.tenant_id))
        if len(table_id_data) == 0:
            raise Exception("can not find table id by table name: {0}. Please check the table name.".format(table_name))
        self.table_id = table_id_data[0][0]
        if self.table_id is None:
            raise Exception("can not find table id by table name: {0}. Please check the table name.".format(table_name))
        # get index id
        if index_name is not None:
            index_id_data = self.sys_connector.execute_sql("select table_id from oceanbase.__all_virtual_table where table_name like '%{0}%' and data_table_id = '{1}' and tenant_id = '{2}';".format(index_name, self.table_id, self.tenant_id))
            if len(index_id_data) == 0:
                raise Exception("can not find index id by index name: {0}. Please check the index name.".format(index_name))
            self.index_id = index_id_data[0][0]
            if self.index_id is None:
                raise Exception("can not find index id by index name: {0}. Please check the index name.".format(index_name))
        # get column names
        if column_names is not None:
            self.column_names = column_names.split(',')
            if len(self.column_names) == 0:
                raise Exception("--column_names parameter format is incorrect: {0}.".format(column_names))
        return True

    def handle(self):
        try:
            self.init_option()
        except Exception as e:
            self.stdio.error("init option failed: {0}".format(str(e)))
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="init option failed: {0}".format(str(e)))
        try:
            # evaluate the space size of the table where the index is located
            self.stdio.start_loading('start query estimated_table_data_size, please wait some minutes...')
            sql = "select svr_ip, svr_port, sum(original_size) as estimated_table_size from oceanbase.__all_virtual_tablet_sstable_macro_info where tablet_id in (select tablet_id from oceanbase.__all_virtual_tablet_to_table_history where table_id = {0}) and (svr_ip, svr_port) in (select svr_ip, svr_port from oceanbase.__all_virtual_ls_meta_table where role = 1) group by svr_ip, svr_port;".format(
                self.table_id
            )
            self.stdio.verbose("execute_sql is {0}".format(sql))
            self.estimated_table_data = self.sys_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.stdio.stop_loading('succeed')
            if len(self.estimated_table_data) == 0:
                raise Exception("can not find estimated_table_data on __all_virtual_tablet_sstable_macro_info by table id: {0}. Please wait major or manually major'".format(self.table_id))
            # get the sum of all column lengths
            sql = "select table_id, sum(data_length) as all_columns_length from oceanbase.__all_virtual_column_history where tenant_id = '{0}' and table_id = '{1}';".format(self.tenant_id, self.table_id)
            self.stdio.verbose("execute_sql is {0}".format(sql))
            self.main_table_sum_of_data_length = int(self.sys_connector.execute_sql_return_cursor_dictionary(sql).fetchall()[0]["all_columns_length"])
            # get the sum of column lengths included in the index
            if self.index_id is not None:
                sql = "select table_id, sum(data_length) as index_columns_length from oceanbase.__all_virtual_column_history where tenant_id = '{0}' and table_id = '{1}';".format(self.tenant_id, self.index_id)
                self.stdio.verbose("execute_sql is {0}".format(sql))
                self.index_table_sum_of_data_length = int(self.sys_connector.execute_sql_return_cursor_dictionary(sql).fetchall()[0]["index_columns_length"])
            elif len(self.column_names) != 0:
                sql = "select table_id, sum(data_length) as columns_length from oceanbase.__all_virtual_column_history where tenant_id = '{0}' and table_id = '{1}' and column_name in ('{2}');".format(
                    self.tenant_id, self.table_id, "','".join(self.column_names)
                )
                self.stdio.verbose("execute_sql is {0}".format(sql))
                self.index_table_sum_of_data_length = int(self.sys_connector.execute_sql_return_cursor_dictionary(sql).fetchall()[0]["columns_length"])
            else:
                raise Exception("please specify an index or column.")

            # estimate the final space size
            estimated_index_data = []
            for node_table_estimated_size in self.estimated_table_data:
                node_estimated_index_data = {}
                node_estimated_index_data["svr_ip"] = node_table_estimated_size["svr_ip"]
                node_estimated_index_data["svr_port"] = node_table_estimated_size["svr_port"]
                estimiated_index_size = int(self.index_table_sum_of_data_length / self.main_table_sum_of_data_length * int(node_table_estimated_size["estimated_table_size"]))
                if self.ob_version == "4.2.3.0" or StringUtils.compare_versions_greater(self.ob_version, "4.2.3.0"):
                    self.stdio.verbose("magnification is 1.5")
                    target_server_estimated_size = int(estimiated_index_size * 15 / 10)
                else:
                    self.stdio.verbose("magnification is 5.5")
                    target_server_estimated_size = int(estimiated_index_size * 55 / 10)
                node_estimated_index_data["estimiated_index_size"] = target_server_estimated_size
                estimated_index_data.append(node_estimated_index_data)
            for node_estimated_index_data in estimated_index_data:
                target_server_ip = node_estimated_index_data["svr_ip"]
                target_server_port = node_estimated_index_data["svr_port"]
                target_server_estimated_index_size = int(node_estimated_index_data["estimiated_index_size"])
                # get target_server_total_size and target_server_used_size
                target_server_data = self.sys_connector.execute_sql_return_cursor_dictionary(
                    "select total_size, used_size from oceanbase.__all_virtual_disk_stat where svr_ip = '{0}' and svr_port = {1};".format(target_server_ip, target_server_port)
                ).fetchall()
                target_server_total_size = int(target_server_data[0]["total_size"])
                target_server_used_size = int(target_server_data[0]["used_size"])
                # get data_disk_usage_limit_percentage
                sql = "SELECT VALUE FROM oceanbase.GV$OB_PARAMETERS WHERE SVR_IP='{0}' and SVR_PORT='{1}' and NAME LIKE  \"data_disk_usage_limit_percentage\"".format(target_server_ip, target_server_port)
                self.stdio.verbose("execute_sql is {0}".format(sql))
                data_disk_usage_limit_percentage = int(self.sys_connector.execute_sql_return_cursor_dictionary(sql).fetchall()[0]["VALUE"])
                # data_disk_usage_limit_percentage is a Cluster level configuration items
                available_disk_space = int(target_server_total_size / 100 * data_disk_usage_limit_percentage - target_server_used_size)
                node_result_map = {}
                node_result_map["ip"] = target_server_ip
                node_result_map["port"] = target_server_port
                node_result_map["estimated_index_space"] = translate_byte(target_server_estimated_index_size)
                node_result_map["available_disk_space"] = translate_byte(available_disk_space)
                self.result_map_list.append(node_result_map)
            self.export_report_table()
            self.stdio.verbose("end analyze index space")
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=self.execute())
        except Exception as e:
            self.stdio.error("analyze index space error: {0}".format(e))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="analyze index space error: {0}".format(e))

    def execute(self):
        result_map = {}
        result_map["result"] = self.result_map_list
        return result_map

    def export_report_table(self):
        try:
            report_index_space_tb = PrettyTable(["ip", "port", "estimated_index_space", "available_disk_space"])
            report_index_space_tb.align["task_report"] = "l"
            report_index_space_tb.title = "estimated-index-space-report"
            for result in self.result_map_list:
                report_index_space_tb.add_row([result["ip"], result["port"], result["estimated_index_space"], result["available_disk_space"]])
            self.stdio.print(report_index_space_tb)
        except Exception as e:
            raise Exception("export report {0}".format(e))
