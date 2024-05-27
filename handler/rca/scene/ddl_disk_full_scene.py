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
@time: 2024/04/01
@file: ddl_disk_full_scene.py
@desc:
"""
import re

from handler.rca.rca_exception import RCAInitException, RCAExecuteException
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import StringUtils


class DDlDiskFullScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.index_table_id = None
        self.estimated_size = None
        self.estimated_data_size = None
        self.index_name = None
        self.action_type = None
        self.table_id = None
        self.tenant_id = None

    def init(self, context):
        super().init(context)

        ## observer version>4.2.1.0
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.2.1.0" or StringUtils.compare_versions_greater(observer_version, "4.2.1.0")):
            self.stdio.error("observer version is {0}, which is less than 4.2.1.0.".format(observer_version))
            raise RCAInitException("observer version is {0}, which is less than 4.2.1.0.".format(observer_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.verbose("observer version is {0}.".format(observer_version))
        # check table_name and tenant_name

        table_name = self.input_parameters.get("table_name")
        tenant_name = self.input_parameters.get("tenant_name")
        action_type = self.input_parameters.get("action_type")
        index_name = self.input_parameters.get("index_name")
        if table_name is None or table_name == "" or tenant_name is None or tenant_name == "":
            raise RCAInitException("table_name or tenant_name is None. Please check the input parameters.")
        if action_type is not None:
            if action_type == "add_index":
                self.action_type = action_type
                self.verbose("action type is {0}.".format(action_type))
                if index_name is not None and index_name.strip() != "":
                    self.verbose("index name is {0}.".format(index_name))
                    self.index_name = index_name.strip()
                else:
                    self.action_type = None
                    self.stdio.error("action type is {0}. but index_name is None. Please input it.".format(action_type))
            else:
                self.stdio.error("action type is {0}, but only support add_index now.".format(action_type))

        tenant_data = self.ob_connector.execute_sql("select tenant_id from oceanbase.__all_tenant where tenant_name = '{0}';".format(tenant_name))
        if len(tenant_data) == 0:
            raise RCAInitException("can not find tenant id by tenant name: {0}. Please check the tenant name.".format(tenant_name))
        self.tenant_id = tenant_data[0][0]
        if self.tenant_id is None:
            raise RCAInitException("can not find tenant id by tenant name: {0}. Please check the tenant name.".format(tenant_name))

        table_id_data = self.ob_connector.execute_sql("select table_id from oceanbase.__all_virtual_table where table_name = '{0}';".format(table_name))
        if len(table_id_data) == 0:
            raise RCAInitException("can not find table id by table name: {0}. Please check the table name.".format(table_name))
        self.table_id = table_id_data[0][0]
        if self.table_id is None:
            raise RCAInitException("can not find table id by table name: {0}. Please check the table name.".format(table_name))
        self.verbose("table_id is {0}, tenant_id is {1}.".format(self.table_id, self.tenant_id))

    def verbose(self, info):
        self.stdio.verbose("[DDlDiskFullScene] {0}".format(info))

    def execute(self):
        try:
            record = RCA_ResultRecord()
            record.add_record("table_id is {0}".format(self.table_id))
            record.add_record("tenant_id is {0}".format(self.tenant_id))
            # get estimated_data_size
            self.verbose("start to get estimated_data_size...")
            ## if the action is not add_index
            sql = "select svr_ip, svr_port, sum(original_size) as estimated_data_size from oceanbase.__all_virtual_tablet_sstable_macro_info where tablet_id in (select tablet_id from oceanbase.__all_virtual_tablet_to_table_history where table_id = {0}) and (svr_ip, svr_port) in (select svr_ip, svr_port from oceanbase.__all_virtual_ls_meta_table where role = 1) group by svr_ip, svr_port;".format(
                self.table_id
            )
            self.verbose("execute_sql is {0}".format(sql))
            tablet_size_data = self.ob_connector.execute_sql(sql)
            self.verbose("tablet_size_data is {0}".format(tablet_size_data))
            record.add_record("tablet_size_data is {0}".format(tablet_size_data))
            if len(tablet_size_data) <= 0 or tablet_size_data[0][2] is None:
                raise RCAExecuteException("can not find tablet size info or estimated_data_size. please check the data:{0}.".format(tablet_size_data))
            self.estimated_size = tablet_size_data
            self.verbose("estimated_size is {0}".format(self.estimated_size))
            record.add_record("estimated_size is {0}".format(self.estimated_size))

            # get estimated_size to self.estimated_size
            if self.action_type is not None and self.action_type == "add_index":
                self.verbose("start add_index_action")
                record.add_record("index_name is {0}".format(self.index_name))
                record.add_record("action_type is {0}".format(self.action_type))
                ## if the action is add_index
                sql = "select table_id from oceanbase.__all_virtual_table_history where tenant_id = '{0}' and data_table_id = '{1}' and table_name like '%{2}%';".format(self.tenant_id, self.table_id, self.index_name)
                self.verbose("execute_sql is {0}".format(sql))
                self.index_table_id = self.ob_connector.execute_sql(sql)[0][0]
                self.verbose("index_table_id is {0}".format(self.index_table_id))
                record.add_record("index_table_id is {0}".format(self.index_table_id))

                # Query the sum of the lengths of all columns in the main table
                sql = "select table_id, sum(data_length) from oceanbase.__all_virtual_column_history where tenant_id = '{0}' and table_id = '{1}';".format(self.tenant_id, self.table_id)
                self.verbose("execute_sql is {0}".format(sql))
                main_table_sum_of_data_length = int(self.ob_connector.execute_sql(sql)[0][1])
                self.verbose("main_table_sum_of_data_length is {0}".format(main_table_sum_of_data_length))
                record.add_record("main_table_sum_of_data_length is {0}".format(main_table_sum_of_data_length))

                # The sum of the lengths of all columns in the query index
                sql = "select table_id, sum(data_length) from oceanbase.__all_virtual_column_history where tenant_id = '{0}' and table_id = '{1}';".format(self.tenant_id, self.index_table_id)
                self.verbose("execute_sql is {0}".format(sql))
                index_table_sum_of_data_length = int(self.ob_connector.execute_sql(sql)[0][1])
                self.verbose("index_table_sum_of_data_length is {0}".format(index_table_sum_of_data_length))
                record.add_record("index_table_sum_of_data_length is {0}".format(index_table_sum_of_data_length))

                #
                new_estimated_size = []
                for node_estimated_size in self.estimated_size:
                    new_node_estimated_size = [node_estimated_size[0], node_estimated_size[1]]
                    estimiated_index_size = int(index_table_sum_of_data_length / main_table_sum_of_data_length / 1024 / 1024 * int(node_estimated_size[2]))

                    new_node_estimated_size.append(estimiated_index_size)
                    new_estimated_size.append(new_node_estimated_size)
                self.estimated_size = new_estimated_size
                self.verbose("estimated_size is {0}".format(self.estimated_size))

            for estimated_size in self.estimated_size:
                target_server_ip = estimated_size[0]
                target_server_port = estimated_size[1]
                target_server_estimated_size = int(estimated_size[2])
                self.verbose("On target_server_ip is {0}, target_server_port is {1}, target_server_estimated_size is {2}".format(target_server_ip, target_server_port, target_server_estimated_size))
                record.add_record("On target_server_ip is {0}, target_server_port is {1}, target_server_estimated_size is {2}".format(target_server_ip, target_server_port, target_server_estimated_size))

                # get target_server_total_size and target_server_used_size
                target_server_data = self.ob_connector.execute_sql("select total_size, used_size from oceanbase.__all_virtual_disk_stat where svr_ip = '{0}' and svr_port = {1};".format(target_server_ip, target_server_port))
                target_server_total_size = int(target_server_data[0][0])
                self.verbose("target_server_total_size is {0}".format(target_server_total_size))
                record.add_record("target_server_total_size is {0}".format(target_server_total_size))

                target_server_used_size = int(target_server_data[0][1])
                self.verbose("target_server_used_size is {0}".format(target_server_used_size))
                record.add_record("target_server_used_size is {0}".format(target_server_used_size))

                # get data_disk_usage_limit_percentage
                sql = "SELECT VALUE FROM oceanbase.GV$OB_PARAMETERS WHERE SVR_IP='{0}' and SVR_PORT='{1}' and NAME LIKE  \"data_disk_usage_limit_percentage\"".format(target_server_ip, target_server_port)
                self.verbose("execute_sql is {0}".format(sql))
                data_disk_usage_limit_percentage = int(self.ob_connector.execute_sql(sql)[0][0])
                # data_disk_usage_limit_percentage is a Cluster level configuration items
                self.verbose("data_disk_usage_limit_percentage is {0}".format(data_disk_usage_limit_percentage))
                record.add_record("data_disk_usage_limit_percentage is {0}".format(data_disk_usage_limit_percentage))
                if self.observer_version == "4.3.0.0" or StringUtils.compare_versions_greater(self.observer_version, "4.3.0.0"):
                    target_server_estimated_size = int(target_server_estimated_size * 15 / 10)
                else:
                    target_server_estimated_size = int(target_server_estimated_size * 55 / 10)
                self.verbose("target_server_estimated_size is {0}".format(target_server_estimated_size))
                record.add_record("target_server_estimated_size is {0}".format(target_server_estimated_size))

                available_disk_space = int(target_server_total_size / 100 * data_disk_usage_limit_percentage - target_server_used_size)
                self.verbose("available_disk_space is {0}".format(available_disk_space))
                record.add_record("available_disk_space is {0}".format(available_disk_space))

                if target_server_estimated_size - available_disk_space > 0:
                    record.add_record("target_server_estimated_size - available_disk_space is {0}".format(target_server_estimated_size - available_disk_space))
                    record.add_suggest("the disk space of server({0}:{1}) disk is not enough.  please add the server disk".format(target_server_ip, target_server_port))
                else:
                    record.add_record("target_server_estimated_size - available_disk_space is {0}".format(target_server_estimated_size - available_disk_space))
                    record.add_suggest("the disk space of server({0}:{1}) is enough. Don't warn ".format(target_server_ip, target_server_port))
            self.Result.records.append(record)
        except Exception as e:
            raise RCAExecuteException("DDlDiskFullScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end DDlDiskFullScene execute")

    def export_result(self):
        super().export_result()

    def get_scene_info(self):

        return {
            "name": "ddl_disk_full",
            "info_en": "Insufficient disk space reported during DDL process. ",
            "info_cn": 'DDL过程中报磁盘空间不足的问题',
        }


ddl_disk_full = DDlDiskFullScene()
