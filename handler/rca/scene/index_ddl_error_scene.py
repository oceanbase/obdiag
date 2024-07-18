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
@time: 2024/06/04
@file: index_ddl_error_scene.py
@desc:
"""
import re

from handler.rca.rca_exception import RCAInitException, RCAExecuteException
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import StringUtils


class IndexDDLErrorScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.index_table_id = None
        self.work_path = None
        self.estimated_size = None
        self.estimated_data_size = None
        self.index_name = None
        self.action_type = None
        self.table_id = None
        self.tenant_id = None

    def init(self, context):
        super().init(context)
        ## observer version>4.2.3.0
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.2.3.0" or StringUtils.compare_versions_greater(observer_version, "4.2.3.0")):
            self.stdio.error("observer version is {0}, which is less than 4.2.3.0.".format(observer_version))
            raise RCAInitException("observer version is {0}, which is less than 4.2.3.0.".format(observer_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.verbose("observer version is {0}.".format(observer_version))
        # check table_name and tenant_name and database_name and index_name
        table_name = self.input_parameters.get("table_name")
        tenant_name = self.input_parameters.get("tenant_name")
        action_type = self.input_parameters.get("action_type")
        index_name = self.input_parameters.get("index_name")
        database_name = self.input_parameters.get("database_name")
        if table_name is None or table_name == "" or tenant_name is None or tenant_name == "" or index_name is None or index_name == "" or database_name is None or database_name == "":
            raise RCAInitException("table_name or tenant_name or database_name or index_name is None. Please check the input parameters.")

        tenant_data = self.ob_connector.execute_sql("select tenant_id from oceanbase.__all_tenant where tenant_name = '{0}';".format(tenant_name))
        if len(tenant_data) == 0:
            raise RCAInitException("can not find tenant id by tenant name: {0}. Please check the tenant name.".format(tenant_name))
        self.tenant_id = tenant_data[0][0]
        self.verbose("tenant_id is {0}".format(self.tenant_id))
        if self.tenant_id is None:
            raise RCAInitException("can not find tenant id by tenant name: {0}. Please check the tenant name.".format(tenant_name))
        database_id_data = self.ob_connector.execute_sql("select database_id from oceanbase.__all_database where database_name = '{0}';".format(database_name))
        if len(database_id_data) == 0:
            raise RCAInitException("can not find database id by database name: {0}. Please check the table name.".format(database_name))
        self.database_id = database_id_data[0][0]
        self.verbose("database_id is{0}".format(self.database_id))
        if self.database_id is None:
            raise RCAInitException("can not find database id by tenant name: {0}. Please check the database name.".format(database_name))
        table_id_data = self.ob_connector.execute_sql("select table_id  from oceanbase.__all_virtual_table where table_name='{0}' and tenant_id='{1}' and database_id='{2}';".format(table_name, self.tenant_id, self.database_id))
        if len(table_id_data) == 0:
            raise RCAInitException("can not find table id by table name: {0}. Please check the table name.".format(table_name))
        self.table_id = table_id_data[0][0]
        self.verbose("table_id is{0}".format(self.table_id))
        if self.table_id is None:
            raise RCAInitException("can not find table id by table name: {0}. Please check the database name.".format(table_name))
        # idx_table_id_data=self.ob_connector.execute_sql("select table_id from oceanbase.__all_virtual_table_history where tenant_id ='{0}' and data_table_id='{1}' and table_name like '%{2}%' ;".format(self.tenant_id,self.table_id,index_name))
        idx_table_id_data = self.ob_connector.execute_sql(
            "select table_id from oceanbase.__all_virtual_table_history where tenant_id ='{0}' and data_table_id='{1}' and table_name like '%{2}%' order by  gmt_create desc limit 1;".format(self.tenant_id, self.table_id, index_name)
        )

        if len(idx_table_id_data) == 0:
            raise RCAInitException("can not find index table id by table name: {0}. Please check the index name.".format(index_name))
        self.index_table_id = idx_table_id_data[0][0]
        self.verbose("index_table_id is{0}".format(self.index_table_id))
        if self.index_table_id is None:
            raise RCAInitException("can not find index table id by table name: {0}. Please check the index name.".format(index_name))
        self.verbose("tenant_id is {0},database_id is {1}, table_id is {2},index_table_id is {3}.".format(self.tenant_id, self.database_id, self.table_id, self.index_table_id))

    def verbose(self, info):
        self.stdio.verbose("[IndexDDLErrorScene] {0}".format(info))

    def execute(self):
        try:
            record = RCA_ResultRecord()
            record.add_record("tenant_id is {0}".format(self.tenant_id))
            record.add_record("database_id is {0}".format(self.database_id))
            record.add_record("table_id is {0}".format(self.table_id))
            record.add_record("index_table_id is {0}".format(self.index_table_id))
            record.add_record("index_name is {0}".format(self.input_parameters.get("index_name")))
            self.verbose("start to get trace_id and task_id...")
            # trace_id
            trace_id_data = self.ob_connector.execute_sql("select trace_id from oceanbase.__all_virtual_ddl_error_message where tenant_id = '{0}' and object_id='{1}';".format(self.tenant_id, self.index_table_id))
            self.verbose("trace_id_data is {0}".format(trace_id_data))
            if len(trace_id_data) == 0:
                record.add_suggest("The index creation failure occurs during the RPC sending phase. Manual intervention is required to troubleshoot this issue. Please upload the package to the OcenBase community{0}".format(self.store_dir))
                return
            self.trace_id = trace_id_data[0][0]
            self.verbose("trace_id is{0}".format(self.trace_id))
            if self.trace_id is None:
                raise RCAInitException("can not find trace_id id by index name: {0}. Please check the index name.".format(self.index_name))
            # task_id
            task_id_data = self.ob_connector.execute_sql("select task_id from oceanbase.__all_virtual_ddl_error_message where tenant_id = '{0}' and object_id='{1}';".format(self.tenant_id, self.index_table_id))
            self.verbose("task_id_data is {0}".format(task_id_data))
            if task_id_data is None:
                record.add_suggest("The index creation failure occurs during the RPC sending phase. Manual intervention is required to troubleshoot this issue. Please upload the package to the OcenBase community{0}".format(self.store_dir))
                return
            self.task_id = task_id_data[0][0]
            self.verbose("task_id is {0}".format(self.task_id))

            record.add_record("trace_id is {0}".format(self.trace_id))
            record.add_record("task_id is {0}".format(self.task_id))
            self.verbose("start to get event...")
            # event_data=self.ob_connector.execute_sql("select event, value6,rs_svr_ip, rs_svr_port from oceanbase.__all_rootservice_event_history where value4 = '{0}' and value2 != 0 and event != 'switch_state' and event not like 'index build task process fail' order by gmt_create desc limit 1;".format(self.task_id))
            sql = "select event, value6,rs_svr_ip, rs_svr_port from oceanbase.__all_rootservice_event_history where value4 = '{0}' and value1='{1}' and value2 != 0 and event != 'switch_state' and event not like 'index build task process fail' order by gmt_create desc limit 1;".format(
                self.task_id, self.tenant_id
            )
            event_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("event_data is{0}".format(event_data))
            if event_data is None:
                record.add_record("gather rootservice.log  by {0}".format(self.trace_id))
                # rs
                self.verbose("event_data is None")
                self.verbose("gather rootservice.log  by {0}".format(self.trace_id))
                work_path_rs = self.store_dir + "/{0}_on_rs/".format(self.trace_id)
                self.gather_log.set_parameters("scope", "rootservice")
                if self.input_parameters.get("since") is not None:
                    since = self.input_parameters.get("since")
                    self.gather_log.set_parameters("since", since)
                self.gather_log.grep("{0}".format(self.trace_id))
                logs_name = self.gather_log.execute(save_path=work_path_rs)
                if logs_name is None or len(logs_name) <= 0:
                    self.verbose("no log_disk_full about trace_id:{0}".format(self.trace_id))
                    return False
                record.add_record("Log saving location：{0}".format(work_path_rs))
                record.add_suggest("The index creation failed during the other phase. Please upload {0} to the OceanBase community".format(self.store_dir))
            else:
                record.add_record("event_data is {0}".format(event_data))
                self.event = event_data[0]["event"]
                self.verbose("event is {0}".format(self.event))
                record.add_record("event is {0}".format(self.event))
                self.value6 = event_data[0]["value6"]
                self.inner_sql_execute_addr = self.value6
                self.verbose("inner_sql_execute_addr is {0}".format(self.inner_sql_execute_addr))
                record.add_record("inner_sql_execute_addr is {0}".format(self.inner_sql_execute_addr))
                ip_address = self.inner_sql_execute_addr.split(":")[0].strip('"')
                record.add_record("ip is {0}".format(ip_address))
                if self.event == 'ddl wait trans end ctx try wait':
                    self.verbose("ok,event is ddl wait trans end ctx try wait")
                    record.add_record("event is {0},The failure of index creation occurred during the transaction end phase. In this case, the observer logs need to be retrieved based on the trace_id: {1}".format(self.event, self.trace_id))
                    # ddl_wait_trans_end_ctx_try_wait
                    self.verbose("gather observer.log  by {0}".format(self.trace_id))
                    work_path_ddl_wait_trans_end_ctx_try_wait = self.store_dir + "/{0}_on_obs/".format(self.trace_id)
                    self.gather_log.set_parameters("scope", "observer")
                    if self.input_parameters.get("since") is not None:
                        since = self.input_parameters.get("since")
                        self.gather_log.set_parameters("since", since)
                    self.gather_log.grep("{0}".format(self.trace_id))
                    logs_name = self.gather_log.execute(save_path=work_path_ddl_wait_trans_end_ctx_try_wait)
                    if logs_name is None or len(logs_name) <= 0:
                        self.verbose("no log_disk_full about trace_id:{0}".format(self.trace_id))
                        return False
                    record.add_record(" Log saving location：{0}".format(work_path_ddl_wait_trans_end_ctx_try_wait))
                    record.add_suggest("The failure of index creation occurred during the transaction completion phase. Please upload {0} to the OceanBase community".format(self.store_dir))

                elif self.event == 'index sstable build task finish':
                    self.verbose("ok,event is index sstable build task finish")
                    record.add_record("event is {0},The failure of index creation occurred during the data replenishment phase. In this case, the observer logs need to be retrieved based on the trace_id: {1}".format(self.event, self.trace_id))
                    self.verbose("gather observer.log  by {0}".format(self.trace_id))
                    # index_sstable_build_task_finish
                    work_path_index_sstable_build_task_finish = self.store_dir + "/{0}_on_obs/".format(self.trace_id)
                    self.gather_log.set_parameters("scope", "observer")
                    if self.input_parameters.get("since") is not None:
                        since = self.input_parameters.get("since")
                        self.gather_log.set_parameters("since", since)
                    self.gather_log.grep("{0}".format(self.trace_id))
                    logs_name = self.gather_log.execute(save_path=work_path_index_sstable_build_task_finish)
                    if logs_name is None or len(logs_name) <= 0:
                        self.verbose("no log_disk_full about trace_id:{0}".format(self.trace_id))
                        return False
                    record.add_record(" Log saving location：{0}".format(work_path_index_sstable_build_task_finish))
                    record.add_suggest("The index creation failed during the data replenishment phase. Please upload {0} to the OceanBase community".format(self.store_dir))

                else:
                    record.add_record("gather rootservice.log  by {0}".format(self.trace_id))
                    # rs
                    self.verbose("event_data is None")
                    self.verbose("gather rootservice.log  by {0}".format(self.trace_id))
                    work_path_rs = self.store_dir + "/{0}_on_rs/".format(self.trace_id)
                    self.gather_log.set_parameters("scope", "rootservice")
                    if self.input_parameters.get("since") is not None:
                        since = self.input_parameters.get("since")
                        self.gather_log.set_parameters("since", since)
                    self.gather_log.grep("{0}".format(self.trace_id))
                    logs_name = self.gather_log.execute(save_path=work_path_rs)
                    if logs_name is None or len(logs_name) <= 0:
                        self.verbose("no log_disk_full about trace_id:{0}".format(self.trace_id))
                        return False
                    record.add_record("Log saving location：{0}".format(work_path_rs))
                    record.add_suggest("The index creation failed during the other phase. Please upload {0} to the OceanBase community".format(self.store_dir))

            self.Result.records.append(record)
        except Exception as e:
            raise RCAExecuteException("IndexDDLErrorScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end IndexDDLErrorScene execute")

    def export_result(self):
        super().export_result()

    def get_scene_info(self):
        return {
            "name": "index_ddl_error",
            "info_en": "Troubleshooting errors in indexing execution. ",
            "info_cn": '建索引执行报错问题排查',
        }


index_ddl_error = IndexDDLErrorScene()
