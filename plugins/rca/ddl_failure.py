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
@time: 2024/06/12
@file: ddl_failure_scene.py
@desc:
"""
import os

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils


class DDLFailureScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.tablet_id = None
        self.tenant_id = None
        self.work_path = None
        self.index_table_id = None
        self.estimated_size = None
        self.estimated_data_size = None

    def verbose(self, info):
        self.stdio.verbose("[DDLFailureScene] {0}".format(info))

    def get_scene_info(self):
        return {
            "name": "ddl_failure",
            "info_en": "diagnose ddl failure",
            "info_cn": '诊断ddl失败',
        }

    def init(self, context):
        super().init(context)
        min_supported_version = "4.2.1.0"
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (StringUtils.compare_versions_greater(observer_version, min_supported_version)):
            self.stdio.error("observer version is {0}, which is less than {1}.".format(observer_version, min_supported_version))
            raise RCAInitException("observer version is {0}, which is less than {1}.".format(observer_version, min_supported_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.work_path = context.get_variable('store_dir')
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        result = RCA_ResultRecord(stdio=self.stdio)
        # get target info from input parameters
        self.tenant_id = self.input_parameters.get("tenant_id")
        self.table_id = self.input_parameters.get("table_id")
        self.tablet_id = self.input_parameters.get("tablet_id")
        result.add_record("input parameters: tenant_id: {0}, table_id: {1}, tablet_id: {2}".format(self.tenant_id, self.table_id, self.tablet_id))
        try:
            self.tenant_id = int(self.tenant_id)
        except Exception as e:
            raise RCAInitException("tenant_id is not a valid number, please check the env")
        try:
            self.table_id = int(self.table_id)
        except Exception as e:
            raise RCAInitException("table_id is not a valid number, please check the env")
        try:
            self.tablet_id = int(self.tablet_id)
        except Exception as e:
            raise RCAInitException("tablet_id is not a valid number, please check the env")

        # get table_id from tablet_id if tablet_id is not given
        if self.table_id is None or self.table_id <= 0:
            if self.tablet_id is None or self.tablet_id <= 0:
                raise RCAInitException("input parameters are wrong, both table_id and tablet_id is empty")
            else:
                sql = "select * from oceanbase.__all_virtual_tablet_to_table_history where tenant_id={0} and tablet_id = {1} and is_deleted = 0".format(self.tenant_id, self.tablet_id)
                sql_result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                self.verbose("query sql: {0}\nsql result: {1}".format(sql, sql_result))
                if len(sql_result) <= 0:
                    raise RCAInitException("table_id not found via tablet_id, query sql: {0}".format(sql))
                else:
                    self.table_id = sql_result[0]["table_id"]
        if self.table_id is None or self.table_id <= 0:
            raise RCAInitException("table_id is empty")
        # get data_table_id if not index
        sql = "select * from oceanbase.__all_virtual_table_history where tenant_id={0} and table_id={1} and is_deleted = 0 limit 1".format(self.tenant_id, self.table_id)
        sql_result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
        self.verbose("query sql: {0}\nsql result: {1}".format(sql, sql_result))
        if len(sql_result) <= 0:
            raise RCAInitException("table not found, query sql: {0}".format(sql))
        else:
            if sql_result[0]["index_type"] > 0 or sql_result[0]["data_table_id"] == 0:
                pass
            else:
                self.table_id = sql_result[0]["data_table_id"]
        result.add_record("diagnose use parameters: tenant_id: {0}, table_id: {1}, tablet_id {2}".format(self.tenant_id, self.table_id, self.tablet_id))
        self.Result.records.append(result)

    def execute(self):
        result = RCA_ResultRecord(self.stdio)
        try:
            sql = "select * from oceanbase.__all_virtual_ddl_error_message where tenant_id={0} and object_id={1} and target_object_id=-1 order by gmt_create desc limit 1".format(self.tenant_id, self.table_id)
            sql_result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("query sql: {0}\nsql result: {1}".format(sql, sql_result))
            if len(sql_result) <= 0:
                result.add_record("ddl error message is empty, query sql: {0}".format(sql))
            else:
                error_messg = sql_result[0]
                self.ddl_task_id = error_messg["task_id"]
                self.ret_code = error_messg["ret_code"]
                self.trace_id = error_messg["trace_id"]
                result.add_record("ddl_task_id: {0}, ret_code: {1}, trace_id: {2}".format(self.ddl_task_id, self.ret_code, self.trace_id))
                if int(self.ret_code) == 0:
                    result.add_suggest("the ddl is success, no need to diagnose, query sql: {0}".format(sql))
                else:
                    sql = "select * from oceanbase.__all_rootservice_event_history where module like 'ddl_scheduler' and value1 like '{0}' and value3 like '{1}' and (event like 'switch_state' or value2 like '-%')".format(self.tenant_id, self.trace_id)
                    sql_result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                    self.verbose("query sql: {0}\nsql result: {1}".format(sql, sql_result))
                    for ddl_event in sql_result:
                        if ddl_event["event"] == "switch_state":
                            result.add_record("[{0}] {1}, switch_state: {2}, ret:{3}, rs_svr:{4}:{5}".format(ddl_event["gmt_create"], ddl_event["value5"], ddl_event["extra_info"], ddl_event["value2"], ddl_event["rs_svr_ip"], ddl_event["rs_svr_port"]))
                        else:
                            result.add_record(
                                "[{0}] {1}, ret:{2}, {3}:{4}, {5}:{6}, rs_svr:{7}:{8}".format(
                                    ddl_event["gmt_create"], ddl_event["event"], ddl_event["value2"], ddl_event["name5"], ddl_event["value5"], ddl_event["name6"], ddl_event["value6"], ddl_event["rs_svr_ip"], ddl_event["rs_svr_port"]
                                )
                            )

        except RCANotNeedExecuteException as e:
            self.stdio.print("[Not Need Execute]DDLFailureScene need not execute: {0}".format(e))
        except Exception as e:
            raise RCAExecuteException("DDLFailureScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end DDLFailureScene execute")
        self.Result.records.append(result)


ddl_failure = DDLFailureScene()
