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
@time: 2024/05/20
@file: transaction_execute_timeout_scene.py
@desc:
"""
import datetime
import os
import re

from handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
    RCANotNeedExecuteException,
)
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import StringUtils


class TransactionExecuteTimeoutScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.tenant_id = None
        self.err_type = None
        self.time_tag = None
        self.work_path = self.store_dir
        self.trans_is_killed_log = None

    def init(self, context):
        super().init(context)
        ## observer version>4.0.0.0
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            self.stdio.error("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
            raise RCAInitException("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.work_path = context.get_variable("store_dir")
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        self.err_type = self.input_parameters.get('err_type')
        if self.err_type is None or len(self.err_type.strip()) == 0:
            raise RCAInitException("err_type is None. Please check the --input_parameters.")
        self.err_type = self.err_type.strip().lower()
        if self.err_type not in ["statement is timeout", "transaction is timeout"]:
            raise RCAInitException("err_type is {0}. It should be statement is timeout or transaction is timeout.".format(self.err_type))
        self.record.add_record("err_type is {0}".format(self.err_type))

    def verbose(self, info):
        self.stdio.verbose("[TransactionExecuteTimeoutScene] {0}".format(info))

    def execute(self):
        try:
            self.record = RCA_ResultRecord(self.stdio)
            if self.err_type == "statement is timeout":
                self.verbose("start to check backlog")
                self.execute_statement()
            elif self.err_type == "transaction is timeout":
                self.verbose("start to check transaction is timeout")
                self.execute_transaction()

        except Exception as e:
            raise RCAExecuteException("TransactionExecuteTimeoutScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end TransactionExecuteTimeoutScene execute")
            self.Result.records.append(self.record)

    def execute_statement(self):
        # get ob_query_timeout
        tenant_id = self.input_parameters.get('tenant_id')
        if tenant_id is None or len(self.tenant_id.strip()) == 0:
            raise RCAInitException("tenant_id is None. 'statement_is_timeout_scene' need it. Please check the --input_parameters.")
        self.record.add_record("tenant_id is {0}".format(self.tenant_id))
        ob_query_timeout_cursor = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.CDB_OB_SYS_VARIABLES where tenant_id='{0}' and NAME = \"ob_query_timeout\";".format(self.tenant_id))
        ob_query_timeout_data = ob_query_timeout_cursor.fetchall()
        ob_query_timeout = ob_query_timeout_data[0].get("VALUE")
        self.record.add_record("ob_query_timeout is {0}".format(ob_query_timeout))
        # get trace_id
        trace_id = self.input_parameters.get('trace_id')
        if trace_id is None or len(trace_id.strip()) == 0:
            raise RCAInitException("trace_id is None. 'statement_is_timeout_scene' need it. Please check the --input_parameters.")
        self.record.add_record("trace_id is {0}".format(trace_id))
        # gather log about trace_id and "cur_query_start_time"
        self.gather_log.grep("{0}".format(trace_id))
        work_path_trace_id = self.work_path + "/" + trace_id
        logs_name = self.gather_log.execute(save_path=work_path_trace_id)
        # found cur_query_start_time and timeout_timestamp
        cur_query_start_time = None
        timeout_timestamp = None
        for log_name in logs_name:
            if cur_query_start_time and timeout_timestamp:
                break
            with open(log_name, 'r', encoding='utf-8') as f:
                content = f.readlines()
                for line in content:
                    if "cur_query_start_time" in line:
                        match = re.search(r'cur_query_start_time=(\d+)', line)
                        if match:
                            cur_query_start_time = match.group(1)
                            self.record.add_record("cur_query_start_time is {0}".format(cur_query_start_time))
                    if "timeout_timestamp" in line:
                        match = re.search(r'timeout_timestamp=(\d+)', line)
                        if match:
                            report_timeout_time = match.group(1)
                            self.record.add_record("timeout_timestamp is {0}".format(report_timeout_time))
        if cur_query_start_time is None or timeout_timestamp is None:
            self.record.add_record("can not find cur_query_start_time or timeout_timestamp")
            return
        self.record.add_record("cur_query_start_time is {0}, timeout_timestamp is {1}".format(cur_query_start_time, timeout_timestamp))
        if int(timeout_timestamp or 0) - int(cur_query_start_time or 0) == int(ob_query_timeout or 0):
            self.record.add_record("timeout_timestamp - cur_query_start_time == ob_query_timeout")
            self.record.add_suggest("Meets expectations")
        else:
            self.record.add_record("timeout_timestamp - cur_query_start_time != ob_query_timeout")
            self.record.add_suggest("Not meet expectations")

    def execute_transaction(self):
        # gather log about "dump tenant"
        work_path_dump_tenant = self.work_path + "/dump_tenant"
        self.gather_log.grep("dump tenant")
        logs_name = self.gather_log.execute(save_path=work_path_dump_tenant)
        if logs_name is None or len(logs_name) <= 0:
            self.record.add_record("no log about 'dump tenant'")
            return False
        work_tag = False
        for log_name in logs_name:
            with open(log_name, 'r', encoding='utf-8') as f:
                content = f.readlines()
                if work_tag:
                    break
                for line in content:
                    if 'dump tenant' in line:
                        match = re.search(r'req_queue:total_size=(\d+)', line)
                        if match:
                            date_str = match.group(1)
                            if date_str is not "0":
                                # exist ,get tenant_id
                                match_tenant_id = re.search(r'tenant={id:(\d+)', line)
                                if match_tenant_id:
                                    work_tag = True
                                    tenant_id = match_tenant_id.group(1)
                                    self.record.add_record("dump tenant, tenant_id is {0},total_size is {1}".format(tenant_id, date_str))
                                    self.record.add_suggest("check dump tenant, tenant_id is {0},total_size is {1}".format(tenant_id, date_str))
                                break

    def get_scene_info(self):
        return {
            "name": "transaction_execute_timeout",
            "info_en": "transaction execute timeout error, error_code like -4012. Need input err_msg",
            "info_cn": "事务执行超时报错",
        }


transaction_execute_timeout = TransactionExecuteTimeoutScene()
