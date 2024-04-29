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
@time: 2024/4/28
@file: gather_ash_report.py
@desc:
"""
import datetime
import os

from common.ob_connector import OBConnector
from common.obdiag_exception import OBDIAGFormatException, OBDIAGException
from common.tool import DirectoryUtil, TimeUtils, Util
from stdio import SafeStdio
from colorama import Fore, Style


class GatherAshReportHandler(SafeStdio):
    def __init__(self, context, gather_pack_dir='./'):
        super().__init__()
        self.report_type = None
        self.wait_class = None
        self.sql_id = None
        self.ash_report_file_name = None
        self.from_time_str = None
        self.to_time_str = None
        self.ash_sql = None
        self.trace_id = None
        self.context = context
        self.stdio = self.context.stdio
        self.gather_pack_dir = gather_pack_dir
        if self.context.get_variable("gather_timestamp", None) :
            self.gather_timestamp=self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()
        self.cluster = self.context.cluster_config
        try:
            self.obconn = OBConnector(
                        ip=self.cluster.get("db_host"),
                        port=self.cluster.get("db_port"),
                        username=self.cluster.get("tenant_sys").get("user"),
                        password=self.cluster.get("tenant_sys").get("password"),
                        stdio=self.stdio,
                        timeout=10000
                    )
        except Exception as e:
            self.stdio.error("Failed to connect to database: {0}".format(e))
            raise OBDIAGFormatException("Failed to connect to database: {0}".format(e))


    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        self.__init_variables()
        self.__init_report_path()
        self.__init_task_names()
        self.execute()
        self.__print_result()

    def execute(self):
        try:
            self.stdio.verbose("execute_tasks. the number of tasks is {0} ,tasks is {1}".format(len(self.yaml_tasks.keys()), self.yaml_tasks.keys()))

        except Exception as e:
            self.stdio.error("Internal error :{0}".format(e))


    def __init_report_path(self):
        try:
            self.report_path = os.path.join(self.gather_pack_dir, "gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp), self.stdio))
            self.stdio.verbose("Use {0} as pack dir.".format(self.report_path))
            DirectoryUtil.mkdir(path=self.report_path, stdio=self.stdio)
        except Exception as e:
            self.stdio.error("init_report_path failed, error:{0}".format(e))

    def __init_variables(self):
        try:
            self.variables = {
                "observer_data_dir": self.ob_nodes[0].get("home_path") if self.ob_nodes and self.ob_nodes[0].get("home_path") else "",
                "obproxy_data_dir": self.obproxy_nodes[0].get("home_path") if self.obproxy_nodes and self.obproxy_nodes[0].get("home_path") else "",
                "from_time": self.from_time_str,
                "to_time": self.to_time_str
            }
            self.stdio.verbose("gather scene variables: {0}".format(self.variables))
        except Exception as e:
            self.stdio.error("init gather scene variables failed, error: {0}".format(e))


    def init_option(self):
        options = self.context.options
        from_option = Util.get_option(options, 'from')
        to_option = Util.get_option(options, 'to')
        store_dir_option = Util.get_option(options, 'store_dir',"./")
        trace_id_option = Util.get_option(options, 'trace_id')
        sql_id_option = Util.get_option(options, 'sql_id')
        report_type_option = Util.get_option(options, 'report_type')
        wait_class_option = Util.get_option(options, 'wait_class')

        if from_option is not None and to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(from_option)
                to_timestamp = TimeUtils.parse_time_str(to_option)
                self.from_time_str = from_option
                self.to_time_str = to_option
            except OBDIAGFormatException:
                self.stdio.exception('Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. from_datetime={0}, to_datetime={1}'.format(from_option, to_option))
                return False
            if to_timestamp <= from_timestamp:
                self.stdio.exception('Error: from datetime is larger than to datetime, please check.')
                return False
        elif (from_option is None or to_option is None) and since_option is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.warn('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('warn: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        self.ash_sql="CALL DBMS_WORKLOAD_REPOSITORY.ASH_REPORT(  '{0}',  '{1}'".format(self.from_time_str, self.to_time_str)
        if sql_id_option:
            self.sql_id = sql_id_option
            self.ash_sql = self.ash_sql + ", '{0}'".format(self.sql_id)
        else:
            self.ash_sql = self.ash_sql + ", NULL"
        if trace_id_option:
            self.trace_id = trace_id_option
            self.ash_sql = self.ash_sql + ", '{0}'".format(self.trace_id)
        else:
            self.ash_sql = self.ash_sql + ", NULL"
        if wait_class_option:
            self.wait_class = wait_class_option
            self.ash_sql = self.ash_sql + ", '{0}'".format(self.wait_class)
        else:
            self.ash_sql = self.ash_sql + ", NULL"
        if report_type_option:
            self.report_type = report_type_option
            self.ash_sql = self.ash_sql + ", '{0}'".format(self.report_type)
        else:
            self.ash_sql = self.ash_sql + ", NULL"

        try:
            self.ash_sql = self.ash_sql + ");"
            self.stdio.verbose("ash_sql: {0}".format(self.ash_sql))

            ash_report_data=self.obconn.execute_sql(self.ash_sql)
            if not ash_report_data or len(ash_report_data)==0:
                self.stdio.error("ash report data is empty")
                raise OBDIAGException("ash report data is empty")
            ash_report=ash_report_data[0]

            # save ash_report_data
            self.ash_report_file_name="ash_report_{0}.txt".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp))

            with open(self.report_path + "/"+self.ash_report_file_name, 'w') as f:
                f.write(ash_report)
        except Exception as e:
            self.stdio.error("ash report gather failed, error message: {0}".format(e))
            return False

        return True

    def __print_result(self):
        self.stdio.print(Fore.YELLOW + "\nGather scene results stored in this directory: {0}\n".format(self.report_path + "/"+self.ash_report_file_name) + Style.RESET_ALL)





