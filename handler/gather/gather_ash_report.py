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

from common.command import get_observer_version, get_observer_version_by_sql
from common.ob_connector import OBConnector
from common.obdiag_exception import OBDIAGFormatException, OBDIAGException
from common.ssh import SshHelper
from common.tool import DirectoryUtil, TimeUtils, Util, StringUtils
from stdio import SafeStdio
from colorama import Fore, Style


class GatherAshReportHandler(SafeStdio):
    def __init__(self, context, gather_pack_dir='./'):
        super().__init__()
        self.result_summary_file_name = None
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
        self.ob_cluster = self.context.cluster_config
        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()
        self.cluster = self.context.cluster_config

        self.observer_nodes = self.context.cluster_config.get("servers")
        try:
            self.obconn = OBConnector(
                ip=self.cluster.get("db_host"), port=self.cluster.get("db_port"), username=self.cluster.get("tenant_sys").get("user"), password=self.cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=10000, database="oceanbase"
            )
        except Exception as e:
            self.stdio.error("Failed to connect to database: {0}".format(e))
            raise OBDIAGFormatException("Failed to connect to database: {0}".format(e))

    def handle(self):
        if not self.version_check():
            self.stdio.error('version check failed')
            return False
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        self.__init_report_path()
        self.execute()
        self.__print_result()

    def version_check(self):
        observer_version = ""
        try:
            observer_version = get_observer_version_by_sql(self.ob_cluster, self.stdio)
        except Exception as e:
            if len(self.observer_nodes) > 0:
                ssher = SshHelper(self.observer_nodes[0]["ip"], self.observer_nodes[0]["ssh_port"], self.observer_nodes[0]["ssh_username"], self.observer_nodes[0]["ssh_password"])
                observer_version = get_observer_version(True, ssher, self.observer_nodes[0]["home_path"], self.stdio)
            else:
                self.stdio.warn("RCAHandler Failed to get observer version:{0}".format(e))
        self.stdio.verbose("RCAHandler.init get observer version: {0}".format(observer_version))

        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            self.stdio.error("observer version: {0}, must greater than 4.0.0.0".format(observer_version))
            return False
        return True

    def execute(self):
        try:
            ash_report_arg = (self.from_time_str, self.to_time_str, self.sql_id, self.trace_id, self.wait_class, self.report_type)
            self.stdio.verbose("ash report arg: {0}".format(ash_report_arg))
            ash_report_data = self.obconn.callproc("DBMS_WORKLOAD_REPOSITORY.ASH_REPORT", args=ash_report_arg)
            if not ash_report_data or len(ash_report_data) == 0:
                self.stdio.error("ash report data is empty")
                raise OBDIAGException("ash report data is empty")
            ash_report = ash_report_data[0][0]
            if len(ash_report) > 1:
                self.stdio.verbose("ash report: \n{0}".format(ash_report))
            else:
                raise OBDIAGException("ash report data is empty")

            # save ash_report_data
            self.ash_report_file_name = "ash_report_{0}.txt".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp))
            self.ash_report_file_name = os.path.join(self.report_path, self.ash_report_file_name)

            with open(self.ash_report_file_name, 'w+') as f:
                f.write(ash_report)
            self.stdio.print("save ash report file name: " + Fore.YELLOW + "{0}".format(self.ash_report_file_name) + Style.RESET_ALL)
            self.result_summary_file_name = os.path.join(self.report_path, "result_summary.txt")
            with open(self.result_summary_file_name, 'w+') as f:
                f.write(self.ash_report_file_name)

        except Exception as e:
            self.stdio.error("ash report gather failed, error message: {0}".format(e))

    def __init_report_path(self):
        try:
            self.report_path = os.path.join(self.gather_pack_dir, "gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp), self.stdio))
            self.stdio.verbose("Use {0} as pack dir.".format(self.report_path))
            DirectoryUtil.mkdir(path=self.report_path, stdio=self.stdio)
        except Exception as e:
            self.stdio.error("init_report_path failed, error:{0}".format(e))

    def init_option(self):
        options = self.context.options
        from_option = Util.get_option(options, 'from')
        to_option = Util.get_option(options, 'to')
        trace_id_option = Util.get_option(options, 'trace_id')
        sql_id_option = Util.get_option(options, 'sql_id')
        report_type_option = Util.get_option(options, 'report_type')
        wait_class_option = Util.get_option(options, 'wait_class')
        store_dir_option = Util.get_option(options, 'store_dir')

        since_option = "30m"
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
        elif from_option is None or to_option is None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=0)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.warn('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('warn: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        if sql_id_option:
            self.sql_id = sql_id_option
        else:
            self.sql_id = None
        if trace_id_option:
            self.trace_id = trace_id_option
        else:
            self.trace_id = None

        if report_type_option:
            self.report_type = report_type_option.strip()
            if report_type_option.upper() != "TEXT":
                self.stdio.error("Invalid argument for report type, Now just support TEXT")
                return False
        else:
            self.report_type = None
        if wait_class_option:
            self.wait_class = wait_class_option
        else:
            self.wait_class = None
        if store_dir_option:
            self.gather_pack_dir = store_dir_option
        else:
            self.gather_pack_dir = "./"
        self.stdio.print(
            "from_time: {0}, to_time: {1}, sql_id: {2}, trace_id: {3}, report_type: {4}, wait_class: {5}, store_dir: {6}".format(self.from_time_str, self.to_time_str, self.sql_id, self.trace_id, self.report_type, self.wait_class, self.gather_pack_dir)
        )

        return True

    def __print_result(self):
        self.stdio.print(Fore.YELLOW + "\nGather ash_report results stored in this directory: {0}".format(self.report_path) + Style.RESET_ALL)
        self.stdio.print("")
