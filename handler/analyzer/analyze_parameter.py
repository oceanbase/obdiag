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
@time: 2024/6/16
@file: analyze_parameter.py
@desc:
"""
import os
from common.command import get_observer_version_by_sql
from common.tool import DirectoryUtil, TimeUtils, Util, StringUtils
from common.obdiag_exception import OBDIAGFormatException
from common.ob_connector import OBConnector
import csv
from prettytable import PrettyTable
import json
import datetime
from colorama import Fore, Style


class AnalyzeParameterHandler(object):
    def __init__(self, context, analyze_type='default'):
        self.context = context
        self.stdio = self.context.stdio
        self.export_report_path = None
        self.parameter_file_name = None
        self.ob_cluster = self.context.cluster_config
        self.analyze_type = analyze_type
        if self.context.get_variable("gather_timestamp", None):
            self.analyze_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.analyze_timestamp = TimeUtils.get_current_us_timestamp()
        self.observer_nodes = self.context.cluster_config.get("servers")
        try:
            self.obconn = OBConnector(
                ip=self.ob_cluster.get("db_host"),
                port=self.ob_cluster.get("db_port"),
                username=self.ob_cluster.get("tenant_sys").get("user"),
                password=self.ob_cluster.get("tenant_sys").get("password"),
                stdio=self.stdio,
                timeout=10000,
                database="oceanbase",
            )
        except Exception as e:
            self.stdio.error("Failed to connect to database: {0}".format(e))
            raise OBDIAGFormatException("Failed to connect to database: {0}".format(e))

    def get_version(self):
        observer_version = ""
        try:
            observer_version = get_observer_version_by_sql(self.ob_cluster, self.stdio)
        except Exception as e:
            self.stdio.warn("failed to get observer version:{0}".format(e))
        self.stdio.verbose("get observer version: {0}".format(observer_version))
        return observer_version

    def handle(self):
        if self.analyze_type == 'default':
            if not self.init_option_default():
                self.stdio.error('init option failed')
                return False
        else:
            if not self.init_option_diff():
                self.stdio.error('init option failed')
                return False
        self.stdio.verbose("Use {0} as pack dir.".format(self.export_report_path))
        DirectoryUtil.mkdir(path=self.export_report_path, stdio=self.stdio)
        self.execute()

    def check_file_valid(self):
        with open(self.parameter_file_name, 'r') as f:
            header = f.readline()
            flag = 1
            if header:
                header = header.strip()
            if not header:
                flag = 0
            if not header.startswith('VERSION'):
                flag = 0
            if not header.endswith('ISDEFAULT'):
                flag = 0
            if flag == 0:
                self.stdio.error('args --file [{0}] is not a valid parameter file, Please specify it again'.format(os.path.abspath(self.parameter_file_name)))
                exit(-1)

    def init_option_default(self):
        options = self.context.options
        store_dir_option = Util.get_option(options, 'store_dir')
        offline_file_option = Util.get_option(options, 'file')
        if store_dir_option and store_dir_option != "./":
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.export_report_path = os.path.abspath(store_dir_option)
        else:
            store_dir_option = './parameter_report'
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('The report directory is not specified, and a "parameter_report" directory will be created in the current directory.'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.export_report_path = os.path.abspath(store_dir_option)
        if offline_file_option:
            if not os.path.exists(os.path.abspath(offline_file_option)):
                self.stdio.error('args --file [{0}] not exist: No such file, Please specify it again'.format(os.path.abspath(offline_file_option)))
                exit(-1)
            else:
                self.parameter_file_name = os.path.abspath(offline_file_option)
                self.check_file_valid()
        return True

    def init_option_diff(self):
        options = self.context.options
        store_dir_option = Util.get_option(options, 'store_dir')
        offline_file_option = Util.get_option(options, 'file')
        if store_dir_option and store_dir_option != "./":
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.export_report_path = os.path.abspath(store_dir_option)
        else:
            store_dir_option = './parameter_report'
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('The report directory is not specified, and a "parameter_report" directory will be created in the current directory.'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.export_report_path = os.path.abspath(store_dir_option)

        if offline_file_option:
            if not os.path.exists(os.path.abspath(offline_file_option)):
                self.stdio.error('args --file [{0}] not exist: No such file, Please specify it again'.format(os.path.abspath(offline_file_option)))
                return False
            else:
                self.parameter_file_name = os.path.abspath(offline_file_option)
                self.check_file_valid()
        return True

    def analyze_parameter_default(self):
        observer_version = self.get_version()
        if StringUtils.compare_versions_greater(observer_version, "4.2.2.0"):
            if self.parameter_file_name is not None:
                self.stdio.warn("the version of OceanBase is greater than 4.2.2, an initialization parameter file will be ignored")
            sql = '''select substr(version(),8), svr_ip,svr_port,zone,scope,TENANT_ID,name,value,section,
EDIT_LEVEL, now(),default_value,isdefault from GV$OB_PARAMETERS where isdefault='NO' order by 5,2,3,4,7'''
            parameter_info = self.obconn.execute_sql(sql)
            report_default_tb = PrettyTable(["IP", "PORT", "ZONE", "CLUSTER", "TENANT_ID", "NAME", "DEFAULT_VALUE", "CURRENT_VALUE"])
            now = datetime.datetime.now()
            date_format = now.strftime("%Y-%m-%d-%H-%M-%S")
            file_name = self.export_report_path + '/parameter_default_{0}.table'.format(date_format)
            fp = open(file_name, 'a+', encoding="utf8")
            for row in parameter_info:
                if row[5] is None:
                    tenant_id = 'None'
                else:
                    tenant_id = row[5]
                report_default_tb.add_row([row[1], row[2], row[3], row[4], tenant_id, row[6], row[11], row[7]])
            fp.write(report_default_tb.get_string() + "\n")
            self.stdio.print(report_default_tb.get_string())
            self.stdio.print("Analyze parameter default finished. For more details, please run cmd '" + Fore.YELLOW + " cat {0} ".format(file_name) + Style.RESET_ALL + "'")
        else:
            if self.parameter_file_name is None:
                self.stdio.error("the version of OceanBase is lower than 4.2.2, an initialization parameter file must be provided to find non-default values")
                exit(-1)
            else:
                sql = '''select substr(version(),8), svr_ip,svr_port,zone,scope,TENANT_ID,name,value,section,
EDIT_LEVEL, now(),'','' from GV$OB_PARAMETERS order by 5,2,3,4,7'''
                db_parameter_info = self.obconn.execute_sql(sql)
                db_parameter_dict = dict()
                for row in db_parameter_info:
                    key = str(row[1]) + '-' + str(row[2]) + '-' + str(row[3]) + '-' + str(row[4]) + '-' + str(row[5]) + '-' + str(row[6])
                    value = row[7]
                    db_parameter_dict[key] = value
                file_parameter_dict = dict()
                with open(self.parameter_file_name, 'r', newline='') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        if row[0] == 'VERSION':
                            continue
                        key = str(row[1]) + '-' + str(row[2]) + '-' + str(row[3]) + '-' + str(row[4]) + '-' + str(row[5]) + '-' + str(row[6])
                        value = row[7]
                        file_parameter_dict[key] = value
                report_default_tb = PrettyTable(["IP", "PORT", "ZONE", "CLUSTER", "TENANT_ID", "NAME", "DEFAULT_VALUE", "CURRENT_VALUE"])
                now = datetime.datetime.now()
                date_format = now.strftime("%Y-%m-%d-%H-%M-%S")
                file_name = self.export_report_path + '/parameter_default_{0}.table'.format(date_format)
                fp = open(file_name, 'a+', encoding="utf8")
                is_empty = True
                for key in db_parameter_dict:
                    if key in file_parameter_dict and db_parameter_dict[key] != file_parameter_dict[key]:
                        col_list = key.split('-')
                        report_default_tb.add_row([col_list[0], col_list[0], col_list[2], col_list[3], col_list[4], col_list[5], file_parameter_dict[key], db_parameter_dict[key]])
                        is_empty = False
                fp.write(report_default_tb.get_string() + "\n")
                if not is_empty:
                    self.stdio.print(report_default_tb.get_string())
                    self.stdio.print("Analyze parameter default finished. For more details, please run cmd '" + Fore.YELLOW + " cat {0} ".format(file_name) + Style.RESET_ALL + "'")
                else:
                    self.stdio.print("Analyze parameter default finished. All parameter values are the same as the default values.")

    def alalyze_parameter_diff(self):
        if self.parameter_file_name is None:
            sql = '''select substr(version(),8), svr_ip,svr_port,zone,scope,TENANT_ID,name,value,section,
EDIT_LEVEL, now(),'','' from GV$OB_PARAMETERS order by 5,2,3,4,7'''
            parameter_info = self.obconn.execute_sql(sql)
        else:
            parameter_info = []
            with open(self.parameter_file_name, 'r', newline='') as file:
                reader = csv.reader(file)
                for row in reader:
                    if row[0] == 'VERSION':
                        continue
                    parameter_info.append(row)
        tenants_dict = dict()
        for row in parameter_info:
            if row[5] is None:
                scope = 'CLUSTER'
            else:
                scope = row[5]
            tenant_id = str(scope)
            observer = str(row[1]) + ':' + str(row[2])
            name = row[6]
            value = row[7]
            if tenant_id not in tenants_dict:
                tenants_dict[tenant_id] = []
                tenants_dict[tenant_id].append({'observer': observer, 'name': name, 'value': value})
            else:
                tenants_dict[tenant_id].append({'observer': observer, 'name': name, 'value': value})
        diff_parameter_dict = dict()
        for tenant, parameters_list in tenants_dict.items():
            diff_parameter_dict[tenant] = []
            parameter_dict = dict()
            for parameter_info in parameters_list:
                name = parameter_info['name']
                observer = parameter_info['observer']
                value = parameter_info['value']
                if name not in parameter_dict:
                    parameter_dict[name] = []
                    parameter_dict[name].append({'observer': observer, 'value': value})
                else:
                    parameter_dict[name].append({'observer': observer, 'value': value})

            for name, value_list in parameter_dict.items():
                if name in ['local_ip', 'observer_id', 'zone']:
                    continue
                value_set = set()
                for value_info in value_list:
                    value_set.add(value_info['value'])
                if len(value_set) > 1:
                    diff_parameter_dict[tenant].append({'name': name, 'value_list': value_list})
        now = datetime.datetime.now()
        date_format = now.strftime("%Y-%m-%d-%H-%M-%S")
        file_name = self.export_report_path + '/parameter_diff_{0}.table'.format(date_format)
        fp = open(file_name, 'a+', encoding="utf8")
        is_empty = True
        for tenant, value_list in diff_parameter_dict.items():
            if len(value_list) > 0:
                report_diff_tb = PrettyTable(["name", "diff"])
                report_diff_tb.align["task_report"] = "l"
                if tenant == 'CLUSTER':
                    report_diff_tb.title = 'SCOPE:' + tenant
                else:
                    report_diff_tb.title = 'SCOPE:TENANT-' + tenant
                for value_dict in value_list:
                    value_str_list = []
                    for value in value_dict['value_list']:
                        value_str = json.dumps(value)
                        value_str_list.append(value_str)
                    report_diff_tb.add_row([value_dict['name'], '\n'.join(value_str_list)])
                fp.write(report_diff_tb.get_string() + "\n")
                self.stdio.print(report_diff_tb.get_string())
                is_empty = False
        fp.close()
        if not is_empty:
            self.stdio.print("Analyze parameter diff finished. For more details, please run cmd '" + Fore.YELLOW + " cat {0} ".format(file_name) + Style.RESET_ALL + "'")
        else:
            self.stdio.print("Analyze parameter diff finished. All parameter settings are consistent among observers")

    def execute(self):
        try:
            if self.analyze_type == 'default':
                self.analyze_parameter_default()
            elif self.analyze_type == 'diff':
                self.alalyze_parameter_diff()
        except Exception as e:
            self.stdio.error("parameter info analyze failed, error message: {0}".format(e))
