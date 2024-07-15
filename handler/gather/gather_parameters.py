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
@file: gather_parameters.py
@desc:
"""
import os
from common.command import get_observer_version_by_sql
from common.tool import DirectoryUtil, TimeUtils, Util, StringUtils
from common.obdiag_exception import OBDIAGFormatException
from common.ob_connector import OBConnector
import csv
from colorama import Fore, Style


class GatherParametersHandler(object):
    def __init__(self, context, gather_pack_dir='./'):
        self.context = context
        self.stdio = self.context.stdio
        self.gather_pack_dir = gather_pack_dir
        self.parameter_file_name = None
        self.ob_cluster = self.context.cluster_config
        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()
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

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        # example of the format of pack dir for this command: (gather_pack_dir)/gather_pack_20190610123344
        pack_dir_this_command = os.path.join(self.gather_pack_dir, "gather_parameters")
        self.stdio.verbose("Use {0} as pack dir.".format(pack_dir_this_command))
        DirectoryUtil.mkdir(path=pack_dir_this_command, stdio=self.stdio)
        self.gather_pack_dir = pack_dir_this_command
        self.execute()

    def init_option(self):
        options = self.context.options
        store_dir_option = Util.get_option(options, 'store_dir')
        if store_dir_option and store_dir_option != "./":
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('warn: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(
                    os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
        self.gather_pack_dir = os.path.abspath(store_dir_option)
        return True

    def get_version(self):
        observer_version = ""
        try:
            observer_version = get_observer_version_by_sql(self.ob_cluster, self.stdio)
        except Exception as e:
            self.stdio.warn("GatherHandler Failed to get observer version:{0}".format(e))
        self.stdio.verbose("GatherHandler.init get observer version: {0}".format(observer_version))
        return observer_version

    def get_cluster_name(self):
        cluster_name = ""
        try:
            sql = '''select value from oceanbase.__all_virtual_tenant_parameter_stat t2 where name = 'cluster' '''
            cluster_info = self.obconn.execute_sql(sql)
            cluster_name = cluster_info[0][0]
        except Exception as e:
            self.stdio.warn("RCAHandler Failed to get oceanbase cluster name:{0}".format(e))
        self.stdio.verbose("RCAHandler.init get oceanbase cluster name {0}".format(cluster_name))
        return cluster_name

    def get_parameters_info(self):
        observer_version = self.get_version()
        cluster_name = self.get_cluster_name()
        if observer_version:
            if StringUtils.compare_versions_greater(observer_version, "4.2.2.0"):
                sql = '''select substr(version(),8), svr_ip,svr_port,zone,scope,TENANT_ID,name,value,section,
EDIT_LEVEL, now(), DEFAULT_VALUE,ISDEFAULT from GV$OB_PARAMETERS order by 5,2,3,4,7'''
            elif StringUtils.compare_versions_greater(observer_version, "4.0.0.0"):
                sql = '''select substr(version(),8), svr_ip,svr_port,zone,scope,TENANT_ID,name,value,section,
                EDIT_LEVEL, now(), '','' from GV$OB_PARAMETERS order by 5,2,3,4,7'''
            else:
                sql = '''select version(), svr_ip,svr_port,zone,scope,TENANT_ID,name,value,section,
                EDIT_LEVEL, now(), '','' from oceanbase.__all_virtual_tenant_parameter_info
union
select version(), svr_ip,svr_port,zone,scope,'None' tenant_id,name,value,section,
                EDIT_LEVEL, now(), '','' from oceanbase.__all_virtual_sys_parameter_stat where scope='CLUSTER' 
'''
            parameter_info = self.obconn.execute_sql(sql)
            self.parameter_file_name = self.gather_pack_dir + '/{0}_parameters_{1}.csv'.format(cluster_name,
                                                                                               TimeUtils.timestamp_to_filename_time(
                                                                                                   self.gather_timestamp))
            header = ['VERSION', 'SVR_IP', 'SVR_PORT', 'ZONE', 'SCOPE', 'TENANT_ID', 'NAME', 'VALUE', 'SECTION',
                      'EDIT_LEVEL', 'RECORD_TIME', 'DEFAULT_VALUE', 'ISDEFAULT']
            with open(self.parameter_file_name, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(header)
                for row in parameter_info:
                    if row[5] is None:
                        tmp_row = [col for col in row]
                        tmp_row[5] = 'None'
                        writer.writerow(tmp_row)
                    else:
                        writer.writerow(row)
            self.stdio.print(
                "Gather parameters finished. For more details, please run cmd '" + Fore.YELLOW + "cat {0}".format(
                    self.parameter_file_name) + Style.RESET_ALL + "'")
        else:
            self.stdio.warn(
                "Failed to retrieve the database version. Please check if the database connection is normal.")

    def execute(self):
        try:
            self.get_parameters_info()
        except Exception as e:
            self.stdio.error("parameter info gather failed, error message: {0}".format(e))
