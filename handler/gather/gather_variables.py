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
@file: gather_variables.py
@desc:
"""
import os
from common.tool import DirectoryUtil, TimeUtils, Util
from common.obdiag_exception import OBDIAGFormatException
from common.ob_connector import OBConnector
import csv
from colorama import Fore, Style


class GatherVariablesHandler(object):
    def __init__(self, context, gather_pack_dir='./'):
        self.context = context
        self.stdio = self.context.stdio
        self.gather_pack_dir = gather_pack_dir
        self.variable_file_name = None
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
        pack_dir_this_command = os.path.join(self.gather_pack_dir, "gather_variables")
        self.stdio.verbose("Use {0} as pack dir.".format(pack_dir_this_command))
        DirectoryUtil.mkdir(path=pack_dir_this_command, stdio=self.stdio)
        self.gather_pack_dir = pack_dir_this_command
        self.execute()

    def init_option(self):
        options = self.context.options
        store_dir_option = Util.get_option(options, 'store_dir')
        if store_dir_option and store_dir_option != "./":
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('warn: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
        self.gather_pack_dir = os.path.abspath(store_dir_option)
        return True

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

    def get_variables_info(self):
        cluster_name = self.get_cluster_name()
        sql = '''select version(), tenant_id, zone, name,gmt_modified, value, flags, min_val, max_val, now() 
 from oceanbase.__all_virtual_sys_variable order by 2, 4, 5'''
        variable_info = self.obconn.execute_sql(sql)
        self.variable_file_name = self.gather_pack_dir + '/{0}_variables_{1}.csv'.format(cluster_name, TimeUtils.timestamp_to_filename_time(self.gather_timestamp))
        header = ['VERSION', 'TENANT_ID', 'ZONE', 'NAME', 'GMT_MODIFIED', 'VALUE', 'FLAGS', 'MIN_VALUE', 'MAX_VALUE', 'RECORD_TIME']
        with open(self.variable_file_name, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for row in variable_info:
                writer.writerow(row)
        self.stdio.print("Gather variables finished. For more details, please run cmd '" + Fore.YELLOW + "cat {0}".format(self.variable_file_name) + Style.RESET_ALL + "'")

    def execute(self):
        try:
            self.get_variables_info()
        except Exception as e:
            self.stdio.error("parameter info gather failed, error message: {0}".format(e))
