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
@time: 2025/04/8
@file: syslog_io_bandwidth_limit.py
@desc:
"""
from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class SyslogIoBandwidthLimit(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        self.expected_value = "30MB"  # expected_value
        self.param_name = "syslog_io_bandwidth_limit"  # param_name

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")
            if self.observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0"):
                self.__execute_4()
            elif StringUtils.compare_versions_greater("4.0.0.0", self.observer_version):
                self.__execute_3()
            else:
                return self.report.add_fail("observer version is {0}. Not support".format(self.observer_version))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def __execute_4(self):
        try:
            sql = "select /*+READ_CONSISTENCY(WEAK), QUERY_TIMEOUT(100000000)*/ * from oceanbase.GV$OB_PARAMETERS where name='{0}';".format(self.param_name)
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not result:
                return self.report.add_warning("can't find this param_name")

            non_compliant_nodes = []
            for data_item in result:
                svr_ip = data_item['SVR_IP']
                value = data_item['VALUE']
                if value != self.expected_value:
                    non_compliant_nodes.append(svr_ip)

            if non_compliant_nodes:
                node_str = ", ".join(non_compliant_nodes)
                return self.report.add_warning("svr_ip: {0} syslog_io_bandwidth_limit !='30MB',please check ".format(node_str))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def __execute_3(self):
        try:
            sql = "select /*+READ_CONSISTENCY(WEAK), QUERY_TIMEOUT(100000000)*/ * from oceanbase.__all_virtual_sys_parameter_stat where name='{0}';".format(self.param_name)
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not result:
                return self.report.add_warning("can't find this param_name")

            non_compliant_nodes = []
            for data_item in result:
                svr_ip = data_item['svr_ip']
                value = data_item['value']
                if value != self.expected_value:
                    non_compliant_nodes.append(svr_ip)

            if non_compliant_nodes:
                node_str = ", ".join(non_compliant_nodes)
                return self.report.add_warning("svr_ip: {0} syslog_io_bandwidth_limit !='30MB',please check ".format(node_str))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "syslog_io_bandwidth_limit", "info": "Check the disk IO bandwidth limit that the system log can occupy. System logs that exceed the bandwidth upper limit will be discarded. It is recommended not to exceed 30M. issue #841"}


syslog_io_bandwidth_limit = SyslogIoBandwidthLimit()
