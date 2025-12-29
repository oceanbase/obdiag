#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
@file: trace_log_slow_query_watermark.py
@desc:
"""
import re
from src.common.tool import StringUtils
from src.handler.check.check_task import TaskBase


class TraceLogSlowQueryWatermark(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        self.expected_value = "1s"  # expected_value
        self.param_name = "trace_log_slow_query_watermark"  # param_name

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

            non_compliant_ip = []

            for data_item in result:
                svr_ip = str(data_item['SVR_IP'])
                value = data_item['VALUE']
                value_match = re.search(r'(\d+)(\w*)', value)
                num_value = int(value_match.group(1))
                unit = value_match.group(2).lower() if value_match.group(2) else 's'
                if unit == 'ms':
                    num_value = num_value / 1000
                elif unit == 'h':
                    num_value = num_value * 3600
                elif unit == 'd':
                    num_value = num_value * 86400
                # num_value = int(re.search(r'\d+', value).group())
                expected_num = int(re.search(r'\d+', self.expected_value).group())
                if num_value < expected_num or num_value >= 2:
                    non_compliant_ip.append(svr_ip)

            if non_compliant_ip:
                ip_str = ", ".join(non_compliant_ip)
                return self.report.add_warning("ip: {0} trace_log_slow_query_watermark not in [1,2),please check ".format(ip_str))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def __execute_3(self):
        try:
            sql = "select /*+READ_CONSISTENCY(WEAK), QUERY_TIMEOUT(100000000)*/ * from oceanbase.__all_virtual_sys_parameter_stat where name='{0}';".format(self.param_name)
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not result:
                return self.report.add_warning("can't find this param_name")

            non_compliant_ip = []

            for data_item in result:
                svr_ip = str(data_item['svr_ip'])
                value = data_item['value']
                value_match = re.search(r'(\d+)(\w*)', value)
                num_value = int(value_match.group(1))
                unit = value_match.group(2).lower() if value_match.group(2) else 's'
                if unit == 'ms':
                    num_value = num_value / 1000
                elif unit == 'h':
                    num_value = num_value * 3600
                elif unit == 'd':
                    num_value = num_value * 86400
                # num_value = int(re.search(r'\d+', value).group())
                expected_num = int(re.search(r'\d+', self.expected_value).group())
                if num_value < expected_num or num_value >= 2:
                    non_compliant_ip.append(svr_ip)

            if non_compliant_ip:
                ip_str = ", ".join(non_compliant_ip)
                return self.report.add_warning("ip: {0} trace_log_slow_query_watermark not in [1,2),please check ".format(ip_str))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "trace_log_slow_query_watermark",
            "info": "Check the execution time threshold of the query. If the execution time of the query exceeds this threshold, it is considered a slow query, and the trace log of the slow query will be printed to the system log. It is recommended not less than 1s, not more than 2s",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/842",
        }


trace_log_slow_query_watermark = TraceLogSlowQueryWatermark()
