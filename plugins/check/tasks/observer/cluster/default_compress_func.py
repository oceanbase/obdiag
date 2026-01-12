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
@file: default_compress_func.py
@desc:
"""
from src.common.tool import StringUtils
from src.handler.check.check_task import TaskBase


class DefaultCompressFunc(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("this version:{} is not support this task".format(self.observer_version))
            default_compress_func_data = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.GV$OB_PARAMETERS where Name=\"default_compress_func\";").fetchall()
            for default_compress_func_one in default_compress_func_data:
                default_compress_func_value = default_compress_func_one.get("VALUE")
                svr_ip = default_compress_func_one.get("SVR_IP")
                if default_compress_func_value is None:
                    return self.report.add_fail("get default_compress_func value error")
                # get default_value
                default_value = default_compress_func_one.get("default_value") or default_compress_func_one.get("DEFAULT_VALUE")
                if default_value is None:
                    default_value = "zstd_1.0"
                if not (self.observer_version == "4.2.2" or StringUtils.compare_versions_greater(self.observer_version, "4.2.2")):
                    default_value = "zstd_1.3.8"
                if default_compress_func_value != default_value:
                    self.report.add_warning("svr_ip: {1}. default_compress_func is {0}, recommended value is {2}.".format(default_compress_func_value, svr_ip, default_value))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "default_compress_func",
            "info": "The default compression algorithm for checklist data. Recommend using default value with ob_version to improve compression ratio and reduce storage costs. For scenarios with high requirements for querying rt, consider using lz4_1.0 or turning off compression",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/792",
        }


default_compress_func = DefaultCompressFunc()
