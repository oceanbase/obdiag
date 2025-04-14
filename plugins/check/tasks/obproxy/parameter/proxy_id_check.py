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
@file: default_compress_func.py
@desc:
"""
from src.handler.checker.check_task import TaskBase


class DefaultCompressFunc(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            proxy_ids = {}
            proxy_ids_data = self.ob_connector.execute_sql_return_cursor_dictionary("show proxyconfig like 'proxy_id';").fetchall()
            for proxy_id_data in proxy_ids_data:
                proxy_id_value = proxy_id_data.get("value")
                if proxy_id_value is None:
                    return self.report.add_fail("get proxy_id value error")
                if int(proxy_id_value) in proxy_ids:
                    return self.report.add_warning("proxy_id is not unique, please check")
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "proxy_id_check",
            "info": "The default compression algorithm for checklist data. Recommend using default value with ob_version to improve compression ratio and reduce storage costs. For scenarios with high requirements for querying rt, consider using lz4_1.0 or turning off compression",
        }


proxy_id_check = DefaultCompressFunc()
