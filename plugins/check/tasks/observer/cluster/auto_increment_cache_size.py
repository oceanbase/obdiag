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
@file: auto_increment_cache_size.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class AutoIncrementCacheSize(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            auto_increment_cache_size_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * from oceanbase.`CDB_OB_SYS_VARIABLES` where name= \"auto_increment_cache_size\";").fetchall()
            if len(auto_increment_cache_size_data) < 1:
                return self.report.add_fail("get auto_increment_cache_size data error")
            for auto_increment_cache_size_one in auto_increment_cache_size_data:
                # check VALUE is exist

                auto_increment_cache_size_value = auto_increment_cache_size_one.get("VALUE")
                if auto_increment_cache_size_value is None:
                    return self.report.add_fail("get auto_increment_cache_size value error")
                auto_increment_cache_size_value = int(auto_increment_cache_size_value)
                tenant_id = auto_increment_cache_size_one.get("TENANT_ID")

                # check DEFAULT_VALUE is exist
                default_value = auto_increment_cache_size_data[0].get("default_value") or auto_increment_cache_size_data[0].get("DEFAULT_VALUE")
                if default_value is None:
                    default_value = 1000000
                if auto_increment_cache_size_value != default_value:
                    self.report.add_warning("tenant_id: {2} auto_increment_cache_size is {0}, default value is {1}".format(auto_increment_cache_size_value, default_value, tenant_id))
                max_value = auto_increment_cache_size_data[0].get("MAX_VALUE")
                if max_value is None:
                    self.report.add_critical("auto_increment_cache_size max_value is None")
                    return
                else:
                    max_value = int(max_value)
                min_value = auto_increment_cache_size_data[0].get("MIN_VALUE")
                if min_value is None:
                    self.report.add_critical("auto_increment_cache_size min_value is None")
                    return
                else:
                    min_value = int(min_value)
                if auto_increment_cache_size_value < min_value or auto_increment_cache_size_value > max_value:
                    self.report.add_critical("tenant_id: {3} auto_increment_cache_size is {0}, min value is {1}, max value is {2}".format(auto_increment_cache_size_value, min_value, max_value, tenant_id))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "auto_increment_cache_size", "info": "Check the global available cache of all tenant self added columns in the cluster"}


auto_increment_cache_size = AutoIncrementCacheSize()
