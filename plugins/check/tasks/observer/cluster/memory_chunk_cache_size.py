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
@time: 2025/05/6
@file: memory_chunk_cache_size.py
@desc:
"""

from src.handler.checker.check_task import TaskBase


class MemoryChunkCacheSize(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            memory_chunk_cache_size_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * FROM oceanbase.__all_virtual_sys_parameter_stat WHERE name='memory_chunk_cache_size';").fetchall()

            for memory_chunk_cache_size_one in memory_chunk_cache_size_data:
                memory_chunk_cache_size_value = memory_chunk_cache_size_one.get("value")
                svr_ip = memory_chunk_cache_size_one.get("svr_ip")
                if memory_chunk_cache_size_value is None:
                    return self.report.add_fail("get memory_chunk_cache_size value error")
                if memory_chunk_cache_size_value.lower() != "0m":
                    self.report.add_warning("svr_ip: {1}. memory_chunk_cache_size is {0}, recommended value is 0M.".format(memory_chunk_cache_size_value, svr_ip))

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "memory_chunk_cache_size", "info": "Check the memory block capacity of the memory allocator cache. It is recommended to set to 0. issue#843"}


memory_chunk_cache_size = MemoryChunkCacheSize()
