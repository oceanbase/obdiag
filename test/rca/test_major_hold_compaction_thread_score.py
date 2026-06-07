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

import unittest
from unittest.mock import MagicMock

from plugins.rca.major_hold import MajorHoldScene
from src.common.tool import YamlUtils


class TestMajorHoldCompactionThreadScore(unittest.TestCase):

    def test_gather_compaction_collects_low_thread_score_for_all_tenants(self):
        task_data = YamlUtils.read_yaml_data("plugins/gather/tasks/observer/compaction.yaml")
        steps = next(item["steps"] for item in task_data["task"] if item["version"] == "[4.0.0.0, *]")
        sql_list = [step["sql"].lower() for step in steps if step.get("type") == "sql"]
        parameter_sql = next(sql for sql in sql_list if "compaction_low_thread_score" in sql)

        self.assertIn("__all_virtual_tenant_parameter_info", parameter_sql)
        self.assertIn("tenant_id", parameter_sql)
        self.assertIn("order by tenant_id, zone, svr_ip, svr_port", parameter_sql)

    def test_major_hold_records_low_thread_score(self):
        scene = MajorHoldScene()
        scene._execute_sql_safe = MagicMock(
            return_value=[
                {
                    "tenant_id": 1001,
                    "zone": "zone1",
                    "svr_ip": "127.0.0.1",
                    "svr_port": 2882,
                    "name": "compaction_low_thread_score",
                    "value": "0",
                }
            ]
        )
        scene._save_to_file = MagicMock()
        record = MagicMock()

        scene._check_compaction_low_thread_score(record)

        scene._execute_sql_safe.assert_called_once()
        self.assertIn("compaction_low_thread_score", scene._execute_sql_safe.call_args[0][0])
        scene._save_to_file.assert_called_once()
        record.add_record.assert_any_call("Collected compaction_low_thread_score for 1 tenant/server scope rows")
        record.add_record.assert_any_call("compaction_low_thread_score value 0 means OceanBase uses the internal default thread score")


if __name__ == '__main__':
    unittest.main()
