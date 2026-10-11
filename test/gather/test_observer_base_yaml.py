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

from src.common.tool import YamlUtils


OBSERVER_BASE_TASK = "plugins/gather/tasks/observer/base.yaml"


class TestObserverBaseYaml(unittest.TestCase):
    def test_table_size_sql_aggregates_leader_tablets_by_table(self):
        task_data = YamlUtils.read_yaml_data(OBSERVER_BASE_TASK)
        steps = next(item["steps"] for item in task_data["task"] if item["version"] == "[4.0.0.0, *]")
        table_size_sql = next(step["sql"] for step in steps if "total_data_size_gb" in step.get("sql", ""))
        normalized_sql = " ".join(table_size_sql.lower().split())

        self.assertIn("where role='leader'", normalized_sql)
        self.assertIn("sum(ifnull(t2.data_size,0)) / 1073741824 as total_data_size_gb", normalized_sql)
        self.assertIn("t1.svr_ip = t2.svr_ip", normalized_sql)
        self.assertIn("t1.ls_id = t2.ls_id", normalized_sql)
        self.assertIn("t1.tablet_id = t2.tablet_id", normalized_sql)
        self.assertIn("group by t1.tenant_id, t1.database_name, t1.table_name", normalized_sql)

        self.assertNotIn("t1.svr_ip, t1.role", normalized_sql)


if __name__ == '__main__':
    unittest.main()
