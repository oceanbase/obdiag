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
@file: parameters_default.py
@desc:
"""
from src.common.command import get_observer_version
from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class ParametersDefault(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            # check version 4.2.2.0 ≤ obversion < 4.3.0.0 or 4.3.1.0≤ obversion
            obversion = get_observer_version(self.context)
            if (super().check_ob_version_min("4.2.2.0") or obversion == "4.2.2.0") and StringUtils.compare_versions_greater("4.3.0.0", obversion):
                pass
            elif super().check_ob_version_min("4.3.1.0"):
                pass
            else:
                return self.report.add_warning("the task is not supported in this version: {0}".format(obversion))
            # get all PARAMETERS
            sql = """
            SELECT 
    name, 
    GROUP_CONCAT(DISTINCT tenant_id ORDER BY tenant_id ASC) AS tenant_ids, 
    value, 
    default_value 
FROM 
    oceanbase.GV$OB_PARAMETERS 
WHERE 
    value <> default_value 
GROUP BY 
    name, 
    value, 
    default_value;
            """
            parameters_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if len(parameters_data) == 0:
                return
            for parameters_data_one in parameters_data:
                parameters_name = parameters_data_one.get("name")
                parameters_value = parameters_data_one.get("value")
                parameters_default_value = parameters_data_one.get("default_value")
                parameters_tenant_ids = parameters_data_one.get("tenant_ids")
                if parameters_tenant_ids is None:
                    report_str = "the {0} is not default_value. value: {1}, default_value: {2}".format(parameters_name, parameters_value, parameters_default_value)
                else:
                    report_str = "the {0} is not default_value. tenant_ids: {1}, value: {2}, default_value: {3}".format(parameters_name, parameters_tenant_ids, parameters_value, parameters_default_value)
                self.report.add_warning(report_str)

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "parameters_default", "info": "Confirm if the configuration item is the default value. issue#850"}


parameters_default = ParametersDefault()
