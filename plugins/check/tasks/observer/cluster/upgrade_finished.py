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
@time: 2024/03/28
@file: upgrade_finished.py
@desc:
"""
import re

from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class UpgradeFinished(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # check observer version
            min_supported_version = "4.2.0.0"
            if self.observer_version:
                if not (StringUtils.compare_versions_greater(self.observer_version, min_supported_version)) and self.observer_version != min_supported_version:
                    return self.report.add_warning("[SKIP] this task need observer version more than {1}, this cluster is {0}.".format(self.observer_version, min_supported_version))
            else:
                # this task need to check observer version, if observer version is not exist,
                return self.report.add_warning("[SKIP] this task need to check observer version, if observer version is not exist, please check")

            pass_tag = True
            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")
            build_version_data = self.ob_connector.execute_sql_return_cursor_dictionary("select distinct build_version from oceanbase.__all_server; ").fetchall()
            if len(build_version_data) != 1:
                return self.report.add_critical("build_version count >1, please check")
            # like 4.3.5.2_1-b4a7793c1cd6935a42a3c210373eb3c97c57b9ec(Mar 28 2025 22:35:37)
            build_version = build_version_data[0].get("build_version")
            # Define the regex pattern to match the data before the underscore
            pattern = r'([^_]+)_'
            # Search for the pattern in the version string
            match = re.search(pattern, build_version)
            if match:
                result = match.group(1)
                build_version = result
            else:
                return self.report.add_critical("build_version format error, please check, build_version info: {0}".format(build_version))
            min_observer_version_data = self.ob_connector.execute_sql_return_cursor_dictionary("show parameters like 'min_observer_version'; ").fetchall()
            for row in min_observer_version_data:
                if row.get("value") != build_version:
                    pass_tag = False
                    self.report.add_critical("min_observer_version value not equal build_version. node:{0} min_observer_version:{1} build_version:{2}".format(row.get("svr_ip"), row.get("value"), build_version))
            if not pass_tag:
                return
            compatible_diff_data = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.DBA_OB_TENANTS where compatible<>'{0}' and TENANT_ROLE<>'STANDBY';".format(build_version)).fetchall()
            if len(compatible_diff_data) > 0:
                compatible_diff_tenants = ""
                for row in compatible_diff_data:
                    self.report.add_critical("there tenant:{0} compatible not equal min_observer_version. tenants: {0} ,compatible:{1} min_observer_version:{2}".format(compatible_diff_tenants, row.get("compatible"), build_version))

            # alter system run job 'root_inspection'
            try:
                self.ob_connector.execute_sql("alter system run job 'root_inspection';")
            except Exception as e:
                self.report.add_warning("alter system run job 'root_inspection' error {0}".format(e))
            enable_upgrade_mode_data = self.ob_connector.execute_sql_return_cursor_dictionary("SHOW PARAMETERS LIKE 'enable_upgrade_mode';").fetchall()
            for row in enable_upgrade_mode_data:
                if row.get("value") != "False":
                    self.report.add_critical("enable_upgrade_mode is True.")
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "upgrade_finished", "info": "check obcluster upgrade if finished. issue #759"}


upgrade_finished = UpgradeFinished()
