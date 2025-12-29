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
@time: 2025/04/29
@file: cgroup.py
@desc:
"""
import re
from src.handler.check.check_task import TaskBase


class Cgroup(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if super().check_ob_version_min("4.0.0.0") is False:
                return
            # check dir "cgroup" on home_path
            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                if node.get("home_path") is None:
                    return self.report.add_fail("node: {0} home_path is None".format(ssh_client.get_name()))
                home_path = node.get("home_path")
                cgroup_path = "{0}/cgroup".format(home_path)
                cgroup_exist = ssh_client.exec_cmd('[ -d "{0}" ] && echo "Directory exists" || echo "Directory does not exist"'.format(cgroup_path))
                if cgroup_exist.strip() == "Directory does not exist":
                    self.report.add_warning("node: {0} cgroup path is not exist. Tenant isolation not enabled. issue #849".format(ssh_client.get_name()))
                    continue
            # check enable_cgroup
            enable_cgroup = self.ob_connector.execute_sql_return_cursor_dictionary("SHOW PARAMETERS LIKE 'enable_cgroup';").fetchall()
            if len(enable_cgroup) < 1:
                return self.report.add_warning("not get enable_cgroup data. maybe the version not support")
            for enable_cgroup_one in enable_cgroup:
                # check VALUE is exist
                enable_cgroup_value = enable_cgroup_one.get("value")
                svr_ip = enable_cgroup_one.get("SVR_IP")
                if enable_cgroup_value is None:
                    return self.report.add_fail("get enable_cgroup value error")
                if enable_cgroup_value.lower() != "true":
                    self.report.add_warning("svr_ip: {0} enable_cgroup is {1}, recommended is True".format(svr_ip, enable_cgroup_value))
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "cgroup",
            "info": "Check if tenant isolation is enabled in versions 4. x and above. Default should be enabled to ensure performance",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/849",
        }


cgroup = Cgroup()
