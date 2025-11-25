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
@file: observer_port.py
@desc:
"""
from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class ObserverPort(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            if StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0"):
                pass
            else:
                return None
            # get all server sql port and rpc port
            servers_data = self.ob_connector.execute_sql_return_cursor_dictionary("SELECT * FROM oceanbase.DBA_OB_SERVERS;").fetchall()
            for node in self.observer_nodes:
                self._execute_one(node, servers_data)

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _execute_one(self, node, servers_data):
        try:
            ssh_client = node.get("ssher")
            if not self.check_command_exist(ssh_client, "nc"):
                self.report.add_warning("node: {0}. nc command not exist, please install nc command".format(ssh_client.get_name()))
                return
            for server_data in servers_data:
                # test SVR_PORT
                err_info = ssh_client.exec_cmd("echo | nc -v  -w 5 {0} {1}".format(server_data.get("SVR_IP"), server_data.get("SVR_PORT")))
                if err_info.strip() != "" and "Connection refused" in err_info:
                    self.report.add_critical("node: {0}. can not conn {1}:{2} ".format(ssh_client.get_name(), server_data.get("SVR_IP"), server_data.get("SVR_PORT")))
                # test SQL_PORT
                err_info = ssh_client.exec_cmd("echo | nc -v  -w 5 {0} {1}".format(server_data.get("SVR_IP"), server_data.get("SQL_PORT")))
                if err_info.strip() != "" and "Connection refused" in err_info:
                    self.report.add_critical("node: {0}. can not conn {1}:{2} ".format(ssh_client.get_name(), server_data.get("SVR_IP"), server_data.get("SQL_PORT")))
        except Exception as e:
            self.stdio.error("ssh execute Exception:{0}".format(e).strip())
            self.report.add_fail("node:{1} execute error {0}".format(node.get("ip"), e).strip())

    def get_task_info(self):
        return {"name": "observer_port", "info": "Check if the necessary ports between OceanBase cluster nodes are connected. issue #845"}


observer_port = ObserverPort()
