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
@time: 2024/12/29
@file: check_task.py
@desc: Base class for Python check tasks
"""

from src.common.command import get_observer_version, get_obproxy_version, get_obproxy_full_version
from src.common.tool import StringUtils


class TaskBase:
    """
    Base class for Python check tasks.

    All check tasks should inherit from this class and implement:
    - execute(): Main task logic
    - get_task_info(): Returns task metadata (name and info)
    """

    def __init__(self):
        self.work_path = None
        self.gather_log = None
        self.stdio = None
        self.input_parameters = None
        self.ob_cluster = None
        self.ob_connector = None
        self.store_dir = None
        self.obproxy_version = None
        self.obproxy_full_version = None
        self.observer_version = None
        self.report = None
        self.obproxy_nodes = []
        self.observer_nodes = []
        self.oms_nodes = []
        self.context = None
        self.name = type(self).__name__
        self.Result = None

    def init(self, context, report):
        """
        Initialize task context and report.

        Args:
            context: Handler context with cluster config and options
            report: TaskReport instance for recording results
        """
        self.report = report
        self.context = context
        self.stdio = context.stdio
        # get ob_cluster
        self.ob_cluster = self.context.cluster_config

        # Reuse nodes with SSH connections from handler (avoid recreating SSH connections)
        # The handler already created SSH connections for each node
        observer_nodes = self.context.cluster_config.get("servers")
        if observer_nodes:
            # Use existing nodes with ssher already attached by handler
            self.observer_nodes = observer_nodes

        obproxy_nodes = self.context.obproxy_config.get("servers")
        if obproxy_nodes:
            # Use existing nodes with ssher already attached by handler
            self.obproxy_nodes = obproxy_nodes

        # Reuse observer_version from context if available
        self.observer_version = self.context.cluster_config.get("version", "")
        if not self.observer_version:
            try:
                self.observer_version = get_observer_version(self.context)
            except Exception as e:
                self.stdio.error("get observer_version fail: {0}".format(e))

        # Reuse ob_connector from connection pool if available
        ob_connector_pool = self.context.get_variable('check_obConnector_pool')
        if ob_connector_pool:
            self.ob_connector = ob_connector_pool.get_connection()
            self._using_pool_connection = True
        else:
            # Fallback: create new connector if pool not available
            from src.common.ob_connector import OBConnector

            self.ob_connector = OBConnector(
                context=self.context,
                ip=self.ob_cluster.get("db_host"),
                port=self.ob_cluster.get("db_port"),
                username=self.ob_cluster.get("tenant_sys").get("user"),
                password=self.ob_cluster.get("tenant_sys").get("password"),
                timeout=10000,
            )
            self._using_pool_connection = False

        # get obproxy version (only once, reuse if available)
        if self.obproxy_nodes is None or len(self.obproxy_nodes) == 0:
            self.stdio.verbose("obproxy_nodes is None. So set obproxy_version and obproxy_full_version to None")
        else:
            try:
                self.obproxy_version = get_obproxy_version(self.context)
            except Exception as e:
                self.stdio.error("get obproxy_version fail: {0}".format(e))
            try:
                self.obproxy_full_version = get_obproxy_full_version(self.context)
            except Exception as e:
                self.stdio.error("get obproxy_full_version fail: {0}".format(e))

    def cleanup(self):
        """
        Cleanup resources after task execution.
        Release connection back to pool if using pooled connection.
        """
        if hasattr(self, '_using_pool_connection') and self._using_pool_connection:
            ob_connector_pool = self.context.get_variable('check_obConnector_pool')
            if ob_connector_pool and self.ob_connector:
                ob_connector_pool.release_connection(self.ob_connector)
                self.ob_connector = None

    def execute(self):
        """
        Execute the check task. Must be implemented by subclass.

        This method should:
        1. Perform diagnostic checks (SQL queries, SSH commands, etc.)
        2. Analyze results
        3. Report findings using self.report.add_warning/add_critical/add_fail
        """
        raise NotImplementedError("Subclass must implement execute()")

    def get_task_info(self):
        """
        Get task metadata. Must be implemented by subclass.

        Returns:
            dict: Task info with 'name', 'info' and optional 'issue_link' keys
            Example: {
                "name": "task_name",
                "info": "Task description",
                "issue_link": "https://github.com/oceanbase/obdiag/issues/xxx"
            }
        """
        raise NotImplementedError("Subclass must implement get_task_info()")

    def check_ob_version_min(self, min_version: str) -> bool:
        """
        Check if observer version meets minimum requirement.

        Args:
            min_version: Minimum version string (e.g., "4.0.0.0")

        Returns:
            bool: True if current version >= min_version
        """
        if self.observer_version is None:
            return False
        if self.observer_version == min_version:
            return True
        if StringUtils.compare_versions_greater(self.observer_version, min_version):
            return True
        else:
            return False

    def check_obproxy_version_min(self, min_version: str) -> bool:
        """
        Check if obproxy version meets minimum requirement.

        Args:
            min_version: Minimum version string

        Returns:
            bool: True if current version >= min_version
        """
        if self.obproxy_version is None:
            return False
        if self.obproxy_version == min_version:
            return True
        if StringUtils.compare_versions_greater(self.obproxy_version, min_version):
            return True
        else:
            return False

    def check_ob_version_max(self, max_version: str) -> bool:
        """
        Check if observer version is below maximum.

        Args:
            max_version: Maximum version string

        Returns:
            bool: True if current version <= max_version
        """
        if self.observer_version is None:
            return False
        if self.observer_version == max_version:
            return True
        if StringUtils.compare_versions_greater(max_version, self.observer_version):
            return True
        else:
            return False

    def get_obproxy_parameter(self, parameter_name: str) -> list:
        """
        Get obproxy parameter value.

        Args:
            parameter_name: Name of the parameter to query

        Returns:
            list: Parameter data from query result
        """
        try:
            sql = "show proxyconfig like '{0}';".format(parameter_name)
            data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            return data
        except Exception as e:
            self.stdio.error("get {0} fail: {1}. please check".format(parameter_name, e))
            return []

    def check_command_exist(self, ssh_client, command: str) -> bool:
        """
        Check if a command exists on remote node.

        Args:
            ssh_client: SSH client instance
            command: Command to check

        Returns:
            bool: True if command exists
        """
        if ssh_client is None:
            return False
        try:
            result = ssh_client.exec_cmd("command -v " + command)
            if result is None or len(result) == 0:
                return False
            return True
        except Exception as e:
            self.stdio.error("check_command_exist error: {0}".format(e))
            return False

    def get_system_parameter(self, ssh_client, parameter_name: str) -> str:
        """
        Get system parameter value from remote node.

        Args:
            ssh_client: SSH client instance
            parameter_name: Parameter name (e.g., "net.ipv4.tcp_rmem")

        Returns:
            str: Parameter value or None if not found
        """
        try:
            parameter_name = parameter_name.replace(".", "/")
            # check parameter_name exists
            if ssh_client.exec_cmd('find /proc/sys/ -name "{0}"'.format(parameter_name.split("/")[-1])) == "":
                self.stdio.warn("{0} is not exist".format(parameter_name))
                return None
            parameter_value = ssh_client.exec_cmd("cat /proc/sys/" + parameter_name).strip()
            return parameter_value
        except Exception as e:
            self.stdio.warn("get {0} fail: {1}. please check, the parameter_value will be set None".format(parameter_name, e))
            return None
