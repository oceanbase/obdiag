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

import re

from src.common.command import get_observer_version, get_obproxy_version, get_obproxy_full_version
from src.common.tool import StringUtils, Util


class NodeWrapper(dict):
    """
    Wrapper for node dict to support get_name() used by some plugins.

    Delegates to ssher.get_name() when ssher exists, else falls back to node.get("ip", "unknown").
    """

    def get_name(self):
        ssher = self.get("ssher")
        if ssher is not None and hasattr(ssher, "get_name"):
            return ssher.get_name()
        return self.get("ip", "unknown")


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

        # Get SSH connections from pool (shared across tasks).
        # Only request nodes for current check_target_type to avoid pool exhaustion:
        # obproxy tasks should not request observer nodes (and vice versa).
        ssh_manager = self.context.get_variable("check_ssh_manager")
        check_target_type = self.context.get_variable("check_target_type") or "observer"

        if check_target_type == "observer":
            observer_nodes_config = self.context.cluster_config.get("servers")
            if observer_nodes_config:
                self.observer_nodes = []
                for node in observer_nodes_config:
                    node_copy = NodeWrapper(node.copy())
                    ssher = ssh_manager.get_connection(node) if ssh_manager else None
                    node_copy["ssher"] = ssher
                    self.observer_nodes.append(node_copy)
            else:
                self.observer_nodes = []

        if check_target_type == "obproxy":
            obproxy_config = self.context.obproxy_config or {}
            obproxy_nodes_config = obproxy_config.get("servers")
            if obproxy_nodes_config:
                self.obproxy_nodes = []
                for node in obproxy_nodes_config:
                    node_copy = NodeWrapper(node.copy())
                    ssher = ssh_manager.get_connection(node) if ssh_manager else None
                    node_copy["ssher"] = ssher
                    self.obproxy_nodes.append(node_copy)
            else:
                self.obproxy_nodes = []

        # Check if this is build_before case (should not create DB connection or get version)
        cases_option = Util.get_option(self.context.options, 'cases')
        is_build_before = cases_option == "build_before"

        # Reuse observer_version from context if available
        # For build_before cases, do not get observer version
        if is_build_before:
            self.observer_version = None
            self.stdio.verbose("cases is build_before, skip getting observer version")
        else:
            # Prefer context cache, then cluster_config, then query
            self.observer_version = self.context.get_variable("check_observer_version") or ""
            if not self.observer_version:
                self.observer_version = self.context.cluster_config.get("version", "")
            if not self.observer_version:
                try:
                    self.observer_version = get_observer_version(self.context)
                except Exception as e:
                    self.stdio.error("get observer_version fail: {0}".format(e))

        # Reuse ob_connector from connection pool if available
        # For build_before cases, do not create database connection

        ob_connector_pool = self.context.get_variable('check_obConnector_pool')
        if ob_connector_pool:
            self.ob_connector = ob_connector_pool.get_connection()
            self._using_pool_connection = True
        elif is_build_before:
            # For build_before cases, do not create database connection
            self.ob_connector = None
            self._using_pool_connection = False
            self.stdio.verbose("cases is build_before, skip creating database connection")
        else:
            # Fallback: create new connector if pool not available
            from src.common.ob_connector import OBConnector

            tenant_sys = self.ob_cluster.get("tenant_sys") or {}
            self.ob_connector = OBConnector(
                context=self.context,
                ip=self.ob_cluster.get("db_host"),
                port=self.ob_cluster.get("db_port"),
                username=tenant_sys.get("user"),
                password=tenant_sys.get("password"),
                timeout=10000,
            )
            self._using_pool_connection = False

        # get obproxy version (only once, reuse if available)
        if check_target_type == "obproxy":
            if self.obproxy_nodes is None or len(self.obproxy_nodes) == 0:
                self.stdio.verbose("obproxy_nodes is None. So set obproxy_version and obproxy_full_version to None")
            else:
                # Prefer context cache (set by parent before task dispatch), then query
                self.obproxy_version = self.context.get_variable("check_obproxy_version") or ""
                if not self.obproxy_version:
                    try:
                        self.obproxy_version = get_obproxy_version(self.context)
                    except Exception as e:
                        self.stdio.error("get obproxy_version fail: {0}".format(e))
                # Prefer context cache for full_version too
                self.obproxy_full_version = self.context.get_variable("check_obproxy_full_version") or ""
                if not self.obproxy_full_version:
                    try:
                        self.obproxy_full_version = get_obproxy_full_version(self.context)
                    except Exception as e:
                        self.stdio.error("get obproxy_full_version fail: {0}".format(e))

    def cleanup(self):
        """
        Cleanup resources after task execution.
        Release DB and SSH connections back to their pools.
        """
        # Release database connection back to pool
        if hasattr(self, "_using_pool_connection") and self._using_pool_connection:
            ob_connector_pool = self.context.get_variable("check_obConnector_pool")
            if ob_connector_pool and self.ob_connector:
                ob_connector_pool.release_connection(self.ob_connector)
                self.ob_connector = None

        # Release SSH connections back to pool (do not close, they are reused)
        ssh_manager = self.context.get_variable("check_ssh_manager")
        if ssh_manager:
            if hasattr(self, "observer_nodes") and self.observer_nodes:
                for node in self.observer_nodes:
                    ssher = node.get("ssher")
                    if ssher:
                        ssh_manager.release_connection(ssher)
            if hasattr(self, "obproxy_nodes") and self.obproxy_nodes:
                for node in self.obproxy_nodes:
                    ssher = node.get("ssher")
                    if ssher:
                        ssh_manager.release_connection(ssher)

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
            dict: Task info with required and optional keys:
            Required:
                - name: Task name
                - info: Task description
            Optional:
                - issue_link: Related issue URL
                - supported_os: List of supported OS types ["linux", "darwin"]
                              If not specified, task runs on all platforms.
                              Use ["linux"] for Linux-only tasks (e.g., cgroup, aio checks)

            Example: {
                "name": "task_name",
                "info": "Task description",
                "issue_link": "https://github.com/oceanbase/obdiag/issues/xxx",
                "supported_os": ["linux"]  # Linux only task
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
        return StringUtils.compare_versions_greater(self.observer_version, min_version)

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
        return StringUtils.compare_versions_greater(self.obproxy_version, min_version)

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
        return StringUtils.compare_versions_greater(max_version, self.observer_version)

    def get_obproxy_parameter(self, parameter_name: str) -> list:
        """
        Get obproxy parameter value.

        Args:
            parameter_name: Name of the parameter to query (must match ^[\\w.]+$)

        Returns:
            list: Parameter data from query result
        """
        if not re.match(r'^[\w.]+$', parameter_name):
            self.stdio.error("invalid parameter_name '{0}': only word characters and dots are allowed".format(parameter_name))
            return []
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
            command: Command to check (must contain only word characters and hyphens)

        Returns:
            bool: True if command exists
        """
        if ssh_client is None:
            return False
        if not re.match(r'^[\w.\-]+$', command):
            self.stdio.error("invalid command name '{0}': only word characters, dots and hyphens are allowed".format(command))
            return False
        try:
            result = ssh_client.exec_cmd("command -v " + command)
            if result is None or len(result) == 0:
                return False
            return True
        except Exception as e:
            self.stdio.error("check_command_exist error: {0}".format(e))
            return False

    def get_os_type(self, ssh_client) -> str:
        """
        Detect operating system type on remote node.

        Args:
            ssh_client: SSH client instance

        Returns:
            str: 'linux', 'darwin' (macOS), or 'unknown'
        """
        if ssh_client is None:
            return "unknown"
        try:
            result = ssh_client.exec_cmd("uname -s").strip().lower()
            if "linux" in result:
                return "linux"
            elif "darwin" in result:
                return "darwin"
            else:
                return "unknown"
        except Exception as e:
            self.stdio.warn("get os type fail: {0}".format(e))
            return "unknown"

    def is_linux(self, ssh_client) -> bool:
        """Check if remote node is Linux"""
        return self.get_os_type(ssh_client) == "linux"

    def is_macos(self, ssh_client) -> bool:
        """Check if remote node is macOS"""
        return self.get_os_type(ssh_client) == "darwin"

    def get_system_parameter(self, ssh_client, parameter_name: str) -> str:
        """
        Get system parameter value from remote node.
        Supports both Linux (/proc/sys/) and macOS (sysctl).

        Args:
            ssh_client: SSH client instance
            parameter_name: Parameter name (e.g., "net.ipv4.tcp_rmem")

        Returns:
            str: Parameter value or None if not found
        """
        if ssh_client is None:
            return None
        try:
            os_type = self.get_os_type(ssh_client)

            # Validate parameter_name before any branch to prevent shell injection
            if not re.match(r'^[\w.\-]+$', parameter_name):
                self.stdio.error("invalid parameter_name '{0}': only word characters, dots and hyphens are allowed".format(parameter_name))
                return None

            if os_type == "darwin":
                # macOS: use sysctl command
                # Convert Linux-style parameter name to macOS style if needed
                mac_param_name = self._convert_param_name_for_mac(parameter_name)
                if mac_param_name is None:
                    self.stdio.verbose("Parameter {0} is not supported on macOS".format(parameter_name))
                    return None

                result = ssh_client.exec_cmd("sysctl -n {0} 2>/dev/null".format(mac_param_name)).strip()
                if result == "" or "unknown oid" in result.lower():
                    self.stdio.verbose("{0} is not available on macOS".format(mac_param_name))
                    return None
                return result

            else:
                # Linux: use /proc/sys/
                proc_path = parameter_name.replace(".", "/")
                check_result = ssh_client.exec_cmd("test -f /proc/sys/{0} && echo exists".format(proc_path))
                if "exists" not in (check_result or ""):
                    self.stdio.warn("{0} does not exist".format(parameter_name))
                    return None
                parameter_value = ssh_client.exec_cmd("cat /proc/sys/" + proc_path).strip()
                return parameter_value

        except Exception as e:
            self.stdio.warn("get {0} fail: {1}. please check, the parameter_value will be set None".format(parameter_name, e))
            return None

    def _convert_param_name_for_mac(self, linux_param_name: str) -> str:
        """
        Convert Linux parameter name to macOS sysctl equivalent.

        Args:
            linux_param_name: Linux-style parameter name

        Returns:
            str: macOS sysctl parameter name, or None if not supported
        """
        # Mapping of Linux parameters to macOS equivalents
        linux_to_mac_mapping = {
            # Network parameters
            "net.core.somaxconn": "kern.ipc.somaxconn",
            "net.core.rmem_default": "net.local.stream.recvspace",
            "net.core.wmem_default": "net.local.stream.sendspace",
            "net.core.rmem_max": "kern.ipc.maxsockbuf",
            "net.core.wmem_max": "kern.ipc.maxsockbuf",
            "net.ipv4.ip_forward": "net.inet.ip.forwarding",
            "net.ipv4.tcp_fin_timeout": "net.inet.tcp.keepinit",
            # File system parameters
            "fs.file-max": "kern.maxfiles",
            # Kernel parameters
            "kernel.core_pattern": "kern.corefile",
            # VM parameters (some don't have direct equivalents)
            "vm.swappiness": None,  # No direct equivalent on macOS
            "vm.max_map_count": None,  # Linux specific
            "vm.overcommit_memory": None,  # Linux specific
            "vm.nr_hugepages": None,  # Linux specific
            "vm.min_free_kbytes": None,  # Linux specific
            "vm.zone_reclaim_mode": None,  # Linux specific
            "kernel.numa_balancing": None,  # Linux specific
            # AIO parameters (Linux specific)
            "fs.aio-max-nr": None,
            "fs.aio-nr": None,
            # TCP parameters
            "net.ipv4.tcp_rmem": None,  # Linux specific format (3 values)
            "net.ipv4.tcp_wmem": None,  # Linux specific format (3 values)
            "net.ipv4.tcp_tw_reuse": None,  # Linux specific
            "net.ipv4.tcp_syncookies": None,  # Linux specific
            "net.ipv4.tcp_max_syn_backlog": None,  # Linux specific
            "net.ipv4.tcp_slow_start_after_idle": None,  # Linux specific
            "net.ipv4.ip_local_port_range": "net.inet.ip.portrange.first",  # Partial mapping
            "net.ipv4.conf.default.rp_filter": None,  # Linux specific
            "net.ipv4.conf.default.accept_source_route": None,  # Linux specific
            "net.core.netdev_max_backlog": None,  # Linux specific
            "fs.pipe-user-pages-soft": None,  # Linux specific
        }

        return linux_to_mac_mapping.get(linux_param_name, linux_param_name)
