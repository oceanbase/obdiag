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
@file: check_handler.py
@desc: Handler for executing Python check tasks.

This module provides:
- CheckHandler: Main handler that loads, filters, and executes check tasks concurrently
- CheckOBConnectorPool: Connection pool for OceanBase database connections used by tasks
"""
import os
import queue
import traceback
import re
import oyaml as yaml
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.common.ob_connector import OBConnector
from src.common.scene import get_version_by_type
from src.common.ssh_client.ssh_connection_manager import SSHConnectionManager
from src.handler.check.check_report import TaskReport, CheckReport
from src.common.tool import Util, DynamicLoading
from src.common.tool import StringUtils

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_MAX_WORKERS = 12
MAX_DB_POOL_SIZE = 12
SUPPORTED_REPORT_TYPES = ("table", "json", "xml", "yaml", "html")
TARGET_OBSERVER = "observer"
TARGET_OBPROXY = "obproxy"
CASE_BUILD_BEFORE = "build_before"
PACKAGE_FILE_SUFFIX = "_check_package.yaml"


class CheckHandler:
    """
    Handler for executing Python check tasks.

    Workflow:
    1. Load tasks from tasks directory (observer/ or obproxy/ under work_path)
    2. Filter tasks by input (--observer_tasks, --cases, or all with filter)
    3. Execute tasks concurrently via ThreadPoolExecutor
    4. Generate and export check report

    Task loading modes:
    - input_tasks: Match by pattern (e.g. "system.*" or "clog.clog_disk_full")
    - package_name: Load task list from *_check_package.yaml
    - all: Load all tasks, optionally excluding filter package
    """

    def __init__(self, context, check_target_type="observer"):
        """
        Initialize CheckHandler.

        Args:
            context: HandlerContext with cluster_config, options, stdio
            check_target_type: "observer" or "obproxy", determines which tasks to load
        """
        self.version = None
        self.context = context
        self.stdio = context.stdio
        self.report = None
        self.tasks = None
        self.check_target_type = check_target_type
        self.options = context.options

        # Load config from inner_config
        self._load_config()
        # Validate paths and init connection pools
        self._validate_paths()
        self._init_connection_pool()
        self._init_ssh_pool()

    def _load_config(self):
        """Load configuration from context.inner_config."""
        check_config = self.context.inner_config.get("check", {})
        report_config = check_config.get("report", {})

        self.max_workers = check_config.get("max_workers", DEFAULT_MAX_WORKERS)
        self.work_path = os.path.expanduser(
            check_config.get("work_path") or "~/.obdiag/check"
        )
        self.export_report_path = os.path.expanduser(
            report_config.get("report_path") or "./check_report/"
        )
        self.export_report_type = report_config.get("export_type") or "table"
        self.ignore_version = check_config.get("ignore_version") or False

        self.cluster = self.context.cluster_config
        if self.check_target_type == TARGET_OBSERVER:
            self.nodes = self.context.cluster_config.get("servers")
        elif self.check_target_type == TARGET_OBPROXY:
            self.nodes = self.context.obproxy_config.get("servers")
        else:
            self.nodes = None

        self.tasks_base_path = os.path.expanduser(
            os.path.join(self.work_path, "tasks", "")
        )
        self.input_env = StringUtils.parse_env_display(
            Util.get_option(self.options, "env")
        ) or {}

        self.stdio.verbose(
            "CheckHandler input. ignore_version={0}, cluster={1}, nodes={2}, "
            "export_report_path={3}, export_report_type={4}, check_target_type={5}, "
            "tasks_base_path={6}, input_env={7}".format(
                self.ignore_version,
                self.cluster.get("ob_cluster_name") or self.cluster.get("obproxy_cluster_name"),
                StringUtils.node_cut_passwd_for_log(self.nodes),
                self.export_report_path,
                self.export_report_type,
                self.check_target_type,
                self.tasks_base_path,
                self.input_env,
            )
        )

    def _validate_paths(self):
        """Validate package file and tasks directory exist."""
        if self.check_target_type is None:
            raise Exception("check_target_type is null. Please check the conf")

        # Package file: {work_path}/{observer|obproxy}_check_package.yaml
        self.package_file_name = os.path.expanduser(
            os.path.join(self.work_path, self.check_target_type + PACKAGE_FILE_SUFFIX)
        )
        if not os.path.exists(self.package_file_name):
            raise Exception(
                "case_package_file {0} does not exist".format(self.package_file_name)
            )
        self.stdio.verbose("case_package_file is " + self.package_file_name)

        # Tasks directory: {work_path}/tasks/{observer|obproxy}
        tasks_path = os.path.join(self.tasks_base_path, self.check_target_type)
        tasks_path = os.path.expanduser(tasks_path)
        if not os.path.exists(tasks_path):
            raise Exception("tasks_base_path {0} does not exist".format(tasks_path))
        self.tasks_base_path = tasks_path
        self.stdio.verbose("tasks_base_path is " + self.tasks_base_path)

    def _init_connection_pool(self):
        """
        Initialize DB connection pool for tasks.

        Skipped when cases=build_before (pre-install check, no DB connection).
        Pool size is capped at MAX_DB_POOL_SIZE to avoid overloading the database.
        """
        if Util.get_option(self.options, "cases") == CASE_BUILD_BEFORE:
            self.stdio.warn("check cases is build_before, skip getting version")
            return

        self.version = get_version_by_type(
            self.context, self.check_target_type, self.stdio
        )
        # Cache version in context for tasks and other modules
        if self.check_target_type == TARGET_OBSERVER:
            self.context.set_variable("check_observer_version", self.version)
        elif self.check_target_type == TARGET_OBPROXY:
            self.context.set_variable("check_obproxy_version", self.version)
        ob_connector_pool = None
        try:
            pool_size = min(self.max_workers, MAX_DB_POOL_SIZE)
            ob_connector_pool = CheckOBConnectorPool(
                self.context, pool_size, self.cluster
            )
        except Exception as e:
            self.stdio.warn("obConnector init error: {0}".format(e))
        finally:
            self.context.set_variable("check_obConnector_pool", ob_connector_pool)

    def _init_ssh_pool(self):
        """
        Initialize SSH connection pool for tasks.

        Pool is shared across all check tasks. Config: max_connections_per_node, idle_timeout.
        """
        check_config = self.context.inner_config.get("check", {})
        ssh_config = check_config.get("ssh_manager", {})
        max_per_node = ssh_config.get("max_connections_per_node", 5)
        idle_timeout = ssh_config.get("idle_timeout", 300)
        ssh_manager = SSHConnectionManager(
            self.context,
            max_connections_per_node=max_per_node,
            idle_timeout=idle_timeout,
        )
        self.context.set_variable("check_ssh_manager", ssh_manager)
        self.stdio.verbose(
            "SSHConnectionManager init: max_per_node={0}, idle_timeout={1}".format(
                max_per_node, idle_timeout
            )
        )

    def handle(self):
        """
        Main entry point for check execution.

        Resolves input (tasks/package), loads tasks, executes concurrently, exports report.
        """
        try:
            input_tasks, package_name = self._resolve_input_options()
            if self._should_skip_obproxy():
                return

            self._prepare_report_output()
            self._load_and_filter_tasks(input_tasks, package_name)
            self.stdio.verbose("tasks is {0}".format(self.tasks.keys()))
            return self.__execute()
        except Exception as e:
            self.stdio.error("Get package tasks failed: {0}".format(e))
            self.stdio.verbose(traceback.format_exc())
            raise Exception("Internal error: {0}".format(e))

    def _resolve_input_options(self):
        """
        Resolve input_tasks and package_name from options based on check_target_type.

        Returns:
            tuple: (input_tasks or None, package_name or None)
        """
        input_tasks = None
        package_name = None
        if self.check_target_type == TARGET_OBPROXY:
            input_tasks = Util.get_option(self.options, "obproxy_tasks")
            package_name = Util.get_option(self.options, "obproxy_cases")
        elif self.check_target_type == TARGET_OBSERVER:
            input_tasks = Util.get_option(self.options, "observer_tasks")
            package_name = Util.get_option(self.options, "cases")
        return input_tasks, package_name

    def _should_skip_obproxy(self):
        """Skip obproxy check when cases=build_before."""
        if (
            Util.get_option(self.options, "cases") == CASE_BUILD_BEFORE
            and self.check_target_type == TARGET_OBPROXY
        ):
            self.stdio.print("when cases is build_before, not check obproxy")
            return True
        return False

    def _prepare_report_output(self):
        """Prepare export path and report type from options."""
        if Util.get_option(self.options, "store_dir"):
            self.export_report_path = Util.get_option(self.options, "store_dir")
            self.stdio.verbose(
                "export_report_path overridden to " + self.export_report_path
            )
        self.export_report_path = os.path.expanduser(self.export_report_path)
        if not os.path.exists(self.export_report_path):
            self.stdio.warn("{0} not exists, creating".format(self.export_report_path))
            os.makedirs(self.export_report_path, exist_ok=True)

        # Create timestamp subdir: obdiag_check_YYYYMMDDHHmmss
        ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.export_report_path = os.path.join(
            self.export_report_path, "obdiag_check_{0}".format(ts)
        )
        os.makedirs(self.export_report_path, exist_ok=True)
        self.stdio.verbose("report output dir: " + self.export_report_path)

        if Util.get_option(self.options, "report_type"):
            self.export_report_type = Util.get_option(self.options, "report_type")
            if self.export_report_type not in SUPPORTED_REPORT_TYPES:
                raise Exception(
                    "report_type must be one of: {0}".format(
                        ", ".join(SUPPORTED_REPORT_TYPES)
                    )
                )
        self.stdio.verbose("export_report_path is " + self.export_report_path)

    def _load_and_filter_tasks(self, input_tasks, package_name):
        """
        Load tasks and filter by input_tasks, package_name, or filter package.

        - input_tasks: Match task names by regex pattern (supports ;-separated list)
        - package_name: Load task list from package yaml, then match
        - else: Load all tasks, exclude those matching filter package patterns
        """
        self.tasks = {}
        if input_tasks:
            raw = [p.strip() for p in input_tasks.replace(" ", "").split(";")]
            patterns = [self._strip_task_prefix(p) for p in raw]
            self._load_tasks_by_patterns(patterns)
        elif package_name:
            self.stdio.verbose("package_name is {0}".format(package_name))
            package_patterns = self.get_package_tasks(package_name)
            self._load_tasks_by_patterns(package_patterns)
        else:
            self.stdio.verbose("tasks_package is all")
            self.get_all_tasks()
            filter_patterns = self.get_package_tasks("filter")
            if filter_patterns:
                self._apply_filter(filter_patterns)

        # Pre-filter by OS compatibility before execution
        self._filter_tasks_by_compatibility()

    def _filter_tasks_by_compatibility(self):
        """
        Filter out tasks incompatible with current OS before execution.
        Tasks without supported_os are kept (run on all platforms).
        """
        current_os = self.__get_current_os()
        compatible = {}
        for task_name, task_cls in self.tasks.items():
            try:
                info = task_cls.get_task_info()
                supported = info.get("supported_os")
                if not supported:
                    compatible[task_name] = task_cls
                elif current_os in supported:
                    compatible[task_name] = task_cls
                else:
                    self.stdio.verbose(
                        "Task {0} skipped (requires {1}, current OS: {2})".format(
                            task_name, supported, current_os
                        )
                    )
            except Exception as e:
                self.stdio.warn("get_task_info for {0} failed: {1}, keeping task".format(task_name, e))
                compatible[task_name] = task_cls
        self.tasks = compatible

    def _strip_task_prefix(self, pattern):
        """Strip leading check_target_type (e.g. observer.) from pattern for path consistency."""
        prefix = self.check_target_type + "."
        if pattern.startswith(prefix):
            return pattern[len(prefix):]
        return pattern

    def _load_tasks_by_patterns(self, patterns):
        """
        Load all tasks, then filter to those matching any pattern.

        Args:
            patterns: List of regex patterns or exact task names
        """
        self.get_all_tasks()
        filtered = {}
        for pattern in patterns:
            for task_name, task_cls in self.tasks.items():
                if pattern == task_name or re.match(pattern, task_name):
                    filtered[task_name] = task_cls
        if not filtered:
            raise Exception("no cases matched by *_tasks: {0}".format(patterns))
        self.tasks = filtered
        self.stdio.verbose("filtered tasks: {0}".format(list(self.tasks.keys())))

    def _apply_filter(self, filter_patterns):
        """Exclude tasks that match any filter pattern."""
        new_tasks = {}
        for task_name, task_value in self.tasks.items():
            matched = any(
                re.match(p.strip(), task_name.strip())
                for p in filter_patterns
            )
            if not matched:
                new_tasks[task_name] = task_value
        self.tasks = new_tasks

    def get_all_tasks(self):
        """
        Load all Python check tasks from tasks_base_path.

        Walks directory, imports .py modules, expects module to expose task class/instance
        as attribute matching filename (e.g. python_version.py -> python_version).
        """
        self.stdio.verbose("get all tasks")
        current_path = self.tasks_base_path
        tasks = {}

        for root, _dirs, files in os.walk(current_path):
            for file in files:
                if not file.endswith(".py") or file.startswith("__"):
                    continue
                folder_name = os.path.basename(root)
                task_name = "{0}.{1}".format(folder_name, file[:-3])
                try:
                    DynamicLoading.add_lib_path(root)
                    task_module = DynamicLoading.import_module(file[:-3], self.stdio)
                    attr_name = task_name.split(".")[-1]
                    if task_module is None:
                        self.stdio.error(
                            "{0} import_module failed: module is None".format(
                                task_name
                            )
                        )
                        continue
                    if not hasattr(task_module, attr_name):
                        self.stdio.error(
                            "{0} import_module failed: missing {1}. attrs: {2}".format(
                                task_name,
                                attr_name,
                                [x for x in dir(task_module) if not x.startswith("_")],
                            )
                        )
                        continue
                    tasks[task_name] = getattr(task_module, attr_name)
                except Exception as e:
                    self.stdio.error(
                        "import_module {0} failed: {1}".format(task_name, e)
                    )
                    raise Exception(
                        "import_module {0} failed: {1}".format(task_name, e)
                    )

        if not tasks:
            raise Exception("No tasks found in {0}".format(current_path))
        self.tasks = tasks

    def get_package_tasks(self, package_name):
        """
        Get task list from package configuration file.

        Args:
            package_name: Key in package yaml (e.g. "ad", "filter")

        Returns:
            List of task names or regex patterns. Empty list for "filter" if not defined.
        """
        with open(self.package_file_name, "r", encoding="utf-8") as f:
            package_data = yaml.safe_load(f)
        if package_data is None:
            package_data = {}

        if package_name not in package_data:
            if package_name == "filter":
                return []
            raise Exception("no cases name is {0}".format(package_name))

        tasks = package_data[package_name].get("tasks")
        self.stdio.verbose(
            "by cases name: {0}, get cases: {1}".format(
                package_name, package_data[package_name]
            )
        )
        return tasks if tasks else []

    def __execute_one(self, task_name):
        """
        Execute a single check task.

        - Checks OS compatibility via task's supported_os
        - Inits task (SSH, DB connector from pool), runs execute(), cleans up
        """
        task_instance = None
        try:
            self.stdio.verbose("execute task: {0}".format(task_name))
            report = TaskReport(self.context, task_name)
            task_cls = self.tasks[task_name]

            # OS compatibility check
            task_info = task_cls.get_task_info()
            supported_os = task_info.get("supported_os")
            if supported_os:
                current_os = self.__get_current_os()
                if current_os not in supported_os:
                    self.stdio.verbose(
                        "Task {0} skipped: requires {1}, current OS is {2}".format(
                            task_name, supported_os, current_os
                        )
                    )
                    report.add_warning(
                        "Task skipped: requires OS {0}, current is {1}".format(
                            supported_os, current_os
                        )
                    )
                    return report

            # Version check (skip when ignore_version or build_before)
            if not self.ignore_version:
                version = self.version
                if not version and Util.get_option(
                    self.options, "cases"
                ) != CASE_BUILD_BEFORE:
                    self.stdio.error("can't get version")
                    return report
                self.cluster["version"] = version
                self.stdio.verbose("cluster.version is {0}".format(version))

            # Execute task (SSH connections created in TaskBase.init)
            task_instance = task_cls
            task_instance.init(self.context, report)
            task_instance.execute()
            self.stdio.verbose("execute task end: {0}".format(task_name))
            return report
        except Exception as e:
            self.stdio.error("execute_one Exception: {0}".format(e))
            raise Exception("execute_one Exception: {0}".format(e))
        finally:
            if task_instance and hasattr(task_instance, "cleanup"):
                try:
                    task_instance.cleanup()
                except Exception as cleanup_error:
                    self.stdio.warn("task cleanup error: {0}".format(cleanup_error))

    def __execute(self):
        """
        Execute all tasks concurrently and generate report.

        Uses ThreadPoolExecutor. Failed tasks are caught by __execute_one_safe
        and reported as failed instead of aborting the whole run.
        """
        try:
            task_count = len(self.tasks)
            self.stdio.verbose(
                "execute_all_tasks. count={0}, tasks={1}".format(
                    task_count, list(self.tasks.keys())
                )
            )
            self.report = CheckReport(
                self.context,
                export_report_path=self.export_report_path,
                export_report_type=self.export_report_type,
                report_target=self.check_target_type,
            )

            actual_workers = min(self.max_workers, task_count) if task_count > 0 else 1
            self.stdio.verbose(
                "Starting concurrent execution with {0} workers".format(
                    actual_workers
                )
            )

            task_names = list(self.tasks.keys())
            failed_tasks = []
            completed_count = 0

            # Start progress bar (skip in silent mode)
            if not self.stdio.silent and task_count > 0:
                self.stdio.start_progressbar(
                    "Check tasks",
                    maxval=task_count,
                    widget_type="simple_progress",
                )

            try:
                with ThreadPoolExecutor(max_workers=actual_workers) as executor:
                    future_to_task = {
                        executor.submit(self.__execute_one_safe, name): name
                        for name in task_names
                    }
                    for future in as_completed(future_to_task):
                        task_name = future_to_task[future]
                        try:
                            t_report = future.result()
                            if t_report:
                                self.report.add_task_report(t_report)
                        except Exception as e:
                            failed_tasks.append(task_name)
                            self.stdio.error(
                                "Task {0} failed: {1}".format(task_name, e)
                            )
                        completed_count += 1
                        if not self.stdio.silent:
                            self.stdio.update_progressbar(completed_count)
            finally:
                if not self.stdio.silent:
                    self.stdio.finish_progressbar()

            if failed_tasks:
                self.stdio.warn("The following tasks failed: {0}".format(failed_tasks))

            self.report.export_report()
            return self.report.report_tobeMap()
        except Exception as e:
            self.stdio.error("Report error: {0}".format(e))
            raise Exception("Report error: {0}".format(e))
        finally:
            self.__cleanup()

    def __execute_one_safe(self, task_name):
        """
        Thread-safe wrapper: catches exceptions and returns failed report instead of raising.
        """
        try:
            return self.__execute_one(task_name)
        except Exception as e:
            self.stdio.error(
                "execute_one_safe Exception for task {0}: {1}".format(
                    task_name, e
                )
            )
            report = TaskReport(self.context, task_name)
            report.add_fail("Task execution failed: {0}".format(str(e)))
            return report

    def __get_current_os(self):
        """Return current OS: 'linux', 'darwin', or 'unknown'."""
        import platform
        system = platform.system().lower()
        if system == "linux":
            return "linux"
        if system == "darwin":
            return "darwin"
        return "unknown"

    def __cleanup(self):
        """Cleanup after check execution. Close SSH connection pool."""
        ssh_manager = self.context.get_variable("check_ssh_manager")
        if ssh_manager:
            try:
                ssh_manager.close_all()
            except Exception as e:
                self.stdio.warn("SSH pool cleanup error: {0}".format(e))
        self.stdio.verbose("Check execution cleanup completed")


class CheckOBConnectorPool:
    """
    Connection pool for OceanBase database connections.

    Used by check tasks to avoid creating a new connection per task.
    Tasks get/release connections via get_connection/release_connection.
    """

    def __init__(self, context, max_size, cluster):
        """
        Args:
            context: HandlerContext
            max_size: Max connections in pool
            cluster: Cluster config with db_host, db_port, tenant_sys
        """
        self.max_size = max_size
        self.cluster = cluster
        self.connections = queue.Queue(maxsize=max_size)
        self.stdio = context.stdio
        try:
            for _ in range(max_size):
                conn = OBConnector(
                    context=context,
                    ip=self.cluster.get("db_host"),
                    port=self.cluster.get("db_port"),
                    username=self.cluster.get("tenant_sys", {}).get("user"),
                    password=self.cluster.get("tenant_sys", {}).get("password"),
                    timeout=10000,
                )
                self.connections.put(conn)
            self.stdio.verbose("CheckOBConnectorPool init success")
        except Exception as e:
            self.stdio.error("CheckOBConnectorPool init fail: {0}".format(e))

    def get_connection(self):
        """Get a connection from the pool. Blocks if pool is empty."""
        try:
            return self.connections.get()
        except Exception as e:
            self.stdio.error("get connection fail: {0}".format(e))
            return None

    def release_connection(self, conn):
        """Release a connection back to the pool."""
        if conn is not None:
            try:
                self.connections.put(conn)
            except Exception:
                pass
