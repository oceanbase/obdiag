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
@desc: Handler for executing Python check tasks (Migrated to BaseHandler)
"""

import os
import queue
import traceback
import re
import oyaml as yaml
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from src.common.base_handler import BaseHandler
from src.common.result_type import ObdiagResult
from src.common.ob_connector import OBConnector
from src.common.scene import get_version_by_type
from src.handler.check.check_exception import CheckException
from src.handler.check.check_report import TaskReport, CheckReport, CheckReportException
from src.common.tool import Util, DynamicLoading
from src.common.tool import StringUtils, TimeUtils


class CheckHandler(BaseHandler):
    """
    Handler for executing Python check tasks.

    This handler:
    1. Loads Python check tasks from the tasks directory
    2. Executes tasks based on specified cases or filters
    3. Generates check reports in various formats
    """

    def _init(self, check_target_type="observer", **kwargs):
        """Subclass initialization"""
        self.version = None
        self.report = None
        self.tasks = None

        # Use ConfigAccessor for configuration access
        self.max_workers = self.config.check_max_workers
        self.work_path = self.config.check_work_path
        self.export_report_type = self.config.check_report_type
        self.ignore_version = self.config.check_ignore_version

        # Initialize export_report_path with default from config (will be updated in handle() if store_dir option is provided)
        self.export_report_path = self.config.check_report_path

        self.cluster = self.context.cluster_config
        self.check_target_type = check_target_type

        # Get nodes based on target type
        if check_target_type == "observer":
            self.nodes = self.context.cluster_config.get("servers")
        elif check_target_type == "obproxy":
            self.nodes = self.context.obproxy_config.get("servers")
        else:
            self.nodes = []

        self.tasks_base_path = os.path.join(self.work_path, "tasks")

        # Get environment option
        env_option = self._get_option('env')
        self.input_env = StringUtils.parse_env_display(env_option) if env_option else {}

        # Log initialization info
        self._log_verbose(
            f"CheckHandler input. ignore_version={self.ignore_version}, "
            f"cluster={self.cluster.get('ob_cluster_name') or self.cluster.get('obproxy_cluster_name')}, "
            f"nodes={StringUtils.node_cut_passwd_for_log(self.nodes)}, "
            f"export_report_path={self.export_report_path}, "
            f"export_report_type={self.export_report_type}, "
            f"check_target_type={self.check_target_type}, "
            f"tasks_base_path={self.tasks_base_path}, "
            f"input_env={self.input_env}"
        )

        # Validate check_target_type
        if not self.check_target_type:
            raise CheckException("check_target_type is null. Please check the conf")

        # case_package_file
        case_package_file = os.path.join(self.work_path, f"{self.check_target_type}_check_package.yaml")
        case_package_file = os.path.expanduser(case_package_file)
        if os.path.exists(case_package_file):
            self.package_file_name = case_package_file
        else:
            raise CheckException(f"case_package_file {case_package_file} is not exist")
        self._log_verbose(f"case_package_file is {self.package_file_name}")

        # checker tasks_base_path
        tasks_base_path = os.path.join(self.tasks_base_path, self.check_target_type)
        tasks_base_path = os.path.expanduser(tasks_base_path)
        if os.path.exists(tasks_base_path):
            self.tasks_base_path = tasks_base_path
        else:
            raise CheckException(f"tasks_base_path {tasks_base_path} is not exist")
        self._log_verbose(f"tasks_base_path is {self.tasks_base_path}")

        # Initialize SSH connection manager for connection pooling
        # If not provided, create a new one for this handler
        if not self.ssh_manager:
            from src.common.ssh_connection_manager import SSHConnectionManager

            # Create SSH connection manager with configurable pool size
            # Each node can have up to max_workers connections (but capped at 5 per node)
            max_conn_per_node = min(self.max_workers, 5)
            self.ssh_manager = SSHConnectionManager(max_connections_per_node=max_conn_per_node, idle_timeout=300)
            self._log_verbose(f"[CheckHandler] Created SSH connection manager with {max_conn_per_node} connections per node")
        else:
            self._log_verbose(f"[CheckHandler] Using provided SSH connection manager")

        # Store SSH manager in context for tasks to use
        self.context.set_variable('check_ssh_manager', self.ssh_manager)

        # Get version and setup connection pool
        # Optimized: Get versions once and cache in context for all tasks to reuse
        if self._get_option('cases') != "build_before":
            self.version = get_version_by_type(self.context, self.check_target_type, self.stdio)
            self.cluster["version"] = self.version

            # Cache versions in context for all tasks to reuse (avoid repeated queries)
            self.context.set_variable('check_observer_version', self.version)
            self._log_verbose(f"Cached observer version in context: {self.version}")

            # Get obproxy versions if obproxy nodes exist
            if self.context.obproxy_config.get("servers"):
                from src.common.command import get_obproxy_version, get_obproxy_full_version

                try:
                    obproxy_version = get_obproxy_version(self.context)
                    obproxy_full_version = get_obproxy_full_version(self.context)
                    self.context.set_variable('check_obproxy_version', obproxy_version)
                    self.context.set_variable('check_obproxy_full_version', obproxy_full_version)
                    self._log_verbose(f"Cached obproxy versions in context: {obproxy_version}, {obproxy_full_version}")
                except Exception as e:
                    self._log_warn(f"Failed to get obproxy version: {e}")
                    self.context.set_variable('check_obproxy_version', None)
                    self.context.set_variable('check_obproxy_full_version', None)
            else:
                self.context.set_variable('check_obproxy_version', None)
                self.context.set_variable('check_obproxy_full_version', None)

            # Use global OBConnectionPool instead of CheckOBConnectorPool
            from src.common.ob_connection_pool import OBConnectionPool

            obConnectorPool = None
            try:
                # Connection pool size matches max_workers for optimal concurrency
                pool_size = min(self.max_workers, 12)  # max 12 connections to avoid overloading DB
                obConnectorPool = OBConnectionPool(context=self.context, cluster_config=self.cluster, max_size=pool_size, timeout=30)
            except Exception as e:
                self._log_warn(f"obConnector init error. Error info is {e}")
            finally:
                self.context.set_variable('check_obConnector_pool', obConnectorPool)
        else:
            self._log_warn("check cases is build_before, so don't get version")
            # Set versions to None for build_before cases
            self.context.set_variable('check_observer_version', None)
            self.context.set_variable('check_obproxy_version', None)
            self.context.set_variable('check_obproxy_full_version', None)

    def handle(self) -> ObdiagResult:
        """Main entry point for check execution."""
        self._validate_initialized()

        try:
            package_name = None
            input_tasks = None

            # Get input tasks or package name
            if self.check_target_type == "obproxy":
                input_tasks = self._get_option('obproxy_tasks')
                package_name = self._get_option('obproxy_cases')
            elif self.check_target_type == "observer":
                input_tasks = self._get_option('observer_tasks')
                package_name = self._get_option('cases')

            if self._get_option('cases') == "build_before" and self.check_target_type == "obproxy":
                self._log_info("when cases is build_before, not check obproxy")
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "obproxy check skipped"})

            # Update export_report_path from options using BaseHandler template method
            base_store_dir = self._get_option('store_dir', default='./')
            base_store_dir = os.path.abspath(os.path.expanduser(base_store_dir))

            # Create timestamped subdirectory similar to gather
            target_dir = "obdiag_check_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp()))
            self.export_report_path = os.path.join(base_store_dir, target_dir)

            # Use BaseHandler template method to ensure directory exists
            if not os.path.exists(self.export_report_path):
                os.makedirs(self.export_report_path, exist_ok=True)
            self._log_verbose(f"export_report_path: {self.export_report_path}")

            # Change self.export_report_type
            report_type = self._get_option('report_type')
            if report_type:
                self.export_report_type = report_type
                if self.export_report_type not in ["table", "json", "xml", "yaml", "html"]:
                    raise CheckException("report_type must be table, json, xml, yaml, html")
            self._log_verbose(f"export_report_path is {self.export_report_path}")

            # Optimized: Load tasks on demand instead of loading all tasks
            self.tasks = {}
            if input_tasks:
                input_tasks = input_tasks.replace(" ", "")
                input_tasks = input_tasks.split(";")
                # Optimized: Load only requested tasks instead of all tasks
                for task_pattern in input_tasks:
                    # Try exact match first
                    task_class = self._load_task_by_path(task_pattern)
                    if task_class:
                        self.tasks[task_pattern] = task_class
                    else:
                        # Try pattern match
                        matched_tasks = self._load_tasks_by_pattern(task_pattern)
                        self.tasks.update(matched_tasks)

                if len(self.tasks) == 0:
                    raise CheckException("no cases is check by *_tasks: {0}".format(input_tasks))
                self.stdio.verbose("input_tasks is {0}, loaded {1} tasks".format(input_tasks, len(self.tasks)))
            elif package_name:
                self.stdio.verbose("package_name is {0}".format(package_name))
                # Optimized: Load only tasks in the package
                self.tasks = self.get_tasks_by_package(package_name)
                if len(self.tasks) == 0:
                    raise CheckException(f"No tasks found for package: {package_name}")
            else:
                self.stdio.verbose("tasks_package is all")
                self.get_all_tasks()
                filter_tasks = self.get_package_tasks("filter")
                if len(filter_tasks) > 0:
                    self.tasks = {key: value for key, value in self.tasks.items() if key not in filter_tasks}
                    new_tasks = {}
                    for task_name, task_value in self.tasks.items():
                        filter_tag = False
                        for filter_task in filter_tasks:
                            if re.match(filter_task.strip(), task_name.strip()):
                                filter_tag = True
                        if not filter_tag:
                            new_tasks[task_name] = task_value
                    self.tasks = new_tasks

            # Optimized: Pre-filter tasks by compatibility (version and OS) before execution
            self.tasks = self._filter_tasks_by_compatibility(self.tasks)

            # Resolve task dependencies and determine execution order
            task_execution_order = self._resolve_task_dependencies(self.tasks)
            if task_execution_order:
                # Reorder tasks according to dependencies
                self.tasks = {task_name: self.tasks[task_name] for task_name in task_execution_order if task_name in self.tasks}
                self._log_verbose(f"Task execution order after dependency resolution: {task_execution_order}")

            self._log_verbose(f"tasks after filtering: {list(self.tasks.keys())}")
            result = self.__execute()
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=result)
        except CheckException as e:
            return self._handle_error(e, error_code=ObdiagResult.INPUT_ERROR_CODE)
        except Exception as e:
            self._log_error(f"Get package tasks failed. Error info is {e}")
            self._log_verbose(traceback.format_exc())
            return self._handle_error(e)

    def get_all_tasks(self):
        """Load all Python check tasks from the tasks directory."""
        self._log_verbose("Getting all tasks")
        current_path = self.tasks_base_path
        tasks = {}

        for root, dirs, files in os.walk(current_path):
            for file in files:
                # Only load Python files
                if file.endswith('.py') and not file.startswith('__'):
                    folder_name = os.path.basename(root)
                    task_name = f"{folder_name}.{file.split('.')[0]}"
                    try:
                        DynamicLoading.add_lib_path(root)
                        task_module = DynamicLoading.import_module(file[:-3], self.stdio)
                        attr_name = task_name.split('.')[-1]
                        if task_module is None:
                            self._log_error(f"{task_name} import_module failed: module is None")
                            continue
                        if not hasattr(task_module, attr_name):
                            self._log_error(f"{task_name} import_module failed: missing {attr_name} attribute. " f"Module attrs: {[x for x in dir(task_module) if not x.startswith('_')]}")
                            continue
                        tasks[task_name] = getattr(task_module, attr_name)
                    except Exception as e:
                        self._log_error(f"import_module {task_name} failed: {e}")
                        raise CheckException(f"import_module {task_name} failed: {e}")

        if len(tasks) == 0:
            raise CheckException(f"No tasks found in {current_path}")
        self.tasks = tasks

    def _load_task_by_path(self, task_path):
        """
        Load a single task by full path (e.g., 'observer.system.python_version').

        Args:
            task_path: Full task path like 'observer.system.python_version'

        Returns:
            Task class or None if not found
        """
        parts = task_path.split('.')
        if len(parts) < 2:
            return None

        # tasks_base_path is already e.g. .../tasks/observer, so strip leading observer. if present
        if parts[0] == self.check_target_type:
            parts = parts[1:]
        if len(parts) < 1:
            return None

        # Find the task file
        task_file_name = parts[-1] + '.py'
        task_dir_parts = parts[:-1]

        # Try to find the task file
        search_path = self.tasks_base_path
        for part in task_dir_parts:
            search_path = os.path.join(search_path, part)

        task_file = os.path.join(search_path, task_file_name)

        if not os.path.exists(task_file):
            # Try pattern matching: find files matching the pattern
            return None

        try:
            DynamicLoading.add_lib_path(search_path)
            module_name = parts[-1]
            task_module = DynamicLoading.import_module(module_name, self.stdio)
            if task_module and hasattr(task_module, module_name):
                return getattr(task_module, module_name)
        except Exception as e:
            self._log_error(f"Failed to load task {task_path}: {e}")

        return None

    def _load_tasks_by_pattern(self, task_pattern):
        """
        Load tasks matching a pattern (e.g., 'observer.system.*').

        Args:
            task_pattern: Task name pattern with wildcards

        Returns:
            dict: Task name -> task class mapping
        """
        tasks = {}
        pattern_re = re.compile(task_pattern.replace('*', '.*'))

        # Walk through task directory
        for root, dirs, files in os.walk(self.tasks_base_path):
            for file in files:
                if file.endswith('.py') and not file.startswith('__'):
                    folder_name = os.path.basename(root)
                    task_name = f"{folder_name}.{file.split('.')[0]}"

                    if pattern_re.match(task_name):
                        task_class = self._load_task_by_path(task_name)
                        if task_class:
                            tasks[task_name] = task_class

        return tasks

    def get_tasks_by_package(self, package_name):
        """
        Optimized: Load tasks for a specific package without loading all tasks.

        Args:
            package_name: Package name from package file

        Returns:
            dict: Task name -> task class mapping
        """
        package_tasks = self.get_package_tasks(package_name)

        if not package_tasks:
            return {}

        tasks = {}
        for task_pattern in package_tasks:
            # Try exact match first
            if '.' in task_pattern and '*' not in task_pattern:
                task_class = self._load_task_by_path(task_pattern)
                if task_class:
                    tasks[task_pattern] = task_class
                    continue

            # Try pattern match
            matched_tasks = self._load_tasks_by_pattern(task_pattern)
            tasks.update(matched_tasks)

        return tasks

    def get_package_tasks(self, package_name):
        """Get task list from package configuration file."""
        with open(self.package_file_name, 'r', encoding='utf-8') as file:
            package_file_data = yaml.safe_load(file)
            packege_tasks = package_file_data

        if package_name not in packege_tasks:
            if package_name == "filter":
                return []
            else:
                raise CheckException(f"no cases name is {package_name}")

        self._log_verbose(f"by cases name: {package_name}, get cases: {packege_tasks[package_name]}")
        if packege_tasks[package_name].get("tasks") is None:
            return []
        return packege_tasks[package_name].get("tasks")

    def __execute_one(self, task_name):
        """Execute a single check task."""
        task_instance = None
        try:
            self._log_verbose(f"execute task: {task_name}")
            report = TaskReport(self.context, task_name)

            # Pre-check: verify OS compatibility
            task_instance = self.tasks[task_name]
            task_info = task_instance.get_task_info()
            supported_os = task_info.get("supported_os")

            if supported_os:
                # Check if current OS is supported
                current_os = self.__get_current_os()
                if current_os not in supported_os:
                    self._log_verbose(f"Task {task_name} skipped: requires {supported_os}, current OS is {current_os}")
                    report.add_warning(f"Task skipped: requires OS {supported_os}, current is {current_os}")
                    return report

            if not self.ignore_version:
                version = self.version
                if version or self._get_option('cases') == "build_before":
                    self.cluster["version"] = version
                    self._log_verbose(f"cluster.version is {self.cluster['version']}")

                    # Execute Python task
                    # SSH connections are created in TaskBase.init() for thread safety
                    task_instance = self.tasks[task_name]
                    task_instance.init(self.context, report)
                    task_instance.execute()

                    self._log_verbose(f"execute task end: {task_name}")
                    return report
                else:
                    self._log_error("can't get version")
            else:
                self._log_verbose("ignore version")
                # Execute Python task without version check
                # SSH connections are created in TaskBase.init() for thread safety
                task_instance = self.tasks[task_name]
                task_instance.init(self.context, report)
                task_instance.execute()
                return report
        except Exception as e:
            self._log_error(f"execute_one Exception: {e}")
            raise CheckException(f"execute_one Exception: {e}")
        finally:
            # Cleanup task resources (release connection back to pool and close SSH connections)
            if task_instance and hasattr(task_instance, 'cleanup'):
                try:
                    task_instance.cleanup()
                except Exception as cleanup_error:
                    self._log_warn(f"task cleanup error: {cleanup_error}")

    def __execute(self):
        """Execute all check tasks concurrently and generate report."""
        execution_start_time = time.time()
        task_timings = {}

        try:
            task_count = len(self.tasks.keys())
            self._log_verbose(f"execute_all_tasks. the number of tasks is {task_count}, tasks is {list(self.tasks.keys())}")
            self.report = CheckReport(self.context, export_report_path=self.export_report_path, export_report_type=self.export_report_type, report_target=self.check_target_type)

            # Determine actual worker count (don't use more workers than tasks)
            actual_workers = min(self.max_workers, task_count) if task_count > 0 else 1
            self._log_verbose(f"Starting concurrent execution with {actual_workers} workers")

            # Execute tasks concurrently using ThreadPoolExecutor
            task_names = list(self.tasks.keys())
            failed_tasks = []
            completed_count = 0

            # Start progress bar
            if self.stdio and task_count > 0:
                progress_text = f"Running {self.check_target_type} checks"
                self.stdio.start_progressbar(progress_text, maxval=task_count, widget_type='simple_progress')

            with ThreadPoolExecutor(max_workers=actual_workers) as executor:
                # Submit all tasks
                future_to_task = {executor.submit(self.__execute_one_safe, task_name): task_name for task_name in task_names}

                # Collect results as they complete
                for future in as_completed(future_to_task):
                    task_name = future_to_task[future]
                    task_start_time = time.time()
                    try:
                        t_report = future.result()
                        task_end_time = time.time()
                        task_timings[task_name] = task_end_time - task_start_time

                        if t_report:
                            self.report.add_task_report(t_report)
                    except Exception as e:
                        task_end_time = time.time()
                        task_timings[task_name] = task_end_time - task_start_time
                        failed_tasks.append(task_name)
                        self._log_error(f"Task {task_name} failed with exception: {e}")

                    # Update progress bar
                    completed_count += 1
                    if self.stdio:
                        self.stdio.update_progressbar(completed_count)

            # Finish progress bar
            if self.stdio:
                self.stdio.finish_progressbar()

            # Log performance statistics
            execution_end_time = time.time()
            total_execution_time = execution_end_time - execution_start_time

            self._log_performance_stats(task_timings, total_execution_time, task_count, failed_tasks)

            if failed_tasks:
                self._log_warn(f"The following tasks failed: {failed_tasks}")

            self.report.export_report()
            return self.report.report_tobeMap()
        except CheckReportException as e:
            self._log_error(f"Report error: {e}")
            # Ensure progress bar is finished even on error
            if self.stdio:
                self.stdio.finish_progressbar()
            raise CheckException(f"Report error: {e}")
        except Exception as e:
            # Ensure progress bar is finished even on error
            if self.stdio:
                self.stdio.finish_progressbar()
            raise CheckException(f"Internal error: {e}")
        finally:
            # Ensure progress bar is finished
            if self.stdio:
                self.stdio.finish_progressbar()
            # Cleanup: close SSH connections
            self.__cleanup()

    def __execute_one_safe(self, task_name, max_retries=1):
        """
        Thread-safe wrapper for __execute_one with retry mechanism.

        Args:
            task_name: Name of the task to execute
            max_retries: Maximum number of retries (default: 1)

        Returns:
            TaskReport instance
        """
        for attempt in range(max_retries + 1):
            try:
                return self.__execute_one(task_name)
            except CheckException as e:
                # Don't retry for CheckException (user errors)
                self._log_error(f"execute_one_safe Exception for task {task_name}: {e}")
                report = TaskReport(self.context, task_name)
                report.add_fail(f"Task execution failed: {str(e)}")
                return report
            except Exception as e:
                if attempt < max_retries:
                    self._log_warn(f"Task {task_name} failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                    # Wait before retry with exponential backoff
                    import time

                    time.sleep(0.5 * (attempt + 1))
                else:
                    # Final attempt failed
                    self._log_error(f"Task {task_name} failed after {max_retries + 1} attempts: {e}")
                    report = TaskReport(self.context, task_name)
                    report.add_fail(f"Task execution failed after {max_retries + 1} attempts: {str(e)}")
                    return report

        # Should not reach here, but just in case
        report = TaskReport(self.context, task_name)
        report.add_fail(f"Task execution failed: Unknown error")
        return report

    def __get_current_os(self):
        """
        Get the current operating system type.
        Returns: 'linux', 'darwin' (macOS), or 'unknown'
        """
        import platform

        system = platform.system().lower()
        if system == "linux":
            return "linux"
        elif system == "darwin":
            return "darwin"
        else:
            return "unknown"

    def _version_meets_min(self, current_version, min_version):
        """Check if current version meets minimum requirement."""
        if not current_version or not min_version:
            return True  # Skip check if version info unavailable
        if current_version == min_version:
            return True
        return StringUtils.compare_versions_greater(current_version, min_version)

    def _version_meets_max(self, current_version, max_version):
        """Check if current version is below maximum."""
        if not current_version or not max_version:
            return True  # Skip check if version info unavailable
        if current_version == max_version:
            return True
        return StringUtils.compare_versions_greater(max_version, current_version)

    def _log_performance_stats(self, task_timings, total_time, task_count, failed_tasks):
        """
        Log performance statistics for check execution.

        Args:
            task_timings: Dict of task_name -> execution_time
            total_time: Total execution time
            task_count: Total number of tasks
            failed_tasks: List of failed task names
        """
        if not task_timings:
            return

        # Calculate statistics
        times = list(task_timings.values())
        avg_time = sum(times) / len(times) if times else 0
        max_time = max(times) if times else 0
        min_time = min(times) if times else 0

        # Find slowest and fastest tasks
        slowest_task = max(task_timings.items(), key=lambda x: x[1]) if task_timings else None
        fastest_task = min(task_timings.items(), key=lambda x: x[1]) if task_timings else None

        # Log statistics
        self._log_verbose("=" * 60)
        self._log_verbose("Performance Statistics:")
        self._log_verbose(f"  Total tasks: {task_count}")
        self._log_verbose(f"  Successful: {task_count - len(failed_tasks)}")
        self._log_verbose(f"  Failed: {len(failed_tasks)}")
        self._log_verbose(f"  Total execution time: {total_time:.2f}s")
        self._log_verbose(f"  Average task time: {avg_time:.2f}s")
        self._log_verbose(f"  Min task time: {min_time:.2f}s")
        self._log_verbose(f"  Max task time: {max_time:.2f}s")

        if slowest_task:
            self._log_verbose(f"  Slowest task: {slowest_task[0]} ({slowest_task[1]:.2f}s)")
        if fastest_task:
            self._log_verbose(f"  Fastest task: {fastest_task[0]} ({fastest_task[1]:.2f}s)")

        # Log top 5 slowest tasks
        sorted_tasks = sorted(task_timings.items(), key=lambda x: x[1], reverse=True)
        if len(sorted_tasks) > 1:
            self._log_verbose("  Top 5 slowest tasks:")
            for i, (task_name, task_time) in enumerate(sorted_tasks[:5], 1):
                self._log_verbose(f"    {i}. {task_name}: {task_time:.2f}s")

        self._log_verbose("=" * 60)

    def _resolve_task_dependencies(self, tasks):
        """
        Resolve task dependencies and return execution order using topological sort.

        Args:
            tasks: Dict of task_name -> task_class

        Returns:
            List of task names in execution order, or None if circular dependency detected
        """
        # Build dependency graph
        graph = {}
        for task_name, task_class in tasks.items():
            try:
                task_info = task_class.get_task_info()
                deps = task_info.get("dependencies", [])
                # Filter dependencies to only include tasks that exist
                graph[task_name] = [dep for dep in deps if dep in tasks]
            except Exception as e:
                self._log_warn(f"Failed to get task info for {task_name}: {e}")
                graph[task_name] = []

        # Topological sort using DFS
        execution_order = []
        visited = set()
        visiting = set()

        def visit(task_name):
            if task_name in visiting:
                self._log_error(f"Circular dependency detected involving {task_name}")
                raise CheckException(f"Circular dependency detected involving {task_name}")
            if task_name in visited:
                return

            visiting.add(task_name)

            # Visit dependencies first
            for dep in graph.get(task_name, []):
                if dep in tasks:
                    visit(dep)

            visiting.remove(task_name)
            visited.add(task_name)
            execution_order.append(task_name)

        # Visit all tasks
        for task_name in tasks.keys():
            if task_name not in visited:
                try:
                    visit(task_name)
                except CheckException:
                    # Circular dependency detected, return None to use original order
                    self._log_warn("Circular dependency detected, using original task order")
                    return None

        return execution_order

    def _filter_tasks_by_compatibility(self, tasks):
        """
        Optimized: Filter tasks based on version and OS compatibility before execution.
        This avoids unnecessary task initialization for incompatible tasks.

        Args:
            tasks: dict of task_name -> task_class

        Returns:
            dict: Filtered tasks
        """
        filtered = {}
        current_os = self.__get_current_os()
        filtered_count = 0

        for task_name, task_class in tasks.items():
            try:
                # Get task info without instantiating (just call class method)
                task_info = task_class.get_task_info()

                # OS compatibility check
                supported_os = task_info.get("supported_os")
                if supported_os and current_os not in supported_os:
                    self._log_verbose(f"Task {task_name} skipped: requires {supported_os}, current OS is {current_os}")
                    filtered_count += 1
                    continue

                # Version compatibility check (if version available and not ignored)
                if not self.ignore_version and self.version:
                    min_version = task_info.get("min_ob_version")
                    max_version = task_info.get("max_ob_version")

                    if min_version:
                        if not self._version_meets_min(self.version, min_version):
                            self._log_verbose(f"Task {task_name} skipped: version {self.version} < {min_version}")
                            filtered_count += 1
                            continue

                    if max_version:
                        if not self._version_meets_max(self.version, max_version):
                            self._log_verbose(f"Task {task_name} skipped: version {self.version} > {max_version}")
                            filtered_count += 1
                            continue

                filtered[task_name] = task_class
            except Exception as e:
                self._log_warn(f"Failed to check compatibility for {task_name}: {e}")
                # Include task if compatibility check fails (fail-safe)
                filtered[task_name] = task_class

        if filtered_count > 0:
            self._log_verbose(f"Filtered {filtered_count} incompatible tasks, {len(filtered)} tasks remaining")

        return filtered

    def __cleanup(self):
        """Cleanup all resources after check execution."""
        try:
            # Log connection pool statistics before cleanup
            if hasattr(self, 'ssh_manager') and self.ssh_manager:
                try:
                    ssh_stats = self.ssh_manager.get_stats()
                    self._log_verbose(f"SSH connection pool stats: {ssh_stats}")
                except Exception as e:
                    self._log_verbose(f"Failed to get SSH pool stats: {e}")

                # Cleanup idle connections (connections will be returned by tasks)
                self.ssh_manager.cleanup()
                self._log_verbose("SSH connection pool cleanup completed")

            # Log database connection pool statistics before cleanup
            ob_connector_pool = self.context.get_variable('check_obConnector_pool')
            if ob_connector_pool:
                try:
                    db_stats = ob_connector_pool.get_stats()
                    self._log_verbose(f"Database connection pool stats: {db_stats}")
                except Exception as e:
                    self._log_verbose(f"Failed to get DB pool stats: {e}")

                try:
                    ob_connector_pool.close_all()
                    self._log_verbose("Database connection pool cleanup completed")
                except Exception as e:
                    self._log_warn(f"Database connection pool cleanup error: {e}")

            self._log_verbose("Check execution cleanup completed")
        except Exception as e:
            self._log_warn(f"Cleanup error: {e}")


class CheckOBConnectorPool:
    """
    Legacy connection pool for OceanBase database connections.

    @deprecated: This class is deprecated. Use OBConnectionPool from src.common.ob_connection_pool instead.
    This class is kept for backward compatibility during migration.
    """

    def __init__(self, context, max_size, cluster):
        import warnings

        warnings.warn("CheckOBConnectorPool is deprecated. Use OBConnectionPool from src.common.ob_connection_pool instead.", DeprecationWarning, stacklevel=2)

        self.max_size = max_size
        self.cluster = cluster
        self.connections = queue.Queue(maxsize=max_size)
        self.stdio = context.stdio
        self.stdio.verbose("CheckOBConnectorPool init success!")
        try:
            for i in range(max_size):
                conn = OBConnector(context=context, ip=self.cluster.get("db_host"), port=self.cluster.get("db_port"), username=self.cluster.get("tenant_sys").get("user"), password=self.cluster.get("tenant_sys").get("password"), timeout=10000)
                self.connections.put(conn)
            self.stdio.verbose("CheckOBConnectorPool init success!")
        except Exception as e:
            self.stdio.error("CheckOBConnectorPool init fail! err: {0}".format(e))

    def get_connection(self, timeout=30):
        """Get a connection from the pool with timeout."""
        try:
            return self.connections.get(timeout=timeout)
        except queue.Empty:
            self.stdio.error("get connection timeout after {0}s, pool may be exhausted".format(timeout))
            return None
        except Exception as e:
            self.stdio.error("get connection fail! err: {0}".format(e))
            return None

    def release_connection(self, conn):
        """Release a connection back to the pool."""
        if conn is not None:
            self.connections.put(conn)
        return

    def release(self, conn):
        """Release method for compatibility with OBConnectionPool interface."""
        return self.release_connection(conn)
