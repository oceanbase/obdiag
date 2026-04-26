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
import yaml
import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import signal

from src.common.ob_connector import OBConnector
from src.common.scene import get_version_by_type
from src.common.command import get_obproxy_full_version
from src.common.ssh_client.ssh_connection_manager import SSHConnectionManager
from src.handler.check.check_report import TaskReport, CheckReport
from src.common.tool import Util
from src.common.tool import StringUtils

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_MAX_WORKERS = 6
MAX_DB_POOL_SIZE = 6
TASK_TIMEOUT_SECONDS = 60  # Default timeout for each check task
SUPPORTED_REPORT_TYPES = ("table", "json", "xml", "yaml", "html")
TARGET_OBSERVER = "observer"
TARGET_OBPROXY = "obproxy"
CASE_BUILD_BEFORE = "build_before"
PACKAGE_FILE_SUFFIX = "_check_package.yaml"


# ---------------------------------------------------------------------------
# Worker process state (initialized once per worker via _worker_initializer)
# ---------------------------------------------------------------------------
_worker_context_data = None  # context_data dict set by _worker_initializer
_worker_ssh_manager = None  # SSHConnectionManager shared across tasks in same worker
_worker_ob_pool = None  # CheckOBConnectorPool (size=1) shared across tasks in same worker
_worker_init_errors = []  # Initialization errors collected by _worker_initializer,
# reported to parent via init_warnings on first task execution


def _worker_initializer(context_data):
    """
    Initialize per-worker resources once when the worker process starts.

    Called by ProcessPoolExecutor before any task is executed in this worker.
    Resources created here are reused across all tasks in the same worker process,
    avoiding the overhead of creating/destroying connections per task.

    SSH manager and DB pool are expensive to create (network round-trips).
    Since each worker is single-threaded, a DB pool of size 1 is sufficient.

    Errors are stored in _worker_init_errors rather than printed, so they reach
    the parent process through the task result's init_warnings field and are
    logged via the project's stdio mechanism.
    """
    global _worker_context_data, _worker_ssh_manager, _worker_ob_pool, _worker_init_errors
    _worker_context_data = context_data
    _worker_init_errors = []

    from src.common.context import HandlerContext
    from src.common.stdio import FAKE_IO
    from src.common.ssh_client.ssh_connection_manager import SSHConnectionManager

    # Build a minimal context just for resource initialization
    context = HandlerContext(
        cluster_config=context_data.get('cluster_config'),
        obproxy_config=context_data.get('obproxy_config'),
        inner_config=context_data.get('inner_config'),
        options=context_data.get('options'),
        stdio=FAKE_IO,
    )

    # Create SSH connection manager once per worker (reused across all tasks in this worker)
    try:
        check_config = context_data.get('inner_config', {}).get('check', {})
        ssh_config = check_config.get('ssh_manager', {})
        max_connections = ssh_config.get('max_connections_per_node', 6)
        idle_timeout = ssh_config.get('idle_timeout', 300)
        _worker_ssh_manager = SSHConnectionManager(
            context,
            max_connections_per_node=max_connections,
            idle_timeout=idle_timeout,
        )
    except Exception as e:
        _worker_init_errors.append("SSH manager init failed: {0}".format(e))
        _worker_ssh_manager = None

    # Create DB connection pool with size=1 once per worker.
    # Each worker is single-threaded: tasks run sequentially, so only 1 connection
    # is needed at any given time. Previously this created min(max_workers, 6)
    # connections per worker, resulting in up to max_workers*6 total connections.
    try:
        cluster = context_data.get('cluster_config')
        if cluster and cluster.get('db_host'):
            _worker_ob_pool = CheckOBConnectorPool(context, 1, cluster)
    except Exception as e:
        _worker_init_errors.append("DB pool init failed: {0}".format(e))
        _worker_ob_pool = None


# ---------------------------------------------------------------------------
# Minimal IO adapter for worker subprocesses
# ---------------------------------------------------------------------------
class _WorkerIO:
    """
    Captures stdio calls (warn/error/verbose/print) inside a worker subprocess.

    Worker processes use FAKE_IO by default, which silently discards all output.
    This class records messages so they can be returned to the parent process via
    the task result dict and re-emitted there through the real stdio object.

    Any other stdio method (e.g. start_progressbar) is silently ignored via
    __getattr__, keeping full compatibility with tasks that call arbitrary stdio APIs.
    """

    # Suppress progress bars and interactive output in subprocesses
    silent = True

    def __init__(self):
        self._messages = []

    def warn(self, msg, *args, **kwargs):
        self._messages.append(('warn', str(msg)))

    def error(self, msg, *args, **kwargs):
        self._messages.append(('error', str(msg)))

    def verbose(self, msg, *args, **kwargs):
        self._messages.append(('verbose', str(msg)))

    def print(self, msg, *args, **kwargs):
        self._messages.append(('print', str(msg)))

    def __getattr__(self, item):
        return lambda *a, **kw: None

    def get_messages(self):
        return list(self._messages)


def _execute_task_worker(args):
    """
    Worker function executed in subprocess for each check task.

    SSH manager and DB pool are reused from worker-level globals initialized
    by _worker_initializer. A fresh HandlerContext is created per task to
    prevent cross-task state leakage. Returns serializable report data.

    Args:
        args: Tuple of (task_name, task_module_path, task_attr_name, timeout_seconds)

    Returns:
        dict: Serializable task report with keys:
            - task_name: str
            - normal: list
            - warning: list
            - critical: list
            - fail: list
            - error: str (if exception occurred)
            - init_warnings: list
    """
    task_name, task_module_path, task_attr_name, timeout_seconds = args
    # Propagate any worker-level init errors recorded by _worker_initializer.
    # Drain after first task so subsequent tasks in the same worker don't repeat them.
    init_warnings = list(_worker_init_errors)
    if _worker_init_errors:
        _worker_init_errors.clear()

    # Platform check for SIGALRM (Unix only)
    use_sigalarm = hasattr(signal, 'SIGALRM')
    if use_sigalarm:

        def timeout_handler(signum, frame):
            raise TimeoutError(f"Task {task_name} exceeded {timeout_seconds} seconds timeout")

        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(timeout_seconds)

    task_instance = None
    worker_io = _WorkerIO()  # Safety fallback: ensures except branches can always call get_messages()
    # even if the exception fires before the inner reassignment at line 221.

    try:
        # Import task module in subprocess
        import importlib.util

        spec = importlib.util.spec_from_file_location(task_name, task_module_path)
        task_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(task_module)

        task_cls = getattr(task_module, task_attr_name)
        task_instance = task_cls

        from src.common.context import HandlerContext
        from src.handler.check.check_report import TaskReport

        # Build a fresh context per task to avoid cross-task state leakage.
        # Context creation is lightweight (no I/O); expensive resources
        # (SSH manager, DB pool) come from worker-level globals.
        # Use _WorkerIO instead of FAKE_IO so that task-level warn/error/verbose
        # calls are captured and relayed to the parent's stdio via worker_logs.
        context_data = _worker_context_data
        if context_data is None:
            raise RuntimeError("worker context not initialized — _worker_initializer may have failed before setting _worker_context_data")
        worker_io = _WorkerIO()
        context = HandlerContext(
            cluster_config=context_data.get('cluster_config'),
            obproxy_config=context_data.get('obproxy_config'),
            inner_config=context_data.get('inner_config'),
            options=context_data.get('options'),
            stdio=worker_io,
        )

        # Restore version info cached in parent before task dispatch
        if context_data.get('observer_version'):
            context.set_variable("check_observer_version", context_data['observer_version'])
        if context_data.get('obproxy_version'):
            context.set_variable("check_obproxy_version", context_data['obproxy_version'])
        if context_data.get('obproxy_full_version'):
            context.set_variable("check_obproxy_full_version", context_data['obproxy_full_version'])
        context.set_variable("check_target_type", context_data.get('report_target_type'))

        # Inject shared worker resources into per-task context
        if _worker_ssh_manager is not None:
            context.set_variable("check_ssh_manager", _worker_ssh_manager)
        else:
            init_warnings.append("SSH connection manager not available in this worker")

        if _worker_ob_pool is not None:
            context.set_variable("check_obConnector_pool", _worker_ob_pool)

        # Create report and execute task
        report = TaskReport(context, task_name)

        task_instance.init(context, report)
        task_instance.execute()

        if use_sigalarm:
            signal.alarm(0)

        return {
            'task_name': task_name,
            'normal': [msg.replace('[normal] ', '', 1) if msg.startswith('[normal] ') else msg for msg in report.all_normal()],
            'warning': [msg.replace('[warning] ', '', 1) if msg.startswith('[warning] ') else msg for msg in report.all_warning()],
            'critical': [msg.replace('[critical] ', '', 1) if msg.startswith('[critical] ') else msg for msg in report.all_critical()],
            'fail': [msg.replace('[fail] ', '', 1) if msg.startswith('[fail] ') else msg for msg in report.all_fail()],
            'error': None,
            'init_warnings': init_warnings,
            'worker_logs': worker_io.get_messages(),
        }

    except TimeoutError as e:
        if use_sigalarm:
            signal.alarm(0)
        return {
            'task_name': task_name,
            'normal': [],
            'warning': [],
            'critical': [],
            'fail': [f"Task timeout after {timeout_seconds} seconds"],
            'error': str(e),
            'timeout': True,
            'init_warnings': init_warnings,
            'worker_logs': worker_io.get_messages(),
        }
    except Exception as e:
        if use_sigalarm:
            signal.alarm(0)
        return {
            'task_name': task_name,
            'normal': [],
            'warning': [],
            'critical': [],
            'fail': [f"Task execution failed: {str(e)}"],
            'error': traceback.format_exc(),
            'init_warnings': init_warnings,
            'worker_logs': worker_io.get_messages(),
        }
    finally:
        # Only clean up the task instance; SSH/DB resources belong to the worker
        # process lifecycle and must NOT be destroyed between tasks.
        try:
            if task_instance and hasattr(task_instance, 'cleanup'):
                task_instance.cleanup()
        except Exception:
            pass


class CheckHandler:
    """
    Handler for executing Python check tasks.

    Workflow:
    1. Load tasks from tasks directory (observer/ or obproxy/ under work_path)
    2. Filter tasks by input (--observer_tasks, --cases, or all with filter)
    3. Execute tasks concurrently via ProcessPoolExecutor
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
        self.obproxy_full_version = None
        self._package_data_cache = None
        self.context = context
        self.stdio = context.stdio
        self.report = None
        self.tasks = None
        self._task_paths = {}  # Store module paths for multiprocessing
        self.check_target_type = check_target_type
        self.options = context.options

        # Load config from inner_config
        self._load_config()
        # Validate paths and init version
        self._validate_paths()
        self._init_connection_pool()
        self.context.set_variable("check_target_type", self.check_target_type)

    def _load_config(self):
        """Load configuration from context.inner_config."""
        check_config = self.context.inner_config.get("check", {})
        report_config = check_config.get("report", {})

        # CLI --inner_config may pass numeric values as strings; coerce so min()/arithmetic work.
        _raw_max_workers = check_config.get("max_workers", DEFAULT_MAX_WORKERS)
        try:
            self.max_workers = int(_raw_max_workers)
        except (TypeError, ValueError):
            self.max_workers = DEFAULT_MAX_WORKERS
        if self.max_workers < 1:
            self.max_workers = 1

        _raw_task_timeout = check_config.get("task_timeout_seconds", TASK_TIMEOUT_SECONDS)
        try:
            self.task_timeout_seconds = int(_raw_task_timeout)
        except (TypeError, ValueError):
            self.task_timeout_seconds = TASK_TIMEOUT_SECONDS
        if self.task_timeout_seconds < 1:
            self.task_timeout_seconds = TASK_TIMEOUT_SECONDS
        self.work_path = os.path.expanduser(check_config.get("work_path") or "~/.obdiag/check")
        self.export_report_path = os.path.expanduser(report_config.get("report_path") or "./check_report/")
        self.export_report_type = report_config.get("export_type") or "table"

        self.cluster = self.context.cluster_config
        if self.check_target_type == TARGET_OBSERVER:
            self.nodes = self.context.cluster_config.get("servers")
        elif self.check_target_type == TARGET_OBPROXY:
            self.nodes = (self.context.obproxy_config or {}).get("servers")
        else:
            self.nodes = None

        self.tasks_base_path = os.path.expanduser(os.path.join(self.work_path, "tasks", ""))
        self.input_env = StringUtils.parse_env_display(Util.get_option(self.options, "env")) or {}

        self.stdio.verbose(
            "CheckHandler input. cluster={0}, nodes={1}, "
            "export_report_path={2}, export_report_type={3}, check_target_type={4}, "
            "tasks_base_path={5}, input_env={6}, task_timeout_seconds={7}".format(
                self.cluster.get("ob_cluster_name") or self.cluster.get("obproxy_cluster_name"),
                StringUtils.node_cut_passwd_for_log(self.nodes),
                self.export_report_path,
                self.export_report_type,
                self.check_target_type,
                self.tasks_base_path,
                self.input_env,
                self.task_timeout_seconds,
            )
        )

    def _validate_paths(self):
        """Validate package file and tasks directory exist."""
        if self.check_target_type is None:
            raise Exception("check_target_type is null. Please check the conf")

        # Package file: {work_path}/{observer|obproxy}_check_package.yaml
        self.package_file_name = os.path.expanduser(os.path.join(self.work_path, self.check_target_type + PACKAGE_FILE_SUFFIX))
        if not os.path.exists(self.package_file_name):
            raise Exception("case_package_file {0} does not exist".format(self.package_file_name))
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
        Initialize version info for tasks.

        Note: Connection pools are created in subprocesses when using ProcessPoolExecutor.
        Parent process only fetches version info, not connection pools.
        """
        if Util.get_option(self.options, "cases") == CASE_BUILD_BEFORE:
            self.stdio.warn("check cases is build_before, skip getting version")
            return

        self.version = get_version_by_type(self.context, self.check_target_type, self.stdio)
        self.stdio.verbose("Got {0} version: {1}".format(self.check_target_type, self.version))
        # Cache version in context for tasks and other modules
        if self.check_target_type == TARGET_OBSERVER:
            self.context.set_variable("check_observer_version", self.version)
        elif self.check_target_type == TARGET_OBPROXY:
            self.context.set_variable("check_obproxy_version", self.version)
            # Also pre-fetch obproxy_full_version so worker tasks don't each query it
            try:
                self.obproxy_full_version = get_obproxy_full_version(self.context)
            except Exception as e:
                self.stdio.verbose("get obproxy_full_version failed: {0}".format(e))
                self.obproxy_full_version = None

    def handle(self):
        """
        Main entry point for check execution.

        Resolves input (tasks/package), loads tasks, executes concurrently, exports report.
        """
        try:
            input_tasks, package_name = self._resolve_input_options()
            if self._should_skip_obproxy():
                self.stdio.verbose("Skipping obproxy check (cases=build_before)")
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
        if Util.get_option(self.options, "cases") == CASE_BUILD_BEFORE and self.check_target_type == TARGET_OBPROXY:
            self.stdio.print("when cases is build_before, not check obproxy")
            return True
        return False

    def _prepare_report_output(self):
        """Prepare export path and report type from options."""
        if Util.get_option(self.options, "store_dir"):
            self.export_report_path = Util.get_option(self.options, "store_dir")
            self.stdio.verbose("export_report_path overridden to " + self.export_report_path)
        self.export_report_path = os.path.expanduser(self.export_report_path)
        if not os.path.exists(self.export_report_path):
            self.stdio.warn("{0} not exists, creating".format(self.export_report_path))
            os.makedirs(self.export_report_path, exist_ok=True)

        # Create timestamp subdir: obdiag_check_YYYYMMDDHHmmss
        ts = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        self.export_report_path = os.path.join(self.export_report_path, "obdiag_check_{0}".format(ts))
        os.makedirs(self.export_report_path, exist_ok=True)
        self.stdio.verbose("report output dir: " + self.export_report_path)

        if Util.get_option(self.options, "report_type"):
            self.export_report_type = Util.get_option(self.options, "report_type")
            if self.export_report_type not in SUPPORTED_REPORT_TYPES:
                raise Exception("report_type must be one of: {0}".format(", ".join(SUPPORTED_REPORT_TYPES)))
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
                    self.stdio.verbose("Task {0} skipped (requires {1}, current OS: {2})".format(task_name, supported, current_os))
            except Exception as e:
                self.stdio.warn("get_task_info for {0} failed: {1}, keeping task".format(task_name, e))
                compatible[task_name] = task_cls
        self.tasks = compatible

    def _strip_task_prefix(self, pattern):
        """Strip leading check_target_type (e.g. observer.) from pattern for path consistency."""
        prefix = self.check_target_type + "."
        if pattern.startswith(prefix):
            return pattern[len(prefix) :]
        return pattern

    def _scan_task_file_names(self):
        """
        Walk tasks_base_path and return a mapping of task_name -> (file_path, attr_name, root)
        without importing any module.

        This is a cheap filesystem-only scan used to identify candidate files before
        committing to the import cost. Task names follow the pattern folder.stem.
        """
        candidates = {}
        for root, _dirs, files in os.walk(self.tasks_base_path):
            for file in files:
                if not file.endswith(".py") or file.startswith("__"):
                    continue
                folder_name = os.path.basename(root)
                task_name = "{0}.{1}".format(folder_name, file[:-3])
                attr_name = file[:-3]
                candidates[task_name] = (os.path.join(root, file), attr_name, root)
        return candidates

    def _import_single_task(self, task_name, file_path, attr_name, root=None):
        """
        Import a single task module and register it in self.tasks / self._task_paths.

        Uses importlib.util to load by absolute path, avoiding sys.path / sys.modules
        pollution that DynamicLoading causes when multiple tasks share the same file stem.

        Args:
            task_name: Dotted task name (e.g. "system.cpu_usage")
            file_path: Absolute path to the .py file
            attr_name: Attribute name to look up on the module (equals file stem)
            root:      Unused; kept for call-site compatibility
        """
        import importlib.util as _importlib_util

        try:
            spec = _importlib_util.spec_from_file_location(task_name, file_path)
            task_module = _importlib_util.module_from_spec(spec)
            spec.loader.exec_module(task_module)
            if not hasattr(task_module, attr_name):
                self.stdio.error(
                    "{0} import failed: missing {1}. attrs: {2}".format(
                        task_name,
                        attr_name,
                        [x for x in dir(task_module) if not x.startswith("_")],
                    )
                )
                return
            self.tasks[task_name] = getattr(task_module, attr_name)
            self._task_paths[task_name] = (file_path, attr_name)
        except Exception as e:
            self.stdio.error("import {0} failed: {1}".format(task_name, e))
            raise Exception("import {0} failed: {1}".format(task_name, e))

    def _load_tasks_by_patterns(self, patterns):
        """
        Scan task file names, filter by pattern, then import only matching modules.

        Avoids importing the entire task tree when only a subset is needed.
        Falls back to get_all_tasks() only when the pattern is a broad wildcard
        that cannot be cheaply matched against file names alone.

        Args:
            patterns: List of regex patterns or exact task names
        """
        self.tasks = {}
        self._task_paths = {}

        candidates = self._scan_task_file_names()
        matched_names = set()
        for pattern in patterns:
            for task_name in candidates:
                if pattern == task_name or re.fullmatch(pattern, task_name):
                    matched_names.add(task_name)

        if not matched_names:
            raise Exception("no cases matched by *_tasks: {0}".format(patterns))

        for task_name in matched_names:
            file_path, attr_name, root = candidates[task_name]
            self._import_single_task(task_name, file_path, attr_name, root)

        if not self.tasks:
            raise Exception("no cases matched by *_tasks: {0}".format(patterns))

        self.stdio.verbose("filtered tasks (lazy-loaded): {0}".format(list(self.tasks.keys())))

    def _apply_filter(self, filter_patterns):
        """Exclude tasks that match any filter pattern.

        Patterns use re.fullmatch — the entire task name must match.
        Use 'category.*' to exclude all tasks in a category (e.g. 'sysbench.*').
        """
        new_tasks = {}
        for task_name, task_value in self.tasks.items():
            matched = any(re.fullmatch(p.strip(), task_name.strip()) for p in filter_patterns)
            if not matched:
                new_tasks[task_name] = task_value
        self.tasks = new_tasks

    def get_all_tasks(self):
        """
        Load all Python check tasks from tasks_base_path.

        Walks directory, imports .py modules, expects module to expose task class/instance
        as attribute matching filename (e.g. python_version.py -> python_version).

        Stores both task class and module path for subprocess execution.
        """
        self.stdio.verbose("get all tasks")
        current_path = self.tasks_base_path
        self.tasks = {}
        self._task_paths = {}

        for root, _dirs, files in os.walk(current_path):
            for file in files:
                if not file.endswith(".py") or file.startswith("__"):
                    continue
                folder_name = os.path.basename(root)
                attr_name = file[:-3]
                task_name = "{0}.{1}".format(folder_name, attr_name)
                file_path = os.path.join(root, file)
                self._import_single_task(task_name, file_path, attr_name)

        if not self.tasks:
            raise Exception("No tasks found in {0}".format(current_path))

    def get_package_tasks(self, package_name):
        """
        Get task list from package configuration file.

        The package file is parsed once and cached for the lifetime of this handler.

        Args:
            package_name: Key in package yaml (e.g. "ad", "filter")

        Returns:
            List of task names or regex patterns. Empty list for "filter" if not defined.
        """
        if self._package_data_cache is None:
            with open(self.package_file_name, "r", encoding="utf-8") as f:
                self._package_data_cache = yaml.safe_load(f) or {}

        package_data = self._package_data_cache

        if package_name not in package_data:
            if package_name == "filter":
                return []
            raise Exception("no cases name is {0}".format(package_name))

        tasks = package_data[package_name].get("tasks")
        self.stdio.verbose("by cases name: {0}, get cases: {1}".format(package_name, package_data[package_name]))
        return tasks if tasks else []

    def __execute(self):
        """
        Execute all tasks concurrently and generate report.

        Uses ProcessPoolExecutor for true parallel execution with timeout support.
        Each task runs in its own subprocess with independent connections.
        """
        try:
            task_count = len(self.tasks)
            self.stdio.verbose("execute_all_tasks. count={0}, tasks={1}".format(task_count, list(self.tasks.keys())))
            self.report = CheckReport(
                self.context,
                export_report_path=self.export_report_path,
                export_report_type=self.export_report_type,
                report_target=self.check_target_type,
            )

            actual_workers = min(self.max_workers, task_count) if task_count > 0 else 1
            self.stdio.verbose("Starting concurrent execution with {0} workers, timeout={1}s".format(actual_workers, self.task_timeout_seconds))

            task_names = list(self.tasks.keys())
            failed_tasks = []
            timeout_tasks = []
            completed_count = 0

            # Prepare serializable context data passed once to each worker process
            # via initializer (not repeated per task).
            context_data = {
                'cluster_config': self.context.cluster_config,
                'obproxy_config': self.context.obproxy_config,
                'inner_config': self.context.inner_config,
                'options': self.context.options,
                'observer_version': self.version if self.check_target_type == TARGET_OBSERVER else None,
                'obproxy_version': self.version if self.check_target_type == TARGET_OBPROXY else None,
                'obproxy_full_version': self.obproxy_full_version if self.check_target_type == TARGET_OBPROXY else None,
                'report_target_type': self.check_target_type,
            }

            # Build worker args: (task_name, module_path, attr_name, timeout)
            # context_data is passed once via initializer, not per task.
            worker_args = []
            for name in task_names:
                if name not in self._task_paths:
                    self.stdio.warn("Task {0} has no module path, skipping".format(name))
                    continue
                module_path, attr_name = self._task_paths[name]
                worker_args.append((name, module_path, attr_name, self.task_timeout_seconds))

            actual_task_count = len(worker_args)
            if actual_task_count < task_count:
                self.stdio.warn("Skipped {0} task(s) without module path, will execute {1}".format(task_count - actual_task_count, actual_task_count))

            # Start progress bar (skip in silent mode)
            if not self.stdio.silent and actual_task_count > 0:
                self.stdio.start_progressbar(
                    "Check tasks",
                    maxval=actual_task_count,
                    widget_type="simple_progress",
                )

            executor = None
            try:
                executor = ProcessPoolExecutor(
                    max_workers=actual_workers,
                    initializer=_worker_initializer,
                    initargs=(context_data,),
                )
                future_to_task = {executor.submit(_execute_task_worker, args): args[0] for args in worker_args}

                # Total wall-clock budget: enough for all tasks to run serially at worst case.
                # actual_workers parallelism means real maximum is ceil(tasks/workers) * timeout,
                # but use the conservative upper bound to avoid premature cancellation.
                _total_timeout = actual_task_count * self.task_timeout_seconds + 60
                for future in as_completed(future_to_task, timeout=_total_timeout):
                    task_name = future_to_task[future]
                    try:
                        result = future.result(timeout=self.task_timeout_seconds + 5)
                        if result:
                            # Check if task reported timeout from worker
                            if result.get('timeout'):
                                timeout_tasks.append(task_name)
                                self.stdio.warn("Task {0} timed out after {1} seconds".format(task_name, self.task_timeout_seconds))

                            # Log init warnings for troubleshooting (SSH/DB init failures in subprocess)
                            init_warnings = result.get('init_warnings', [])
                            if init_warnings:
                                self.stdio.warn("Task {0} init warnings: {1}".format(task_name, init_warnings))

                            # Reconstruct TaskReport from serializable result
                            t_report = TaskReport(self.context, task_name)
                            for msg in result.get('normal', []):
                                t_report.add_normal(msg)
                            for msg in result.get('warning', []):
                                t_report.add_warning(msg)
                            for msg in result.get('critical', []):
                                t_report.add_critical(msg)
                            for msg in result.get('fail', []):
                                t_report.add_fail(msg)
                            self.report.add_task_report(t_report)

                            if result.get('error'):
                                self.stdio.verbose("Task {0} error: {1}".format(task_name, result['error']))

                            # Replay task-level stdio calls captured by _WorkerIO in subprocess.
                            # Skip messages originating from TaskReport (add_warning/add_critical/
                            # add_normal/add_fail) since those already surface via the report dict
                            # and would cause duplicate output.
                            _REPORT_PREFIXES = ('report warning:', 'report critical:', 'report normal:', 'report fail:')
                            _level_map = {'error': self.stdio.error, 'warn': self.stdio.warn, 'verbose': self.stdio.verbose, 'print': self.stdio.verbose}
                            for log_level, log_msg in result.get('worker_logs', []):
                                if any(log_msg.startswith(p) for p in _REPORT_PREFIXES):
                                    continue
                                emit = _level_map.get(log_level, self.stdio.verbose)
                                emit("[{0}] {1}".format(task_name, log_msg))

                            # Per-task completion log for troubleshooting slow/hanging tasks
                            self.stdio.verbose("Task {0} completed ({1}/{2})".format(task_name, completed_count + 1, actual_task_count))

                    except TimeoutError:
                        # Main process timeout - worker may still be running
                        timeout_tasks.append(task_name)
                        self.stdio.warn("Task {0} timed out after {1} seconds (worker process terminated)".format(task_name, self.task_timeout_seconds))
                        # Add timeout report
                        t_report = TaskReport(self.context, task_name)
                        t_report.add_fail("Task timeout after {0} seconds (worker process terminated)".format(self.task_timeout_seconds))
                        self.report.add_task_report(t_report)
                        self.stdio.verbose("Task {0} timed out ({1}/{2})".format(task_name, completed_count + 1, actual_task_count))
                    except Exception as e:
                        failed_tasks.append(task_name)
                        self.stdio.error("Task {0} failed: {1}".format(task_name, e))
                        self.stdio.verbose("Task {0} completed with exception ({1}/{2})".format(task_name, completed_count + 1, actual_task_count))
                    completed_count += 1
                    if not self.stdio.silent:
                        self.stdio.update_progressbar(completed_count)

            except TimeoutError:
                # as_completed overall timeout - some futures not completed
                self.stdio.warn("Overall execution timeout (as_completed) after {0}s, terminating remaining tasks".format(self.task_timeout_seconds * actual_task_count + 60))
                # Identify tasks that didn't complete and update progress bar
                uncompleted_count = 0
                uncompleted_names = []
                for future, task_name in future_to_task.items():
                    if not future.done():
                        timeout_tasks.append(task_name)
                        uncompleted_names.append(task_name)
                        t_report = TaskReport(self.context, task_name)
                        t_report.add_fail("Task timeout after {0} seconds (worker process terminated)".format(self.task_timeout_seconds))
                        self.report.add_task_report(t_report)
                        uncompleted_count += 1
                if uncompleted_names:
                    self.stdio.warn("Tasks not completed due to overall timeout: {0}".format(uncompleted_names))
                # Update progress bar for uncompleted tasks
                if uncompleted_count > 0 and not self.stdio.silent:
                    completed_count += uncompleted_count
                    self.stdio.update_progressbar(completed_count)

            finally:
                # Force shutdown executor and terminate all worker processes
                if executor:
                    try:
                        # shutdown(wait=False) will terminate running processes
                        executor.shutdown(wait=False, cancel_futures=True)
                    except TypeError:
                        # Python 3.8 doesn't support cancel_futures
                        executor.shutdown(wait=False)

                if not self.stdio.silent:
                    self.stdio.finish_progressbar()

            if failed_tasks:
                self.stdio.warn("The following tasks failed: {0}".format(failed_tasks))
            if timeout_tasks:
                self.stdio.warn("The following tasks timed out: {0}".format(timeout_tasks))

            self.stdio.verbose(
                "Check execution finished. completed={0}, failed={1}, timeout={2}. Exporting report to {3}".format(
                    actual_task_count - len(failed_tasks) - len(timeout_tasks),
                    len(failed_tasks),
                    len(timeout_tasks),
                    self.export_report_path,
                )
            )
            self.report.export_report()
            self.stdio.verbose("Report exported to {0}".format(self.report.get_report_path()))
            return self.report.report_tobeMap()
        except Exception as e:
            self.stdio.error("Report error: {0}".format(e))
            raise Exception("Report error: {0}".format(e))
        finally:
            self.__cleanup()

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
        """Cleanup after check execution."""
        # Note: SSH and DB pools are created in subprocesses, not in parent process.
        # Parent process only caches version info.
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
        """Get a connection from the pool. Raises queue.Empty after 30s if pool is exhausted."""
        try:
            return self.connections.get(timeout=30)
        except queue.Empty:
            self.stdio.error("get_connection timed out: pool exhausted or connection not released")
            return None
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
