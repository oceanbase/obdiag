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
Component log gathering handler module - Collects log files from multiple nodes for OceanBase components.

This module provides the GatherComponentLogHandler class for:
- Collecting logs from observer, obproxy, and oms components
- Supporting log collection by time range, log type (scope), keyword filtering, etc.
- Using multi-threaded concurrent collection to improve efficiency
- Supporting log redaction (sensitive data masking)
- Automatically packaging collected log files and generating summary reports

Main Classes:
    GatherComponentLogHandler: Component log gathering handler, inherits from BaseHandler

Usage Example:
    handler = GatherComponentLogHandler()
    handler.init(context, target="observer", since="30m", scope="all")
    result = handler.handle()

@time: 2024/11/8
@file: gather_component_log.py
@desc: Component log gathering - main entry point (Migrated to BaseHandler)
"""

import copy
import datetime
import os
import shutil
import tarfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback

# Note: PrettyTable import removed - using BaseHandler._generate_summary_table instead
from src.common.base_handler import BaseHandler
from src.common.constant import const
from src.common.tool import FileUtil, TimeUtils
from src.common.result_type import ObdiagResult
from src.handler.gather.plugins.redact import Redact
from src.handler.gather.gather_log import (
    ObserverGatherLogOnNode,
    ObproxyGatherLogOnNode,
    OmsGatherLogOnNode,
)


class GatherComponentLogHandler(BaseHandler):
    """
    Component log gathering handler - Collects log files from multiple nodes for OceanBase components.

    Main features of this handler:
    1. Supports collecting logs from observer, obproxy, and oms components
    2. Supports log collection by time range, log type (scope), keyword filtering, etc.
    3. Uses multi-threaded concurrent collection to improve efficiency
    4. Supports log redaction (sensitive data masking)
    5. Automatically packages collected log files and generates summary reports

    Workflow:
    1. Initialization: Validate and parse input parameters (target, time range, scope, etc.)
    2. Create tasks: Create log gathering tasks for each node
    3. Parallel execution: Use thread pool to concurrently execute all node log gathering tasks
    4. Aggregate results: Collect results from all nodes and generate summary report
    5. Optional redaction: If redact parameter is specified, perform redaction on collected logs

    Usage Example:
        handler = GatherComponentLogHandler()
        handler.init(context, target="observer", since="30m", scope="all")
        result = handler.handle()
    """

    # Component handler mapping: Maps component names to their corresponding log gathering handler classes
    # Each component has a dedicated handler class to handle component-specific log paths and formats
    COMPONENT_HANDLERS = {
        "observer": ObserverGatherLogOnNode,  # OceanBase database service node log gatherer
        "obproxy": ObproxyGatherLogOnNode,  # OceanBase proxy node log gatherer
        "oms": OmsGatherLogOnNode,  # OceanBase management service log gatherer
    }

    # Log scope configuration: Defines supported log types (scope) for each component
    # For example, observer may support log types like "election", "rootservice", "observer", etc.
    LOG_SCOPE_CONFIG = {
        "observer": ObserverGatherLogOnNode.LOG_SCOPES,
        "obproxy": ObproxyGatherLogOnNode.LOG_SCOPES,
        "oms": OmsGatherLogOnNode.LOG_SCOPES,
    }

    # Default configuration constants
    DEFAULT_FILE_NUMBER_LIMIT = 20  # Default maximum number of files to collect per log type
    DEFAULT_FILE_SIZE_LIMIT = 2 * 1024 * 1024 * 1024  # Default total size limit for collected logs: 2GB
    DEFAULT_SINCE_MINUTES = 30  # Default time range: last 30 minutes (when no time parameter is specified)
    DEFAULT_THREAD_NUMS = 3  # Default number of concurrent threads: 3 threads

    def _init(
        self, target=None, from_option=None, to_option=None, since=None, scope=None, grep=None, store_dir=None, temp_dir=None, redact=None, nodes=None, is_scene=False, oms_log_path=None, thread_nums=None, oms_component_id=None, recent_count=0, **kwargs
    ):
        """
        Subclass initialization method - Initialize all parameters and configurations required for log gathering.

        This method will:
        1. Save all input parameters to instance variables
        2. Call __check_option() to validate and normalize all parameters
        3. Build gather_log_conf_dict configuration dictionary for use by subsequent tasks

        Args:
            target (str, optional): Target component type, options: 'observer', 'obproxy', 'oms'. Defaults to 'observer'
            from_option (str, optional): Start time in format "yyyy-mm-dd hh:mm:ss". Used together with to_option
            to_option (str, optional): End time in format "yyyy-mm-dd hh:mm:ss". Used together with from_option
            since (str, optional): Relative time range, e.g., "30m" (30 minutes), "2h" (2 hours). Lower priority than from/to
            scope (str, optional): Log type scope, e.g., "all", "election", "rootservice", etc. Defaults to "all"
            grep (str or list, optional): Keyword filter, only collect log lines containing specified keywords
            store_dir (str, optional): Log storage directory. Defaults to timestamp directory under current directory
            temp_dir (str, optional): Temporary directory for temporarily storing logs on remote nodes. Defaults to temp directory in config
            redact (str or list, optional): Redaction keyword list, comma-separated. Logs will be redacted after collection
            nodes (list, optional): Specify list of nodes to collect from. If not specified, read all nodes for the component from config file
            is_scene (bool, optional): Whether in scene mode. In scene mode, no new timestamp directory will be created
            oms_log_path (str, optional): OMS component-specific log path configuration
            thread_nums (int, optional): Number of concurrent threads. Defaults to config value or DEFAULT_THREAD_NUMS
            oms_component_id (str, optional): OMS component ID, used to filter OMS logs
            recent_count (int, optional): Number of recent files, only collect the most recent N files per log type. Defaults to 0 (no limit)
            **kwargs: Other optional parameters

        Raises:
            Exception: Raised when parameter validation fails, error information will be saved in self.result
        """
        self.all_files = None
        self.gather_tuples = None
        self.oms_component_id = oms_component_id
        self.redact_dir = None
        self.gather_log_conf_dict = None
        self.thread_nums = thread_nums
        self.oms_log_path = oms_log_path
        self.is_scene = is_scene
        self.target = target
        self.from_option = from_option.strip() if from_option else None
        self.to_option = to_option.strip() if to_option else None
        self.since_option = since
        self.scope = scope
        if isinstance(self.scope, bool):
            self.scope = "all"
        self.grep = grep
        self.store_dir = store_dir
        self.temp_dir = temp_dir or const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.redact = redact
        self.nodes = nodes

        # Process recent_count
        if recent_count is None:
            recent_count = 0
        try:
            self.recent_count = int(recent_count)
        except (ValueError, TypeError):
            self.recent_count = 0

        # Initialize file limits as instance variables (not properties)
        # These will be set in __check_inner_config()
        self._file_number_limit = None
        self._file_size_limit = None

        # Initialize result
        self.result = ObdiagResult(ObdiagResult.SUCCESS_CODE, data={})

        try:
            self.__check_option()

            # Build config dict for gather log on node
            self.gather_log_conf_dict = {
                "target": self.target,
                "tmp_dir": self.temp_dir,
                "scope": self.scope,
                "grep": self.grep,
                "store_dir": self.store_dir,
                "from_time": self.from_time_str,
                "to_time": self.to_time_str,
                "file_number_limit": self._file_number_limit,
                "file_size_limit": self._file_size_limit,
                "oms_component_id": self.oms_component_id,
                "recent_count": self.recent_count,
            }

        except Exception as e:
            self._log_error(f"init GatherComponentLogHandler failed, error: {str(e)}")
            self.result = ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data=f"init GatherComponentLogHandler failed, error: {str(e)}")

    def init(self, context, *args, **kwargs):
        """
        Initialization method - Compatibility wrapper for existing initialization call patterns.

        This method first calls the parent class's init method to complete basic initialization
        (setting context, stdio, etc.), then calls _init method to complete subclass-specific initialization.

        Args:
            context: Handler context object containing config, stdio, cluster config, etc.
            *args: Positional arguments (currently unused)
            **kwargs: Keyword arguments that will be passed to _init method

        Returns:
            self: Returns self to support method chaining

        Example:
            handler = GatherComponentLogHandler()
            handler.init(context, target="observer", since="30m")
        """
        # Call BaseHandler.init first
        super().init(context, **kwargs)
        # Then call _init with kwargs
        self._init(**kwargs)
        return self

    def __check_option(self):
        """
        Validate and process all input options.

        This method calls various validation methods in sequence to validate, normalize,
        and fill default values for input parameters. If any validation fails, an exception is raised.

        Validation order:
        1. target - Validate target component type
        2. store_dir - Validate and create storage directory
        3. nodes - Validate and get node list
        4. scope - Validate log type scope
        5. grep - Validate keyword filter parameter
        6. time_options - Validate and process time range parameters
        7. redact - Validate redaction parameter
        8. inner_config - Load limit parameters from internal config file
        9. thread_nums - Validate number of concurrent threads

        Raises:
            Exception: Raised when any parameter validation fails
        """
        self.__check_target()
        self.__check_store_dir()
        self.__check_nodes()
        self.__check_scope()
        self.__check_grep()
        self.__check_time_options()
        self.__check_redact()
        self.__check_inner_config()
        self.__check_thread_nums()

    def __check_target(self):
        """
        Validate target component type parameter.

        If target is not specified, defaults to 'observer'.
        Validates that target must be a string type and must be one of the supported component types.

        Raises:
            Exception: Raised when target is not a string type or not in the allowed component list

        Supported component types:
            - 'observer': OceanBase database service node
            - 'obproxy': OceanBase proxy node
            - 'oms': OceanBase management service node
        """
        if self.target is None or self.target == "":
            self.target = 'observer'
        else:
            self.target = self.target.lower().strip()

        if not isinstance(self.target, str):
            raise Exception("target option can only be string")

        allowed_targets = set(self.COMPONENT_HANDLERS.keys())
        if self.target not in allowed_targets:
            raise Exception(f"Invalid target option: '{self.target}'. Allowed values are: {', '.join(allowed_targets)}")

    def __check_store_dir(self):
        """
        Validate and create log storage directory.

        If store_dir is not specified, defaults to current directory "./".
        Uses BaseHandler's template method to handle base directory path (supports ~ expansion, etc.).

        If not in scene mode (is_scene=False), creates a timestamped subdirectory under the base directory,
        format: obdiag_gather_YYYYMMDDHHmmss, to distinguish logs collected at different times.

        If in scene mode (is_scene=True), uses the base directory directly without creating a timestamp subdirectory.

        Note: If the directory does not exist, it will be created automatically.
        """
        if self.store_dir is None:
            self.store_dir = "./"

        # In scene mode the caller (e.g. analyze_log) passes the exact output dir; do not override
        # with context.options.store_dir so that tar files are written under the run directory.
        if self.is_scene:
            base_store_dir = os.path.abspath(os.path.expanduser(self.store_dir))
            if not os.path.exists(base_store_dir):
                os.makedirs(base_store_dir, exist_ok=True)
        else:
            base_store_dir = self._init_store_dir(default=self.store_dir)

        if not self.is_scene:
            # Non-scene mode: Create timestamped subdirectory
            target_dir = "obdiag_gather_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp()))
            self.store_dir = os.path.join(base_store_dir, target_dir)
            if not os.path.exists(self.store_dir):
                os.makedirs(self.store_dir, exist_ok=True)
        else:
            # Scene mode: Use base directory directly
            self.store_dir = base_store_dir

        self._log_verbose(f"store_dir rebase: {self.store_dir}")

    def __check_nodes(self):
        """
        Validate and get node configuration.

        If user does not specify nodes parameter, reads corresponding node list from config file
        based on target component type:
        - observer -> cluster_config.servers
        - obproxy -> obproxy_config.servers
        - oms -> oms_config.servers

        If nodes is specified, uses the user-provided node list directly.

        Raises:
            Exception: Raised when unable to get node config by target, or node list is empty
        """
        if not self.nodes:
            # Get node list from different configs based on component type
            target_config_map = {
                'observer': lambda: self.context.cluster_config.get("servers"),
                'obproxy': lambda: self.context.obproxy_config.get("servers"),
                'oms': lambda: self.context.oms_config.get("servers"),
            }
            config_getter = target_config_map.get(self.target)
            if config_getter is None:
                raise Exception("can not get nodes by target: {0}".format(self.target))
            self.nodes = config_getter()

        if not self.nodes:
            raise Exception("can not get nodes by target: {0}, nodes is empty.".format(self.target))

    def __check_scope(self):
        """
        Validate log type scope (scope) parameter.

        scope is used to specify which types of logs to collect. For example, observer component may support:
        - "election": Election logs
        - "rootservice": RootService logs
        - "observer": Observer main logs
        - "all": All log types (default)

        If scope is None, empty string, or "all", collects all log types supported by the component.
        Otherwise, validates that scope must be one of the log types supported by the component.

        Raises:
            Exception: Raised when scope is not a string type, or not in the component's supported log type list
        """
        log_scope_list = self.LOG_SCOPE_CONFIG.get(self.target, {})

        if self.scope is None or self.scope == "" or self.scope == "all":
            # Not specified or specified as "all": Collect all log types
            self.scope = log_scope_list
        else:
            if isinstance(self.scope, str):
                self.scope = self.scope.strip()
            else:
                raise Exception("scope option can only be string")

            # Validate that scope is in the component's supported log type list
            if self.scope not in log_scope_list:
                raise Exception("scope option can only be {0}, the {1} just support {2}".format(self.scope, self.target, list(log_scope_list.keys())))
            # Convert to dictionary format, only including the specified log type
            self.scope = {self.scope: log_scope_list[self.scope]}

    def __check_grep(self):
        """
        Validate keyword filter (grep) parameter.

        grep is used to filter log content, only collecting log lines containing specified keywords.
        Supports single keyword (string) or multiple keywords (list).

        Processing logic:
        - If string, convert to single-element list
        - If list, keep unchanged
        - If other type, convert to string and put in list
        - If None, keep unchanged (no filtering)
        """
        if self.grep:
            if isinstance(self.grep, str):
                self.grep = [self.grep.strip()]
            elif not isinstance(self.grep, list):
                self.grep = [str(self.grep)]

    def __check_time_options(self):
        """
        Validate and process time range parameters.

        Supports three ways to specify time range (priority from high to low):
        1. from_option + to_option: Specify absolute time range
           Format: "yyyy-mm-dd hh:mm:ss", e.g., "2024-01-01 10:00:00"
        2. since_option: Specify relative time range
           Format: Time length string, e.g., "30m" (30 minutes), "2h" (2 hours), "1d" (1 day)
        3. Default: If none specified, use last DEFAULT_SINCE_MINUTES (30) minutes

        Processing logic:
        - If both from and to are provided, validate format and use directly
        - If only since is provided, calculate time range from current time backwards
        - If none provided, use default 30-minute time range

        Raises:
            Exception: Raised when time format is invalid, or from time is greater than to time
        """
        # Case 1: Both from and to options provided
        if self.from_option is not None and self.to_option is not None:
            # Validate time format
            try:
                from_timestamp = TimeUtils.parse_time_str(self.from_option)
                to_timestamp = TimeUtils.parse_time_str(self.to_option)
            except Exception:
                raise Exception('Error: Datetime is invalid. Must be in format "yyyy-mm-dd hh:mm:ss". from_datetime={0}, to_datetime={1}'.format(self.from_option, self.to_option))

            # Validate time range reasonableness
            if to_timestamp <= from_timestamp:
                raise Exception('Error: from datetime is larger than to datetime, please check.')
            self.from_time_str = self.from_option
            self.to_time_str = self.to_option
        else:
            # Case 2 & 3: Use BaseHandler template method for time range initialization
            # Set options temporarily for template method
            original_from = self._get_option('from') if hasattr(self, '_get_option') else None
            original_to = self._get_option('to') if hasattr(self, '_get_option') else None
            original_since = self._get_option('since') if hasattr(self, '_get_option') else None

            # Temporarily set options for template method
            if not hasattr(self, 'context') or not hasattr(self.context, 'options'):
                # Fallback to manual calculation if context not available
                now_time = datetime.datetime.now()
                time_format = '%Y-%m-%d %H:%M:%S'
                self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime(time_format)
                if self.since_option:
                    # Use since parameter to calculate time range
                    since_seconds = TimeUtils.parse_time_length_to_sec(self.since_option)
                    self.from_time_str = (now_time - datetime.timedelta(seconds=since_seconds)).strftime(time_format)
                else:
                    # Use default time range (30 minutes)
                    self.from_time_str = (now_time - datetime.timedelta(minutes=self.DEFAULT_SINCE_MINUTES)).strftime(time_format)
            else:
                # Use BaseHandler template method
                self._init_time_range()
                self.from_time_str = TimeUtils.timestamp_to_str(self.from_time)
                self.to_time_str = TimeUtils.timestamp_to_str(self.to_time)

        # Print time range info
        self.__print_time_range_info()

    def __print_time_range_info(self):
        """
        Print time range information.

        Outputs corresponding prompt information based on different parameter combinations:
        - If recent_count is used, display recent file count mode
        - If no time parameters are specified, prompt that default 30-minute time range is used
        - Display actual time range used (from_time and to_time)

        Uses a flag to prevent duplicate output.
        """
        # Use a flag to prevent duplicate output
        if hasattr(self, '_time_range_info_printed'):
            return
        self._time_range_info_printed = True

        if self.recent_count > 0:
            # Use recent file count mode
            self._log_info(f'gather log with recent_count: {self.recent_count} (most recent {self.recent_count} files per log type)')
        else:
            # Only show default message when no time options are provided at all
            if self.from_option is None and self.to_option is None and self.since_option is None:
                self._log_info(f'No time option provided, default processing is based on the last {self.DEFAULT_SINCE_MINUTES} minutes')
            # Display actual time range used
            self._log_info(f'gather log from_time: {self.from_time_str}, to_time: {self.to_time_str}')

    def __check_redact(self):
        """
        Validate redaction (redact) parameter.

        Redaction feature is used to replace sensitive information (such as passwords, IP addresses, etc.)
        with placeholders after collecting logs.

        Supported input formats:
        - String: Multiple keywords separated by commas, e.g., "password,ip,user"
        - List: Keyword list, e.g., ["password", "ip", "user"]
        - Other types: Convert to string and process

        Processing logic:
        - If string, split by comma and remove whitespace
        - If list, keep unchanged
        - If other type, convert to string and put in list
        """
        if self.redact and self.redact != "":
            if isinstance(self.redact, str):
                # String format: Split by comma
                self.redact = [r.strip() for r in self.redact.split(",") if r.strip()]
            elif not isinstance(self.redact, list):
                # Other types: Convert to string and put in list
                self.redact = [str(self.redact)]

    def __check_inner_config(self):
        """
        Load limit parameters from internal configuration file.

        Reads the following limit parameters from config file:
        - file_number_limit: Maximum number of files to collect per log type
        - file_size_limit: Total size limit for collected logs (supports units: B, KB, MB, GB, etc.)
        - config_path: Base configuration file path

        Priority:
        1. If ConfigAccessor (self.config) exists, use it first
        2. Otherwise, read from context.inner_config
        3. If neither exists, use default values

        Note: If file_size_limit is in string format (e.g., "2GB"), it will be converted to bytes first.
        """
        # Use ConfigAccessor if available
        if self.config:
            self._file_number_limit = self.config.gather_file_number_limit
            self._file_size_limit = self.config.gather_file_size_limit
            self.config_path = self.config.basic_config_path
        else:
            # Fallback to direct config access
            if self.context.inner_config is None:
                # Use default values
                self._file_number_limit = self.DEFAULT_FILE_NUMBER_LIMIT
                self._file_size_limit = self.DEFAULT_FILE_SIZE_LIMIT
                self.config_path = None
            else:
                # Read config from inner_config
                basic_config = self.context.inner_config.get('obdiag', {}).get('basic', {})
                self._file_number_limit = int(basic_config.get("file_number_limit", self.DEFAULT_FILE_NUMBER_LIMIT))
                file_size_limit_str = basic_config.get("file_size_limit")
                if file_size_limit_str:
                    # If config is in string format (e.g., "2GB"), convert to bytes
                    self._file_size_limit = int(FileUtil.size(file_size_limit_str))
                else:
                    self._file_size_limit = self.DEFAULT_FILE_SIZE_LIMIT
                self.config_path = basic_config.get('config_path')

        self._log_verbose(f"file_number_limit: {self._file_number_limit}, file_size_limit: {self._file_size_limit}")

    def __check_thread_nums(self):
        """
        Validate number of concurrent threads parameter.

        Number of concurrent threads determines how many threads execute log gathering tasks simultaneously,
        affecting collection efficiency. More threads mean faster collection, but also consume more system resources.

        Priority:
        1. If user specifies thread_nums and it's valid (positive integer), use it directly
        2. Otherwise, read gather.thread_nums from config file
        3. If not in config file either, use default value DEFAULT_THREAD_NUMS (3)

        Note: Thread count cannot be 0 or negative.
        """
        if self.thread_nums is None or not isinstance(self.thread_nums, int) or self.thread_nums <= 0:
            # Use ConfigAccessor if available
            if self.config:
                self.thread_nums = self.config.gather_thread_nums
            else:
                # Fallback to direct config access
                config_thread_nums = None
                if self.context.inner_config:
                    config_thread_nums = self.context.inner_config.get("obdiag", {}).get("gather", {}).get("thread_nums")
                self.thread_nums = int(config_thread_nums) if config_thread_nums else self.DEFAULT_THREAD_NUMS
        self._log_verbose(f"thread_nums: {self.thread_nums}")

    def handle(self) -> ObdiagResult:
        """
        Main handle logic - Core method for executing log gathering.

        This method is responsible for the entire log gathering process:
        1. Validate initialization status
        2. Create log gathering tasks for each node
        3. Execute all tasks in parallel (using thread pool)
        4. Collect results from all nodes
        5. Generate summary report and save to file
        6. Optional: Execute log redaction processing

        Execution flow:
        - Get corresponding handler class based on target component type
        - Create an independent gathering task for each node (using independent context and stdio to avoid thread safety issues)
        - Use thread pool to concurrently execute all tasks, display progress bar
        - Collect results from all tasks, generate summary report
        - If redact parameter is specified, perform redaction on collected logs

        Returns:
            ObdiagResult: ObdiagResult object containing execution results
                - On success: SUCCESS_CODE, data contains store_dir (storage directory path)
                - On failure: Corresponding error code and error message

        Raises:
            Exception: Raised when initialization fails, unsupported target component, or errors occur during execution
        """
        self._validate_initialized()

        try:
            # If initialization phase already failed, return error result directly
            if not self.result.is_success():
                return self.result

            self.gather_tuples = []
            tasks = []
            self.stdio.start_loading("gather start")

            try:
                # Get handler class for target component
                handler_class = self.COMPONENT_HANDLERS.get(self.target)
                if not handler_class:
                    raise Exception(f"Unsupported target: {self.target}")

                # Create tasks for each node
                for node in self.nodes:
                    # Create a shallow copy of context to avoid thread safety issues
                    # Each task gets its own stdio instance
                    task_context = copy.copy(self.context)
                    task_context.stdio = self.stdio.sub_io()

                    # Clear ssh_client from node (will be rebuilt in handler)
                    clear_node = self.__clear_node_ssh_client(node)
                    tasks.append(handler_class(task_context, clear_node, self.gather_log_conf_dict))

                # Execute tasks in parallel
                self.__execute_tasks_parallel(tasks)

                # Collect results
                for task in tasks:
                    self.gather_tuples.append(task.get_result())

                self._log_verbose(f"gather_tuples: {self.gather_tuples}")
                summary_tuples = self.__get_overall_summary(self.gather_tuples)
                # Note: _generate_summary_table already logs the table, so we don't need to log again

                # Save summary report to file
                with open(os.path.join(self.store_dir, "result_summary.txt"), 'w', encoding='utf-8') as fileobj:
                    # summary_tuples is now a string from _generate_summary_table
                    fileobj.write(summary_tuples)

            except Exception as e:
                self.stdio.exception(e)
                self._log_verbose(f"gather log error: {e}")
                self.stdio.stop_loading("failed")
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=f"gather log error: {str(e)}")
            else:
                self.stdio.stop_loading("succeed")

            # Check result
            if os.path.exists(os.path.join(self.store_dir, "result_summary.txt")):
                last_info = f"For result details, please run cmd \033[32m' cat {os.path.join(self.store_dir, 'result_summary.txt')} '\033[0m\n"
                self._log_info(last_info)
            else:
                self._log_warn("No log file is gathered, please check the gather log config")
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="gather log failed, please check the gather log config or check obdiag log")

            # Handle redact if specified
            if self.redact and len(self.redact) > 0:
                return self.__handle_redact()

            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.store_dir})

        except Exception as e:
            self._log_verbose(traceback.format_exc())
            return self._handle_error(e)

    def __clear_node_ssh_client(self, node):
        """
        Clear SSH client objects from node dictionary.

        To avoid thread safety issues, SSH client objects that may exist in the node dictionary
        need to be cleared before creating tasks. These objects will be recreated in the task handler.

        Args:
            node (dict): Node configuration dictionary, may contain ssh_client or ssher keys

        Returns:
            dict: Copy of node dictionary after clearing SSH client objects
        """
        excluded_keys = {"ssh_client", "ssher"}
        if excluded_keys & set(node.keys()):
            return {k: v for k, v in node.items() if k not in excluded_keys}
        return node

    def __execute_tasks_parallel(self, tasks):
        """
        Execute all log gathering tasks in parallel.

        Uses ThreadPoolExecutor thread pool to concurrently execute log gathering tasks for multiple nodes,
        improving collection efficiency. Log gathering is I/O-intensive (mainly network transfer and file I/O),
        suitable for multi-threaded concurrency.

        Execution flow:
        1. Calculate actual number of threads to use (not exceeding task count and configured thread count)
        2. Start progress bar to display collection progress
        3. Submit all tasks to thread pool
        4. Wait for tasks to complete, update progress bar for each completed task
        5. Handle task execution exceptions (single task failure does not affect other tasks)
        6. Finish progress bar

        Args:
            tasks (list): List of log gathering tasks to execute, each task corresponds to one node

        Note:
            - Single task failure will not interrupt execution of other tasks
            - Task failure errors will be logged
            - Progress bar will automatically close after all tasks complete (whether successful or failed)
        """
        if not tasks:
            self._log_warn("No tasks to execute for log gathering")
            return

        task_count = len(tasks)
        actual_workers = min(self.thread_nums, task_count)
        self._log_verbose(f"Executing {task_count} tasks with {actual_workers} workers")

        # Start progress bar
        if self.stdio and task_count > 0:
            progress_text = f"Gathering {self.target} logs"
            self.stdio.start_progressbar(progress_text, maxval=task_count, widget_type='simple_progress')

        completed_count = 0
        try:
            with ThreadPoolExecutor(max_workers=actual_workers) as executor:
                # Submit all tasks to thread pool
                future_to_task = {executor.submit(task.handle): task for task in tasks}

                # Wait for tasks to complete, use as_completed to process results in completion order
                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        future.result()
                    except Exception as e:
                        # Single task failure does not affect other tasks
                        self._log_error(f"Task execution failed: {e}")
                        self.stdio.exception(f"Error in task execution: {e}")
                    finally:
                        # Update progress bar
                        completed_count += 1
                        if self.stdio:
                            self.stdio.update_progressbar(completed_count)

            self._log_verbose("all tasks finished")
        finally:
            # Finish progress bar
            if self.stdio:
                self.stdio.finish_progressbar()

    def __handle_redact(self):
        """
        Handle log redaction operation.

        If user specified redact parameter during initialization, redaction processing will be executed
        after log collection completes. Redaction replaces sensitive information (such as passwords, IP addresses, etc.)
        in logs with placeholders.

        Processing flow:
        1. Create redacted log storage directory (add "_redact" suffix to original directory name)
        2. Open all collected log files (extract tar files)
        3. Call Redact class to perform redaction on log files
        4. Delete extracted temporary files
        5. Return redacted directory path

        Returns:
            ObdiagResult: ObdiagResult object containing redacted storage directory
                - data.store_dir: Redacted log storage directory
                - data.redact_dir: Same as store_dir (compatibility field)

        Raises:
            Exception: Raised when errors occur during redaction processing
        """
        self.stdio.start_loading("gather redact start")
        try:
            self._log_verbose(f"redact_option is {self.redact}")
            # Create redacted directory (add "_redact" suffix to original directory name)
            redact_dir = f"{self.store_dir}_redact"
            self.redact_dir = redact_dir
            # Open all collected log files (extract tar files)
            all_files = self.open_all_file()
            self._log_verbose(str(all_files))
            # Execute redaction processing
            redact = Redact(self.context, self.store_dir, redact_dir)
            redact.redact_files(self.redact, all_files)
            self._log_info(f"redact success the log save on {self.redact_dir}")
            # Delete extracted temporary files
            self.__delete_all_files_in_tar()
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": redact_dir, "redact_dir": self.redact_dir})
        except Exception as e:
            self.stdio.exception(e)
            return self._handle_error(e)
        finally:
            self.stdio.stop_loading("succeed")

    def __get_overall_summary(self, node_summary_tuple):
        """
        Generate summary report for all nodes.

        Aggregates collection results from each node into a table-formatted report, containing:
        - Node: Node name or IP address
        - Status: Collection status (Success/Fail)
        - Size: Size of collected log files
        - info: Detailed information (such as file path, error messages, etc.)

        Uses BaseHandler's template method to generate formatted table string.

        Args:
            node_summary_tuple (list): List of collection results from all nodes, each element is a dictionary containing:
                - node: Node name
                - success: Collection status
                - file_size: File size
                - info: Detailed information

        Returns:
            str: Formatted summary report string (table format)
        """
        self._log_verbose(f"node_summary_tuple: {node_summary_tuple}")

        # Prepare data for template method
        headers = ["Node", "Status", "Size", "info"]
        rows = []
        for tup in node_summary_tuple:
            rows.append([tup["node"], tup["success"], tup["file_size"], tup["info"]])

        # Use BaseHandler template method
        title = "Gather {0} Log Summary on {1}".format(self.target, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return self._generate_summary_table(headers, rows, title)

    def open_all_file(self):
        """
        Open all collected tar compressed files for redaction processing.

        Iterates through collection results from all nodes, extracts each tar.gz file to temporary directory.
        Extracted file paths are saved for use in redaction processing.

        Security measures:
        - Filter out files containing path traversal (..) or absolute paths to prevent security vulnerabilities
        - Only extract safe file paths

        Returns:
            dict: Dictionary with tar file paths as keys and lists of extracted file paths as values
                Format: {tar_file_path: [extracted_file_path1, extracted_file_path2, ...]}

        Raises:
            Exception: Raised when gather_tuples is empty

        Note:
            - If a tar file does not exist or extraction fails, skip that file and continue processing other files
            - Extracted files are saved in the directory where the tar file is located
        """
        all_files = {}
        if not self.gather_tuples:
            raise Exception("summary_tuples is None. can't open all file")

        for tup in self.gather_tuples:
            if not tup["file_path"] or len(tup["file_path"]) == 0 or not os.path.exists(tup["file_path"]):
                self._log_verbose("file_path is None or not exists, can't open file")
                continue

            try:
                file_path = tup["file_path"]
                self._log_verbose(f"open file {tup['file_path']}")
                extract_path = os.path.dirname(file_path)

                with tarfile.open(file_path, 'r:gz') as tar:
                    # Security check: filter out files with path traversal
                    safe_members = []
                    for member in tar.getmembers():
                        member_path = os.path.normpath(member.name)
                        # Security check: Filter out files containing path traversal or absolute paths
                        if member_path.startswith('..') or os.path.isabs(member_path):
                            self.stdio.warn("Skipping potentially unsafe path: {0}".format(member.name))
                            continue
                        safe_members.append(member)

                    # Extract safe files
                    tar.extractall(path=extract_path, members=safe_members)
                    extracted_files = [m.name for m in safe_members]
                    self.stdio.verbose("extracted_files: {0}".format(extracted_files))
                    extracted_files_new = [os.path.join(self.store_dir, f) for f in extracted_files]
                    all_files[file_path] = extracted_files_new

            except Exception as e:
                self.stdio.verbose(traceback.format_exc())
                self.stdio.error("gather open_all_file failed: {0}".format(str(e)))
                continue

        self.all_files = all_files
        return all_files

    def __delete_all_files_in_tar(self):
        """
        Delete extracted temporary files.

        After redaction processing completes, deletes temporary files and directories extracted from tar files.
        Only keeps compressed tar files and redacted files.

        Returns:
            bool: Always returns True

        Note:
            - Only deletes directories, not files (tar files need to be kept)
            - Uses shutil.rmtree to recursively delete directories and all their contents
        """
        for item in os.listdir(self.store_dir):
            item_path = os.path.join(self.store_dir, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
        return True
