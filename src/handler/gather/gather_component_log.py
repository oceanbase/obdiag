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
@time: 2024/11/8
@file: gather_component_log.py
@desc: Component log gathering - main entry point
"""

import copy
import datetime
import os
import shutil
import tarfile
import threading
import traceback

from prettytable import PrettyTable
from src.common.constant import const
from src.common.tool import FileUtil, TimeUtils
from src.common.result_type import ObdiagResult
from src.handler.base_shell_handler import BaseShellHandler
from src.handler.gather.plugins.redact import Redact
from src.handler.gather.gather_log import (
    ObserverGatherLogOnNode,
    ObproxyGatherLogOnNode,
    OmsGatherLogOnNode,
)


class GatherComponentLogHandler(BaseShellHandler):
    """Main handler for component log gathering"""

    # Component handler mapping
    COMPONENT_HANDLERS = {
        "observer": ObserverGatherLogOnNode,
        "obproxy": ObproxyGatherLogOnNode,
        "oms": OmsGatherLogOnNode,
    }

    # Log scope configuration for each component
    LOG_SCOPE_CONFIG = {
        "observer": ObserverGatherLogOnNode.LOG_SCOPES,
        "obproxy": ObproxyGatherLogOnNode.LOG_SCOPES,
        "oms": OmsGatherLogOnNode.LOG_SCOPES,
    }

    # Default configuration constants
    DEFAULT_FILE_NUMBER_LIMIT = 20
    DEFAULT_FILE_SIZE_LIMIT = 2 * 1024 * 1024 * 1024  # 2GB
    DEFAULT_SINCE_MINUTES = 30
    DEFAULT_THREAD_NUMS = 3

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.all_files = None
        self.gather_tuples = None
        self.oms_component_id = None
        self.redact_dir = None
        self.gather_log_conf_dict = None
        self.thread_nums = None
        self.oms_log_path = None
        self.is_scene = None
        self.inner_config = None
        self.stdio = None
        self.context = None
        self.target = None
        self.from_option = None
        self.to_option = None
        self.since_option = None
        self.scope = None
        self.grep = None
        self.store_dir = None
        self.temp_dir = None
        self.redact = None
        self.nodes = None
        self.result = ObdiagResult(ObdiagResult.SUCCESS_CODE, data={})

    def init(self, context, *args, **kwargs):
        try:
            self.context = context
            self.stdio = self.context.stdio
            self.inner_config = self.context.inner_config
            self.target = kwargs.get('target', None)
            self.from_option = kwargs.get('from_option', None)
            if self.from_option:
                self.from_option = self.from_option.strip()
            self.to_option = kwargs.get('to_option', None)
            if self.to_option:
                self.to_option = self.to_option.strip()
            self.since_option = kwargs.get('since', None)
            self.scope = kwargs.get('scope', None)
            if isinstance(self.scope, bool):
                self.scope = "all"
            self.grep = kwargs.get('grep', None)
            self.store_dir = kwargs.get('store_dir', None)
            self.temp_dir = kwargs.get('temp_dir', const.GATHER_LOG_TEMPORARY_DIR_DEFAULT)
            self.redact = kwargs.get('redact', None)
            self.nodes = kwargs.get('nodes', None)
            self.is_scene = kwargs.get('is_scene', False)
            self.oms_log_path = kwargs.get('oms_log_path', None)
            self.thread_nums = kwargs.get('thread_nums', self.DEFAULT_THREAD_NUMS)
            self.oms_component_id = kwargs.get('oms_component_id', None)
            self.recent_count = kwargs.get('recent_count', 0)
            if self.recent_count is None:
                self.recent_count = 0
            try:
                self.recent_count = int(self.recent_count)
            except (ValueError, TypeError):
                self.recent_count = 0

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
                "file_number_limit": self.file_number_limit,
                "file_size_limit": self.file_size_limit,
                "oms_component_id": self.oms_component_id,
                "recent_count": self.recent_count,
            }

        except Exception as e:
            self.stdio.error("init GatherComponentLogHandler failed, error: {0}".format(str(e)))
            self.result = ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="init GatherComponentLogHandler failed, error: {0}".format(str(e)))

    def __check_option(self):
        """Validate and process input options"""
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
        """Validate target option"""
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
        """Validate and create store directory"""
        if self.store_dir is None:
            self.store_dir = "./"

        if not os.path.exists(self.store_dir):
            self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(self.store_dir)))
            os.makedirs(os.path.abspath(self.store_dir))

        if not self.is_scene:
            target_dir = "obdiag_gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp()))
            self.store_dir = os.path.join(self.store_dir, target_dir)
            if not os.path.exists(self.store_dir):
                os.makedirs(self.store_dir)

        self.stdio.verbose("store_dir rebase: {0}".format(self.store_dir))

    def __check_nodes(self):
        """Validate and get nodes configuration"""
        if not self.nodes:
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
        """Validate scope option"""
        log_scope_list = self.LOG_SCOPE_CONFIG.get(self.target, {})

        if self.scope is None or self.scope == "" or self.scope == "all":
            self.scope = log_scope_list
        else:
            if isinstance(self.scope, str):
                self.scope = self.scope.strip()
            else:
                raise Exception("scope option can only be string")

            if self.scope not in log_scope_list:
                raise Exception("scope option can only be {0}, the {1} just support {2}".format(self.scope, self.target, list(log_scope_list.keys())))
            self.scope = {self.scope: log_scope_list[self.scope]}

    def __check_grep(self):
        """Validate grep option"""
        if self.grep:
            if isinstance(self.grep, str):
                self.grep = [self.grep.strip()]
            elif not isinstance(self.grep, list):
                self.grep = [str(self.grep)]

    def __check_time_options(self):
        """Validate and process time options"""
        now_time = datetime.datetime.now()
        time_format = '%Y-%m-%d %H:%M:%S'

        # Case 1: Both from and to options provided
        if self.from_option is not None and self.to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(self.from_option)
                to_timestamp = TimeUtils.parse_time_str(self.to_option)
            except Exception:
                raise Exception('Error: Datetime is invalid. Must be in format "yyyy-mm-dd hh:mm:ss". from_datetime={0}, to_datetime={1}'.format(self.from_option, self.to_option))

            if to_timestamp <= from_timestamp:
                raise Exception('Error: from datetime is larger than to datetime, please check.')
            self.from_time_str = self.from_option
            self.to_time_str = self.to_option
        else:
            # Case 2 & 3: Calculate time range based on since_option or default
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime(time_format)
            if self.since_option:
                since_seconds = TimeUtils.parse_time_length_to_sec(self.since_option)
                self.from_time_str = (now_time - datetime.timedelta(seconds=since_seconds)).strftime(time_format)
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=self.DEFAULT_SINCE_MINUTES)).strftime(time_format)

        # Print time range info
        self.__print_time_range_info()

    def __print_time_range_info(self):
        """Print time range information"""
        if self.recent_count > 0:
            self.stdio.print('gather log with recent_count: {0} (most recent {0} files per log type)'.format(self.recent_count))
        else:
            # Only show default message when no time options are provided at all
            if self.from_option is None and self.to_option is None and self.since_option is None:
                self.stdio.print('No time option provided, default processing is based on the last {0} minutes'.format(self.DEFAULT_SINCE_MINUTES))
            self.stdio.print('gather log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))

    def __check_redact(self):
        """Validate redact option"""
        if self.redact and self.redact != "":
            if isinstance(self.redact, str):
                self.redact = [r.strip() for r in self.redact.split(",") if r.strip()]
            elif not isinstance(self.redact, list):
                self.redact = [str(self.redact)]

    def __check_inner_config(self):
        """Load configuration from inner_config"""
        if self.inner_config is None:
            self.file_number_limit = self.DEFAULT_FILE_NUMBER_LIMIT
            self.file_size_limit = self.DEFAULT_FILE_SIZE_LIMIT
            self.config_path = None
        else:
            basic_config = self.inner_config.get('obdiag', {}).get('basic', {})
            self.file_number_limit = int(basic_config.get("file_number_limit", self.DEFAULT_FILE_NUMBER_LIMIT))
            file_size_limit_str = basic_config.get("file_size_limit")
            if file_size_limit_str:
                self.file_size_limit = int(FileUtil.size(file_size_limit_str))
            else:
                self.file_size_limit = self.DEFAULT_FILE_SIZE_LIMIT
            self.config_path = basic_config.get('config_path')

        self.stdio.verbose("file_number_limit: {0}, file_size_limit: {1}".format(self.file_number_limit, self.file_size_limit))

    def __check_thread_nums(self):
        """Validate thread_nums option"""
        if self.thread_nums is None or not isinstance(self.thread_nums, int) or self.thread_nums <= 0:
            # Safely get thread_nums from config, handle None inner_config
            config_thread_nums = None
            if self.inner_config:
                config_thread_nums = self.inner_config.get("obdiag", {}).get("gather", {}).get("thread_nums")
            self.thread_nums = int(config_thread_nums) if config_thread_nums else self.DEFAULT_THREAD_NUMS
        self.stdio.verbose("thread_nums: {0}".format(self.thread_nums))

    def handle(self):
        """Main handle logic"""
        try:
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

                self.stdio.verbose("gather_tuples: {0}".format(self.gather_tuples))
                summary_tuples = self.__get_overall_summary(self.gather_tuples)
                self.stdio.print(summary_tuples)

                with open(os.path.join(self.store_dir, "result_summary.txt"), 'a', encoding='utf-8') as fileobj:
                    fileobj.write(summary_tuples.get_string())

            except Exception as e:
                self.stdio.exception(e)
                self.stdio.verbose("gather log error: {0}".format(e))
                self.stdio.stop_loading("failed")
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="gather log error: {0}".format(str(e)))
            else:
                self.stdio.stop_loading("succeed")

            # Check result
            if os.path.exists(os.path.join(self.store_dir, "result_summary.txt")):
                last_info = "For result details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(self.store_dir, "result_summary.txt"))
                self.stdio.print(last_info)
            else:
                self.stdio.warn("No log file is gathered, please check the gather log config")
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="gather log failed, please check the gather log config or check obdiag log")

            # Handle redact if specified
            if self.redact and len(self.redact) > 0:
                return self.__handle_redact()

            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.store_dir})

        except Exception as e:
            self.stdio.verbose(traceback.format_exc())
            self.stdio.error("gather log failed: {0}".format(str(e)))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="gather log failed: {0}".format(str(e)))

    def __clear_node_ssh_client(self, node):
        """Remove ssh_client from node dict (will be rebuilt in handler)"""
        excluded_keys = {"ssh_client", "ssher"}
        if excluded_keys & set(node.keys()):
            return {k: v for k, v in node.items() if k not in excluded_keys}
        return node

    def __execute_tasks_parallel(self, tasks):
        """Execute tasks in parallel with thread pool"""
        file_queue = []
        pool_sema = threading.BoundedSemaphore(value=self.thread_nums)

        def handle_from_node(task):
            with pool_sema:
                task.handle()

        for task in tasks:
            file_thread = threading.Thread(target=handle_from_node, args=(task,))
            file_thread.start()
            file_queue.append(file_thread)

        self.stdio.verbose("file_queue len: {0}".format(len(file_queue)))

        for task_thread in file_queue:
            task_thread.join()

        self.stdio.verbose("all tasks finished")

    def __handle_redact(self):
        """Handle redact processing"""
        self.stdio.start_loading("gather redact start")
        try:
            self.stdio.verbose("redact_option is {0}".format(self.redact))
            redact_dir = "{0}_redact".format(self.store_dir)
            self.redact_dir = redact_dir
            all_files = self.open_all_file()
            self.stdio.verbose(all_files)
            redact = Redact(self.context, self.store_dir, redact_dir)
            redact.redact_files(self.redact, all_files)
            self.stdio.print("redact success the log save on {0}".format(self.redact_dir))
            self.__delete_all_files_in_tar()
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": redact_dir, "redact_dir": self.redact_dir})
        except Exception as e:
            self.stdio.exception(e)
            self.stdio.error("redact failed {0}".format(e))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="redact failed {0}".format(e))
        finally:
            self.stdio.stop_loading("succeed")

    def __get_overall_summary(self, node_summary_tuple):
        """Generate overall summary from all node summary tuples"""
        summary_tb = PrettyTable()
        summary_tb.title = "Gather {0} Log Summary on {1}".format(self.target, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        self.stdio.verbose("node_summary_tuple: {0}".format(node_summary_tuple))
        summary_tb.field_names = ["Node", "Status", "Size", "info"]

        try:
            for tup in node_summary_tuple:
                summary_tb.add_row([tup["node"], tup["success"], tup["file_size"], tup["info"]])
        except Exception as e:
            self.stdio.verbose(traceback.format_exc())
            self.stdio.error("gather log __get_overall_summary failed: {0}".format(str(e)))

        return summary_tb

    def open_all_file(self):
        """Open all gathered tar files for redact processing"""
        all_files = {}
        if not self.gather_tuples:
            raise Exception("summary_tuples is None. can't open all file")

        for tup in self.gather_tuples:
            if not tup["file_path"] or len(tup["file_path"]) == 0 or not os.path.exists(tup["file_path"]):
                self.stdio.verbose("file_path is None or not exists, can't open file")
                continue

            try:
                file_path = tup["file_path"]
                self.stdio.verbose("open file {0}".format(tup["file_path"]))
                extract_path = os.path.dirname(file_path)

                with tarfile.open(file_path, 'r:gz') as tar:
                    # Security check: filter out files with path traversal
                    safe_members = []
                    for member in tar.getmembers():
                        member_path = os.path.normpath(member.name)
                        if member_path.startswith('..') or os.path.isabs(member_path):
                            self.stdio.warn("Skipping potentially unsafe path: {0}".format(member.name))
                            continue
                        safe_members.append(member)

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
        """Delete extracted files after redact"""
        for item in os.listdir(self.store_dir):
            item_path = os.path.join(self.store_dir, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
        return True
