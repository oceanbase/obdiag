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
@desc:
"""
import datetime
import os
import re
import tarfile
import threading
import traceback
import uuid
import shutil
import subprocess

from prettytable import PrettyTable
from src.common.command import get_file_size, is_empty_dir
from src.common.constant import const
from src.common.ssh_client.ssh import SshClient
from src.common.tool import FileUtil, TimeUtils
from src.handler.base_shell_handler import BaseShellHandler
from src.handler.gather.plugins.redact import Redact
from src.common.result_type import ObdiagResult


class GatherComponentLogHandler(BaseShellHandler):
    # log_scope_list
    log_scope_list = {
        "observer": {"observer": {"key": "*observer*"}, "rootservice": {"key": "*rootservice*"}, "election": {"key": "*election*"}},
        "obproxy": {
            "obproxy": {"key": "*obproxy*"},
            "obproxy_diagnosis": {"key": "*obproxy_diagnosis*"},
            "obproxy_digest": {"key": "*obproxy_digest*"},
            "obproxy_stat": {"key": "*obproxy_stat*"},
            "obproxy_slow": {"key": "*obproxy_slow*"},
            "obproxy_limit": {"key": "*obproxy_limit*"},
        },
        "oms": {"connector": {"key": "*connector.*"}, "error": {"key": "error"}, "trace.log": {"key": "trace.log"}, "metrics": {"key": "metrics*"}},
        "oms_cdc": {"libobcdc": {"key": "*libobcdc.log*"}, "removed_log_files": {"key": "removed_log_files"}, "store": {"key": "store.log"}},
    }

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
            self.thread_nums = kwargs.get('thread_nums', 3)
            self.oms_component_id = kwargs.get('oms_component_id', None)
            self.recent_count = kwargs.get('recent_count', 0)
            if self.recent_count is None:
                self.recent_count = 0
            try:
                self.recent_count = int(self.recent_count)
            except (ValueError, TypeError):
                self.recent_count = 0
            self.__check_option()
            # build config dict for gather log on node
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
        # target check
        if self.target is None or self.target == "":
            self.target = 'observer'
        else:
            self.target = self.target.lower().strip()
        if not isinstance(self.target, str):
            raise Exception("target option can only be string")
        self.target = self.target.lower().strip()
        allowed_targets = {'observer', 'obproxy', 'oms', 'oms_cdc'}
        if self.target not in allowed_targets:
            raise Exception(f"Invalid target option: '{self.target}'. Allowed values are: {', '.join(allowed_targets)}")

        # check store_dir
        if not os.path.exists(self.store_dir):
            self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(self.store_dir)))
            os.makedirs(os.path.abspath(self.store_dir))
        if self.is_scene is False:
            target_dir = os.path.join("obdiag_gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp())))
            self.store_dir = os.path.join(self.store_dir or "./", target_dir)
            if not os.path.exists(self.store_dir):
                os.makedirs(self.store_dir)
        self.stdio.verbose("store_dir rebase: {0}".format(self.store_dir))

        # check nodes
        if self.nodes is None or len(self.nodes) == 0:
            # if self.nodes not input, use default nodes by self.target
            if self.target == 'observer':
                self.nodes = self.context.cluster_config.get("servers")
            elif self.target == 'obproxy':
                self.nodes = self.context.obproxy_config.get("servers")
            elif self.target == 'oms' or self.target == 'oms_cdc':
                self.nodes = self.context.oms_config.get("servers")
            else:
                raise Exception("can not get nodes by target: {0}".format(self.target))
        if len(self.nodes) == 0:
            raise Exception("can not get nodes by target: {0}, nodes's len is 0.".format(self.target))
        # check scope
        if self.scope is None or self.scope == "" or self.scope == "all":
            self.scope = self.log_scope_list[self.target]
        else:
            if isinstance(self.scope, str):
                self.scope = self.scope.strip()
            else:
                raise Exception("scope option can only be string")
            if self.scope not in self.log_scope_list[self.target]:
                raise Exception("scope option can only be {0},the {1} just support {2}".format(self.scope, self.target, self.log_scope_list))
            self.scope = {self.scope: self.log_scope_list[self.target][self.scope]}
        # check grep
        if self.grep:
            if isinstance(self.grep, list):
                pass
            elif isinstance(self.grep, str):
                self.grep = self.grep.strip()
                self.grep = [self.grep]

        # check since from_option and to_option
        from_timestamp = None
        to_timestamp = None
        if self.from_option is not None and self.to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(self.from_option)
                to_timestamp = TimeUtils.parse_time_str(self.to_option)
                self.from_time_str = self.from_option
                self.to_time_str = self.to_option
            except Exception as e:
                raise Exception('Error: Datetime is invalid. Must be in format "yyyy-mm-dd hh:mm:ss". from_datetime={0}, to_datetime={1}'.format(self.from_option, self.to_option))
            if to_timestamp <= from_timestamp:
                raise Exception('Error: from datetime is larger than to datetime, please check.')
        elif (self.from_option is None or self.to_option is None) and self.since_option is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(self.since_option))).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.print('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if self.since_option:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(self.since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))

        # check redact
        if self.redact:
            if self.redact != "" and len(self.redact) != 0:
                if "," in self.redact and isinstance(self.redact, str):
                    self.redact = self.redact.split(",")
                else:
                    self.redact = [self.redact]

        # check inner_config
        if self.inner_config is None:
            self.file_number_limit = 20
            self.file_size_limit = 2 * 1024 * 1024 * 1024
        else:
            basic_config = self.inner_config['obdiag']['basic']
            self.file_number_limit = int(basic_config["file_number_limit"])
            self.file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
            self.config_path = basic_config['config_path']
        self.stdio.verbose("file_number_limit: {0}, file_size_limit: {1}, gather log config_path: {2}".format(self.file_number_limit, self.file_size_limit, self.config_path))

        # check thread_nums
        if self.thread_nums is None or not isinstance(self.thread_nums, int) or self.thread_nums <= 0:
            self.thread_nums = int(self.context.inner_config.get("obdiag", {}).get("gather", {}).get("thread_nums") or 3)
        self.stdio.verbose("thread_nums: {0}".format(self.thread_nums))

    def handle(self):
        try:
            if not self.result.is_success():
                return self.result
            # run on every node
            node_threads = []
            self.gather_tuples = []
            tasks = []
            self.stdio.start_loading("gather start")
            try:
                for node in self.nodes:
                    new_context = self.context
                    new_context.stdio = self.stdio.sub_io()
                    # use Process must delete ssh_client, and GatherLogOnNode will rebuild it.
                    if "ssh_client" in node or "ssher" in node:
                        clear_node = {}
                        for node_param in node:
                            if node_param == "ssh_client" or node_param == "ssher":
                                continue
                            clear_node[node_param] = node[node_param]
                        tasks.append(GatherLogOnNode(new_context, clear_node, self.gather_log_conf_dict))
                    else:
                        tasks.append(GatherLogOnNode(new_context, node, self.gather_log_conf_dict))
                file_queue = []
                pool_sema = threading.BoundedSemaphore(value=self.thread_nums)

                def handle_from_node(task):
                    with pool_sema:
                        task.handle()

                for task in tasks:
                    file_thread = threading.Thread(
                        target=handle_from_node,
                        args=(task,),
                    )
                    file_thread.start()
                    file_queue.append(file_thread)
                self.stdio.verbose("file_queue len: {0}".format(len(file_queue)))
                for task_thread in file_queue:
                    task_thread.join()
                self.stdio.verbose("start get gather_tuples")
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
            finally:
                self.stdio.stop_loading("succeed")

            if os.path.exists(os.path.join(self.store_dir, "result_summary.txt")):
                last_info = "For result details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(self.store_dir, "result_summary.txt"))
                self.stdio.print(last_info)
            else:
                self.stdio.warn("No log file is gathered, please check the gather log config")
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="gather log failed,please check the gather log config or check obdiag log")
            if self.redact and len(self.redact) > 0:
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
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.store_dir})
        except Exception as e:
            self.stdio.verbose(traceback.format_exc())
            self.stdio.error("gather log failed: {0}".format(str(e)))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="gather log failed: {0}".format(str(e)))

    def __get_overall_summary(self, node_summary_tuple):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple: (node, is_err, err_msg, size, consume_time, node_summary) for each node
        :return: a string indicating the overall summary
        """
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
                # 打开 tar.gz 文件
                extract_path = os.path.dirname(file_path)
                with tarfile.open(file_path, 'r:gz') as tar:
                    # get all files in tar
                    tar.extractall(path=extract_path)
                    extracted_files = tar.getnames()
                    self.stdio.verbose("extracted_files: {0}".format(extracted_files))
                    extracted_files_new = []
                    for extracted_file in extracted_files:
                        extracted_files_new.append(os.path.join(self.store_dir, extracted_file))
                    all_files[file_path] = extracted_files_new
            except Exception as e:
                self.stdio.verbose(traceback.format_exc())
                self.stdio.error("gather open_all_filefailed: {0}".format(str(e)))
                continue
        self.all_files = all_files
        return all_files

    def __delete_all_files_in_tar(self):
        for item in os.listdir(self.store_dir):
            item_path = os.path.join(self.store_dir, item)
            if os.path.isdir(item_path):
                shutil.rmtree(item_path)
        return True


class GatherLogOnNode:
    def __init__(self, context, node, config):
        self.context = context
        self.ssh_client = None
        self.stdio = context.stdio
        self.config = config
        self.node = node
        self.target = self.config.get("target")

        # mkdir tmp_dir
        self.tmp_dir = self.config.get("tmp_dir")

        self.scope = self.config.get("scope")
        self.oms_component_id = self.config.get("oms_component_id")
        if self.target == "oms":
            if self.oms_component_id is None:
                raise Exception("gather log on oms, but oms_component_id is None. please check your config")
            if node.get("run_path") is None:
                raise Exception("gather log on oms, but run_path is None. please check your config")
            self.log_path = os.path.join(node.get("run_path"), self.oms_component_id, "logs")
        elif self.target == "oms_cdc":
            if self.oms_component_id is None:
                raise Exception("gather log on oms_cdc, but oms_component_id is None. please check your config")
            obcdc_id = None
            match = re.search(r"\d+\.\d+\.\d+\.\d+-(\d+)", self.oms_component_id)
            if match:
                number = match.group(1)
                obcdc_id = number
            else:
                self.stdio.error("can not get obcdc_id by component_id. please check component_id.")
            self.log_path = os.path.join(node.get("store_path"), "store" + obcdc_id, "log")
        else:
            self.log_path = os.path.join(node.get("home_path"), "log")
        self.stdio.verbose("log_path: {0}".format(self.log_path))

        self.from_time_str = self.config.get("from_time")
        self.to_time_str = self.config.get("to_time")
        self.grep_option = self.config.get("grep")
        self.store_dir = self.config.get("store_dir")
        #
        self.file_number_limit = self.config.get("file_number_limit")
        self.file_size_limit = self.config.get("file_size_limit")
        # Get search_version from inner_config, default is 2
        inner_config = self.context.inner_config if hasattr(self.context, 'inner_config') else None
        if inner_config:
            self.search_version = int(inner_config.get("obdiag", {}).get("gather", {}).get("search_version", 2))
        else:
            self.search_version = 2
        self.stdio.verbose("search_version: {0}".format(self.search_version))
        # Get recent_count from config, default is 0
        self.recent_count = self.config.get("recent_count", 0)
        if self.recent_count is None:
            self.recent_count = 0
        try:
            self.recent_count = int(self.recent_count)
        except (ValueError, TypeError):
            self.recent_count = 0
        self.stdio.verbose("recent_count: {0}".format(self.recent_count))
        self.gather_tuple = {"node": "", "success": "Fail", "info": "", "file_size": 0, "file_path": ""}

    def get_result(self):
        return self.gather_tuple

    def handle(self):
        self.ssh_client = SshClient(self.context, self.node)
        self.gather_tuple["node"] = self.ssh_client.get_name()
        self.tmp_dir = os.path.join(self.tmp_dir, "obdiag_gather_{0}".format(str(uuid.uuid4())[:6]))
        self.ssh_client.exec_cmd("mkdir -p {0}".format(self.tmp_dir))
        from_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.from_time_str))
        to_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.to_time_str))

        tmp_dir = "{0}_log_{1}_{2}_{3}_{4}".format(self.target, self.ssh_client.get_name().replace(":", "_"), from_datetime_timestamp, to_datetime_timestamp, str(uuid.uuid4())[:6])
        if self.target == "observer" and self.ssh_client.get_name() == "local":
            pid = self.__get_observer_pid(self.node)
            if pid:
                tmp_dir = "{0}_pid_{1}".format(tmp_dir, pid)
        tmp_log_dir = os.path.join(self.tmp_dir, tmp_dir)
        # mkdir tmp_log_dir
        mkdir_response = self.ssh_client.exec_cmd("mkdir -p {0}".format(tmp_log_dir))
        if mkdir_response:
            self.stdio.error("gather_log_on_node {0} mkdir -p {0}: {1}, error:{2}".format(self.ssh_client.get_ip(), tmp_log_dir, mkdir_response))
            return
        self.stdio.verbose("gather_log_on_node {0} tmp_log_dir: {1}, info:{2}".format(self.ssh_client.get_ip(), tmp_log_dir, mkdir_response))
        try:
            # find logs
            logs_name = self.__find_logs_name()
            if logs_name is None or len(logs_name) == 0:
                self.stdio.error("gather_log_on_node {0} failed: no log found".format(self.ssh_client.get_ip()))
                self.gather_tuple["info"] = "no log found"
                return
            elif len(logs_name) > self.file_number_limit:
                self.stdio.error('{0} The number of log files is {1}, out of range (0,{2}], ' "Please adjust the query limit".format(self.ssh_client.get_name(), len(logs_name), self.file_number_limit))
                self.gather_tuple["info"] = "too many files {0} > {1}".format(len(logs_name), self.file_number_limit)
                return

            # gather log to remote tmp_dir ,if grep is exit, with grep
            self.__grep_log_to_tmp(logs_name, tmp_log_dir)

            # build tar file
            if is_empty_dir(self.ssh_client, tmp_log_dir, self.stdio):
                # if remote tmp_log_dir is empty, rm the dir and return
                self.ssh_client.exec_cmd("rm -rf {0}".format(tmp_log_dir))
                self.stdio.error("gather_log_on_node {0} failed: tmp_log_dir({1}) no log found".format(self.ssh_client.get_name(), tmp_log_dir))
                self.gather_tuple["info"] = "tmp_log_dir({0}) no log found".format(tmp_log_dir)
                return

            tar_file = "{0}.tar.gz".format(tmp_log_dir)
            tar_cmd = "cd {0} && tar -czf {1}.tar.gz {1}/*".format(self.tmp_dir, tmp_dir)
            self.stdio.verbose("gather_log_on_node {0} tar_cmd: {1}".format(self.ssh_client.get_ip(), tar_cmd))
            self.stdio.verbose("gather_log_on_node {0} tar request:{1}".format(self.ssh_client.get_ip(), self.ssh_client.exec_cmd(tar_cmd)))

            # download log to local store_dir
            tar_file_size = int(get_file_size(self.ssh_client, tar_file))
            self.stdio.verbose("gather_log_on_node {0} tar_file_size: {1}".format(self.ssh_client.get_ip(), tar_file_size))
            if tar_file_size == 0:
                self.stdio.error("gather_log_on_node {0} failed: tar file size is 0".format(self.ssh_client.get_ip()))
                self.gather_tuple["info"] = "tar file size is 0"
                return
            if tar_file_size > self.file_size_limit:
                self.stdio.error("gather_log_on_node {0} failed: File too large over gather.file_size_limit".format(self.ssh_client.get_ip()))
                self.gather_tuple["info"] = "File too large over gather.file_size_limit"
                return
            else:
                self.stdio.verbose("gather_log_on_node {0} download log to local store_dir: {1}".format(self.ssh_client.get_ip(), self.store_dir))
                self.ssh_client.exec_cmd("chomd -R a+rx {0}".format(self.tmp_dir))
                self.ssh_client.download(tar_file, os.path.join(self.store_dir, os.path.basename("{0}".format(tar_file))))
                self.stdio.verbose("download success. gather_log_on_node {0} delete tar file: {1}".format(self.ssh_client.get_ip(), tar_file))
                self.ssh_client.exec_cmd("rm -rf {0}".format(tar_file))
            tar_file_name = os.path.basename("{0}".format(tar_file))
            self.stdio.verbose("tar_file_name: {0}".format(tar_file_name))
            local_tar_file_path = os.path.join(self.store_dir, tar_file_name)
            self.stdio.verbose("local_tar_file_path: {0}".format(local_tar_file_path))
            self.gather_tuple["file_size"] = FileUtil.size_format(num=int(os.path.getsize(local_tar_file_path) or 0), output_str=True)
            self.gather_tuple["info"] = "file save in {0}".format(local_tar_file_path)
            self.gather_tuple["success"] = "Success"
            self.gather_tuple["file_path"] = local_tar_file_path
        except Exception as e:
            self.stdio.verbose(traceback.format_exc())
            self.stdio.error("gather_log_on_node {0} failed: {1}".format(self.ssh_client.get_ip(), str(e)))
            self.gather_tuple["info"] = str(e)
        finally:
            self.stdio.verbose("clear tmp_log_dir: {0}".format(self.tmp_dir))
            self.ssh_client.exec_cmd("rm -rf {0}".format(self.tmp_dir))
            self.stdio.verbose("gather_log_on_node {0} finished".format(self.ssh_client.get_ip()))
            self.stdio.verbose("gather_log_on_node {0} gather_tuple: {1}".format(self.ssh_client.get_ip(), self.gather_tuple))
            self.stdio.verbose("gather_log_on_node {0} done".format(self.ssh_client.get_ip()))

    def __grep_log_to_tmp(self, logs_name, tmp_log_dir):
        grep_cmd = ""
        if self.grep_option:
            self.stdio.verbose("grep files, grep_option = {0}".format(self.grep_option))
            for grep_option in self.grep_option:
                if grep_cmd == "":
                    grep_cmd = "grep -e '{0}' ".format(grep_option)
                    continue
                grep_cmd += "| grep -e '{0}'".format(grep_option)
        for log_name in logs_name:
            source_log_name = "{0}/{1}".format(self.log_path, log_name)
            target_log_name = "{0}/{1}".format(tmp_log_dir, log_name)
            self.stdio.verbose("grep files, source_log_name = [{0}], target_log_name = [{1}]".format(source_log_name, target_log_name))
            # for oms log
            if log_name.endswith(".gz"):
                log_grep_cmd = "cp -a {0} {1}".format(source_log_name, target_log_name)
                self.stdio.verbose("grep files, run cmd = [{0}]".format(log_grep_cmd))
                self.ssh_client.exec_cmd(log_grep_cmd)
                continue
            log_grep_cmd = ""
            if grep_cmd == "":
                log_grep_cmd = "cp -a {0} {1}".format(source_log_name, target_log_name)
            else:
                log_grep_cmd = grep_cmd + " {0}".format(source_log_name)
                log_grep_cmd += " > {0} ".format(target_log_name)
            self.stdio.verbose("grep files, run cmd = [{0}]".format(log_grep_cmd))
            self.ssh_client.exec_cmd(log_grep_cmd)

    def __find_logs_name(self):
        try:
            logs_scope = ""
            for scope in self.scope:
                target_scopes = self.scope[scope]["key"]
                if isinstance(target_scopes, list):
                    for target_scope in target_scopes:
                        if logs_scope == "":
                            logs_scope = ' -name "{0}" '.format(target_scope)
                            continue
                        logs_scope = logs_scope + ' -o -name "{0}" '.format(target_scope)
                else:
                    if logs_scope == "":
                        logs_scope = ' -name "{0}" '.format(target_scopes)
                        continue
                    logs_scope = logs_scope + ' -o -name "{0}" '.format(target_scopes)
            if logs_scope == "":
                self.stdio.warn("gather_log_on_node {0} find logs scope is null".format(self.ssh_client.get_ip(), logs_scope))
                return []
            self.stdio.verbose("gather_log_on_node {0} find logs scope: {1}".format(self.ssh_client.get_ip(), logs_scope))
            find_cmd = "find {0}/ {1} | awk -F '/' ".format(self.log_path, logs_scope) + "'{print $NF}' | sort"
            self.stdio.verbose("gather_log_on_node {0} find logs cmd: {1}".format(self.ssh_client.get_ip(), find_cmd))
            logs_name = self.ssh_client.exec_cmd(find_cmd)
            if logs_name is not None and len(logs_name) != 0:
                # If recent_count > 0, skip time filtering and get all log files, then filter by recent_count
                if self.recent_count > 0:
                    self.stdio.verbose("recent_count is {0}, skipping time filtering (from/to/since will be ignored)".format(self.recent_count))
                    log_name_list = self.__get_all_logfile_name_list(logs_name)
                    # Apply recent_count filter
                    if len(log_name_list) > self.recent_count:
                        log_name_list = self.__filter_by_recent_count(log_name_list)
                    return log_name_list
                else:
                    # Normal time filtering when recent_count is 0
                    # Use search_version to determine which method to use
                    if self.search_version == 1:
                        self.stdio.verbose("gather_log_on_node {0} using __get_logfile_name_list (search_version=1)".format(self.ssh_client.get_ip()))
                        log_name_list = self.__get_logfile_name_list(self.from_time_str, self.to_time_str, self.log_path, logs_name)
                    else:
                        self.stdio.verbose("gather_log_on_node {0} using __get_logfile_name_list_v2 (search_version={1})".format(self.ssh_client.get_ip(), self.search_version))
                        log_name_list = self.__get_logfile_name_list_v2(self.from_time_str, self.to_time_str, self.log_path, logs_name)
                    return log_name_list
            else:
                self.stdio.warn("gather_log_on_node {0} failed: no log found".format(self.ssh_client.get_ip()))
                return []
        except Exception as e:
            raise Exception("gather_log_on_node {0} find logs failed: {1}".format(self.ssh_client.get_ip(), str(e)))

    def __get_all_logfile_name_list(self, log_files):
        """
        Get all log file names without time filtering.
        Used when recent_count > 0 to get all files first, then filter by recent_count.
        """
        log_name_list = []

        # Handle OMS special case
        if self.target == "oms":
            formatted_time = datetime.datetime.now().strftime("%Y-%m-%d_%H")
            for file_name in log_files.split('\n'):
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue
                if "log.gz" not in file_name or formatted_time in file_name:
                    log_name_list.append(file_name)
            return log_name_list

        # Handle OMS_CDC special case
        if self.target == "oms_cdc":
            self.stdio.warn("oms_cdc not support get log file name list, return all 'libobcdc.log*' log file name list")
            for file_name in log_files.split('\n'):
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue
                if "libobcdc.log" in file_name:
                    log_name_list.append(file_name)
            return log_name_list

        # For observer and obproxy, return all log files
        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            if file_name == "":
                self.stdio.verbose("existing file name is empty")
                continue
            log_name_list.append(file_name)

        self.stdio.verbose("get all log file name list (no time filtering), found {0} files".format(len(log_name_list)))
        return log_name_list

    def __get_logfile_name_list(self, from_time_str, to_time_str, log_dir, log_files):
        # oms get all log file name list, the log size is so small
        if self.target == "oms":
            log_name_list = []
            formatted_time = datetime.datetime.now().strftime("%Y-%m-%d_%H")
            for file_name in log_files.split('\n'):
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue
                if "log.gz" not in file_name or formatted_time in file_name:
                    log_name_list.append(file_name)
                    continue
            return log_name_list
        elif self.target == "oms_cdc":
            self.stdio.warn("oms_cdc not support get log file name list, retrun all 'libobcdc.log*' log file name list")
            log_name_list = []
            for file_name in log_files.split('\n'):
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue
                if "libobcdc.log" in file_name:
                    log_name_list.append(file_name)
                    continue
            return log_name_list

        self.stdio.verbose("get log file name list, from time {0}, to time {1}, log dir {2}, log files {3}".format(from_time_str, to_time_str, log_dir, log_files))
        log_name_list = []
        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            try:
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue
                from_time_str_strp = datetime.datetime.strptime(from_time_str, "%Y-%m-%d %H:%M:%S")
                to_time_str_strp = datetime.datetime.strptime(to_time_str, "%Y-%m-%d %H:%M:%S")

                if not file_name.endswith("log") and not file_name.endswith("wf"):
                    # get end_time from file_name
                    first_line_text = self.ssh_client.exec_cmd('grep -aE "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}" ' + "{0} ".format(os.path.join(log_dir, file_name)) + " | head -n 1").strip()
                    if first_line_text == "":
                        self.stdio.verbose("node: {0}, The log file {1} can't find starttime. skip".format(self.ssh_client.get_name(), file_name))
                        continue
                    self.stdio.verbose("node: {0}, The log file {1} first line text: {2}".format(self.ssh_client.get_name(), file_name, first_line_text))
                    file_start_time_str = TimeUtils.extract_time_from_log_file_text(first_line_text, self.stdio)
                    file_end_time_str = TimeUtils.filename_time_to_datetime(TimeUtils.extract_filename_time_from_log_name(file_name, self.stdio), self.stdio)
                    self.stdio.verbose("node: {0}, file_name: {1}, file_start_time_str: {2}, file_end_time_str: {3}".format(self.ssh_client.get_name(), file_name, file_start_time_str, file_end_time_str))
                    if file_end_time_str == "":
                        self.stdio.warn("node: {0}, The log file {1} can't find endtime. skip".format(self.ssh_client.get_name(), file_name))
                        continue
                    # print file_end_time_str
                    self.stdio.verbose("node: {0}, The log file {1} end time: {2}".format(self.ssh_client.get_name(), file_name, file_end_time_str))
                    file_end_time_str_strp = datetime.datetime.strptime(file_end_time_str, "%Y-%m-%d %H:%M:%S")
                    if file_start_time_str == "":
                        if file_end_time_str_strp < from_time_str_strp:
                            self.stdio.warn("node: {0}, The log file {1} can't find start_time. and end_time<from_time. skip".format(self.ssh_client.get_name(), file_name))
                            continue
                        else:
                            self.stdio.warn("node: {0}, The log file {1} can't find start_time. but end_time>from_time, so add it ".format(self.ssh_client.get_name(), file_name))
                            log_name_list.append(file_name)
                            continue
                    file_start_time_str_strp = datetime.datetime.strptime(file_start_time_str, "%Y-%m-%d %H:%M:%S")

                    # When two time intervals overlap, need to add the file
                    if ((file_start_time_str_strp <= from_time_str_strp) and (file_end_time_str_strp >= from_time_str_strp)) or ((file_start_time_str_strp >= from_time_str_strp) and (file_start_time_str_strp <= to_time_str_strp)):
                        log_name_list.append(file_name)
                        self.stdio.verbose("node: {0}, The log file {1} start {2}, end {3} is range {4} to {5}".format(self.ssh_client.get_name(), file_name, file_start_time_str, file_end_time_str, from_time_str, to_time_str))
                elif file_name.endswith("log") or file_name.endswith("wf"):
                    # get form_time and to_time from the first and last lines of text of the file
                    # Get the first and last lines of text of the file. Here, use a command
                    first_line_text = self.ssh_client.exec_cmd('grep -aE "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}" ' + "{0} ".format(os.path.join(log_dir, file_name)) + " | head -n 1").strip()
                    if first_line_text == "":
                        self.stdio.verbose("node: {0}, The log file {1} can't find starttime. skip".format(self.ssh_client.get_name(), file_name))
                        continue
                    self.stdio.verbose("node: {0}, The log file {1} first line text: {2}".format(self.ssh_client.get_name(), file_name, first_line_text))
                    file_start_time_str = TimeUtils.extract_time_from_log_file_text(first_line_text, self.stdio)
                    file_start_time_str_strp = datetime.datetime.strptime(file_start_time_str, "%Y-%m-%d %H:%M:%S")

                    last_line_text = self.ssh_client.exec_cmd('grep -aE "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}" ' + "{0} ".format(os.path.join(log_dir, file_name)) + " | tail -n 1").strip()
                    if last_line_text == "":
                        file_end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.stdio.warn("node: {0}, The log file {1} can't find endtime. set nowtime:{2}".format(self.ssh_client.get_name(), file_name, file_end_time))
                        file_end_time_str_strp = datetime.datetime.strptime(file_end_time, "%Y-%m-%d %H:%M:%S")
                    else:
                        self.stdio.verbose("node: {0}, The log file {1} last line text: {2}".format(self.ssh_client.get_name(), file_name, last_line_text))
                        file_end_time = TimeUtils.extract_time_from_log_file_text(last_line_text, self.stdio)
                        file_end_time_str_strp = datetime.datetime.strptime(file_end_time, "%Y-%m-%d %H:%M:%S")
                    self.stdio.verbose("node: {0}, The log file {1} start time: {2}, end time: {3}".format(self.ssh_client.get_name(), file_name, file_start_time_str, file_end_time))

                    if file_start_time_str_strp == "":
                        if file_end_time_str_strp < from_time_str_strp:
                            self.stdio.warn("node: {0}, The log file {1} can't find start_time. and end_time<from_time. skip".format(self.ssh_client.get_name(), file_name))
                            continue
                        else:
                            self.stdio.warn("node: {0}, The log file {1} can't find start_time. but end_time>from_time, so add it ".format(self.ssh_client.get_name(), file_name))
                            log_name_list.append(file_name)

                    if ((file_start_time_str_strp <= from_time_str_strp) and (file_end_time_str_strp >= from_time_str_strp)) or ((file_start_time_str_strp >= from_time_str_strp) and (file_start_time_str_strp <= to_time_str_strp)):
                        log_name_list.append(file_name)
                        self.stdio.verbose("node: {0}, The log file {1} start {2}, end {3} is range {4} to {5}".format(self.ssh_client.get_name(), file_name, file_start_time_str, file_end_time, from_time_str, to_time_str))
            except Exception as e:
                self.stdio.error("gather_log_on_node {0} get log file: {2} name failed, Skip it: {1}".format(self.ssh_client.get_ip(), str(e), file_name))
                self.stdio.verbose(traceback.format_exc())
                continue
        if len(log_name_list) > 0:
            self.stdio.verbose("Find the qualified log file {0} on Server [{1}], " "wait for the next step".format(log_name_list, self.ssh_client.get_ip()))
        else:
            self.stdio.warn("No found the qualified log file on Server [{0}]".format(self.ssh_client.get_name()))
        return log_name_list

    def __get_log_type(self, file_name):
        """
        Extract log type from file name by removing timestamp suffix.
        Examples:
            observer.log -> observer.log
            observer.log.20250101120000000 -> observer.log
            observer.log.wf -> observer.log.wf
            observer.log.wf.20250101120000000 -> observer.log.wf
            election.log -> election.log
            election.log.20250101120000000 -> election.log
        """
        # Remove timestamp suffix (17 digits) if present
        log_type = re.sub(r'\.\d{17}$', '', file_name)
        return log_type

    def __filter_by_recent_count(self, log_name_list):
        """
        Filter log files to keep only the most recent N files for EACH log type.
        Files without timestamp (current log files) are treated as newest and included first.

        For example, with recent_count=2:
            - observer.log, observer.log.20250101, observer.log.20250102 -> keep observer.log + observer.log.20250102
            - election.log, election.log.20250101 -> keep both
        """
        if self.recent_count <= 0:
            return log_name_list

        self.stdio.verbose("recent_count is {0}, filtering to keep only the most recent {0} files per log type from {1} files".format(self.recent_count, len(log_name_list)))

        # Group files by log type
        log_type_groups = {}  # log_type -> list of (file_name, timestamp_datetime or None)

        for file_name in log_name_list:
            log_type = self.__get_log_type(file_name)
            if log_type not in log_type_groups:
                log_type_groups[log_type] = []

            timestamp_match = re.search(r'\.(\d{17})$', file_name)
            if timestamp_match:
                # File has timestamp
                timestamp_str = timestamp_match.group(1)
                try:
                    year = int(timestamp_str[0:4])
                    month = int(timestamp_str[4:6])
                    day = int(timestamp_str[6:8])
                    hour = int(timestamp_str[8:10])
                    minute = int(timestamp_str[10:12])
                    second = int(timestamp_str[12:14])
                    microsecond = int(timestamp_str[14:17]) * 1000
                    file_time_dt = datetime.datetime(year, month, day, hour, minute, second, microsecond)
                    log_type_groups[log_type].append((file_name, file_time_dt))
                except (ValueError, IndexError):
                    # Invalid timestamp, treat as current log file (newest)
                    log_type_groups[log_type].append((file_name, None))
            else:
                # File has no timestamp, treat as current log file (newest)
                log_type_groups[log_type].append((file_name, None))

        # Filter each log type group to keep only the most recent N files
        filtered_list = []
        for log_type, files in log_type_groups.items():
            # Separate current log files (no timestamp) and timestamped files
            current_files = [f for f, ts in files if ts is None]
            timestamped_files = [(f, ts) for f, ts in files if ts is not None]

            # Sort timestamped files by time (newest first)
            timestamped_files.sort(key=lambda x: x[1], reverse=True)

            # Build list for this log type: current files first, then most recent timestamped files
            type_filtered = []
            type_filtered.extend(current_files)
            remaining_slots = self.recent_count - len(current_files)
            if remaining_slots > 0:
                type_filtered.extend([f for f, _ in timestamped_files[:remaining_slots]])
            else:
                # If current files already exceed recent_count, only keep them
                type_filtered = current_files[: self.recent_count]

            self.stdio.verbose("Log type '{0}': kept {1} files from {2}".format(log_type, len(type_filtered), len(files)))
            filtered_list.extend(type_filtered)

        self.stdio.verbose("After filtering by recent_count={0}, kept {1} files total: {2}".format(self.recent_count, len(filtered_list), filtered_list))
        return filtered_list

    def __get_logfile_name_list_v2(self, from_time_str, to_time_str, log_dir, log_files):
        """
        Get log file name list by parsing timestamp from filename only (not from file content).
        Filename format examples:
        - observer.log (no timestamp, current log file, should be included)
        - observer.log.20251201131651918 (timestamp: 2025-12-01 13:16:51.918)
        - observer.log.20251015031702026.zst (timestamp: 2025-10-15 03:17:02.026, compressed)
        - observer.log.wf (no timestamp, current wf log file, should be included)
        Timestamp format: YYYYMMDDHHMMSSmmm (17 digits)
        """
        # oms get all log file name list, the log size is so small
        if self.target == "oms":
            log_name_list = []
            formatted_time = datetime.datetime.now().strftime("%Y-%m-%d_%H")
            for file_name in log_files.split('\n'):
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue
                if "log.gz" not in file_name or formatted_time in file_name:
                    log_name_list.append(file_name)
                    continue
            return log_name_list
        elif self.target == "oms_cdc":
            self.stdio.warn("oms_cdc not support get log file name list, retrun all 'libobcdc.log*' log file name list")
            log_name_list = []
            for file_name in log_files.split('\n'):
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue
                if "libobcdc.log" in file_name:
                    log_name_list.append(file_name)
                    continue
            return log_name_list

        self.stdio.verbose("get log file name list v2, from time {0}, to time {1}, log dir {2}, log files {3}".format(from_time_str, to_time_str, log_dir, log_files))
        log_name_list = []

        try:
            from_time_dt = datetime.datetime.strptime(from_time_str, "%Y-%m-%d %H:%M:%S")
            to_time_dt = datetime.datetime.strptime(to_time_str, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self.stdio.error("gather_log_on_node {0} parse time failed: {1}".format(self.ssh_client.get_ip(), str(e)))
            return log_name_list

        for file_name in log_files.split('\n'):
            file_name = file_name.strip()
            try:
                if file_name == "":
                    self.stdio.verbose("existing file name is empty")
                    continue

                # Extract timestamp from filename
                # Pattern: filename.20251201131651918 or filename.20251015031702026.zst
                # Timestamp is 17 digits: YYYYMMDDHHMMSSmmm
                # Match pattern: .(17 digits) followed by either . (for compressed files) or end of string
                timestamp_match = re.search(r'\.(\d{17})(?:\.|$)', file_name)

                if timestamp_match:
                    # File has timestamp in filename
                    timestamp_str = timestamp_match.group(1)
                    try:
                        # Parse timestamp: YYYYMMDDHHMMSSmmm
                        year = int(timestamp_str[0:4])
                        month = int(timestamp_str[4:6])
                        day = int(timestamp_str[6:8])
                        hour = int(timestamp_str[8:10])
                        minute = int(timestamp_str[10:12])
                        second = int(timestamp_str[12:14])
                        microsecond = int(timestamp_str[14:17]) * 1000  # Convert milliseconds to microseconds

                        file_time_dt = datetime.datetime(year, month, day, hour, minute, second, microsecond)

                        self.stdio.verbose("node: {0}, file_name: {1}, extracted timestamp: {2}, file_time: {3}".format(self.ssh_client.get_name(), file_name, timestamp_str, file_time_dt.strftime("%Y-%m-%d %H:%M:%S.%f")))

                        # Check if file time is within or overlaps with the time range
                        # For rotated log files, the timestamp represents when the file was created (end of previous log)
                        # The file contains logs from its creation time until the next file is created
                        # We include the file if its timestamp is within the query range [from_time, to_time]
                        # This is because the file was created during the query period, so it may contain relevant logs
                        if from_time_dt <= file_time_dt <= to_time_dt:
                            log_name_list.append(file_name)
                            self.stdio.verbose("node: {0}, file {1} timestamp {2} is in range [{3}, {4}], include it".format(self.ssh_client.get_name(), file_name, file_time_dt.strftime("%Y-%m-%d %H:%M:%S"), from_time_str, to_time_str))
                        else:
                            self.stdio.verbose("node: {0}, file {1} timestamp {2} is out of range [{3}, {4}], exclude it".format(self.ssh_client.get_name(), file_name, file_time_dt.strftime("%Y-%m-%d %H:%M:%S"), from_time_str, to_time_str))
                    except (ValueError, IndexError) as e:
                        self.stdio.warn("node: {0}, file {1} has invalid timestamp format, skip: {2}".format(self.ssh_client.get_name(), file_name, str(e)))
                        continue
                else:
                    # File has no timestamp (e.g., observer.log, observer.log.wf, rootservice.log, trace.log)
                    # These are current log files being written, should be included
                    if file_name.endswith(".log") or file_name.endswith(".wf"):
                        log_name_list.append(file_name)
                        self.stdio.verbose("node: {0}, file {1} has no timestamp, is current log file, include it".format(self.ssh_client.get_name(), file_name))
                    else:
                        self.stdio.verbose("node: {0}, file {1} has no timestamp and is not a .log or .wf file, skip".format(self.ssh_client.get_name(), file_name))

            except Exception as e:
                self.stdio.error("gather_log_on_node {0} get log file: {2} name failed, Skip it: {1}".format(self.ssh_client.get_ip(), str(e), file_name))
                self.stdio.verbose(traceback.format_exc())
                continue

        # Note: recent_count filtering is now handled in __find_logs_name for both search_version methods

        if len(log_name_list) > 0:
            self.stdio.verbose("Find the qualified log file {0} on Server [{1}], wait for the next step".format(log_name_list, self.ssh_client.get_ip()))
        else:
            self.stdio.warn("No found the qualified log file on Server [{0}]".format(self.ssh_client.get_name()))
        return log_name_list

    def __get_observer_pid(self, node):
        home_path = node.get("home_path")
        pid_file_path = os.path.join(home_path, 'run', 'observer.pid')
        try:
            with open(pid_file_path, 'r') as file:
                first_line = file.readline().strip()
                return first_line
        except FileNotFoundError:
            self.stdio.warn(f"The file {pid_file_path} does not exist. Attempting to find the process using ps.")
        except Exception as e:
            self.stdio.exception(f"An error occurred: {e}")

        try:
            result = subprocess.run(['ps', '-ef'], stdout=subprocess.PIPE)
            processes = result.stdout.decode().splitlines()
            observer_processes = [p for p in processes if f"{home_path}/bin/observer" in p and 'grep' not in p]

            if observer_processes:
                pid = observer_processes[0].split()[1]
                return pid
            else:
                self.stdio.warn("No observer process found at the specified path.")
                return None
        except Exception as e:
            self.stdio.exception(f"An error occurred while trying to find the observer process: {e}")
            return None
