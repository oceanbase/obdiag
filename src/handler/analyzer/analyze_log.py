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
@time: 2023/9/23
@file: analyze_log.py
@desc:
"""
import datetime
import glob
import os
import re
import tarfile

import tabulate

from src.common.ssh_client.local_client import LocalClient
from src.handler.base_shell_handler import BaseShellHandler
from src.common.obdiag_exception import OBDIAGFormatException
from src.common.constant import const
from src.common.command import download_file
from src.common.ob_log_level import OBLogLevel
from src.handler.meta.ob_error import OB_RET_DICT
from src.common.tool import Util
from src.common.tool import DirectoryUtil
from src.common.tool import FileUtil
from src.common.tool import TimeUtils
from src.common.result_type import ObdiagResult
from src.handler.gather.gather_component_log import GatherComponentLogHandler


class AnalyzeLogHandler(BaseShellHandler):
    def __init__(self, context):
        super(AnalyzeLogHandler, self).__init__()
        self.context = context
        self.stdio = context.stdio
        self.directly_analyze_files = False
        self.analyze_files_list = []
        self.is_ssh = True
        self.gather_timestamp = None
        self.gather_ob_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.gather_pack_dir = None
        self.ob_log_dir = None
        self.from_time_str = None
        self.to_time_str = None
        self.grep_args = None
        self.scope = None
        self.zip_encrypt = False
        self.log_level = OBLogLevel.WARN
        self.config_path = const.DEFAULT_CONFIG_PATH

    def init_config(self):
        self.nodes = self.context.cluster_config['servers']
        self.inner_config = self.context.inner_config
        if self.inner_config is None:
            self.file_number_limit = 20
            self.file_size_limit = 2 * 1024 * 1024 * 1024
        else:
            basic_config = self.inner_config['obdiag']['basic']
            self.file_number_limit = int(basic_config["file_number_limit"])
            self.file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
            self.config_path = basic_config['config_path']
        return True

    def init_option(self):
        options = self.context.options
        from_option = Util.get_option(options, 'from')
        to_option = Util.get_option(options, 'to')
        since_option = Util.get_option(options, 'since')
        store_dir_option = Util.get_option(options, 'store_dir')
        grep_option = Util.get_option(options, 'grep')
        scope_option = Util.get_option(options, 'scope')
        log_level_option = Util.get_option(options, 'log_level')
        files_option = Util.get_option(options, 'files')
        temp_dir_option = Util.get_option(options, 'temp_dir')
        if files_option:
            self.is_ssh = False
            self.directly_analyze_files = True
            self.analyze_files_list = files_option
        if from_option is not None and to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(from_option)
                to_timestamp = TimeUtils.parse_time_str(to_option)
                self.from_time_str = from_option
                self.to_time_str = to_option
            except OBDIAGFormatException:
                self.stdio.exception('Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. from_datetime={0}, to_datetime={1}'.format(from_option, to_option))
                return False
            if to_timestamp <= from_timestamp:
                self.stdio.exception('Error: from datetime is larger than to datetime, please check.')
                return False
        elif (from_option is None or to_option is None) and since_option is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            if not self.directly_analyze_files:
                self.stdio.print('analyze log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.print('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option is not None:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            if not self.directly_analyze_files:
                self.stdio.print('analyze log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option is not None:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        if grep_option is not None:
            self.grep_args = grep_option
        if scope_option:
            self.scope = scope_option
        if log_level_option:
            self.log_level = OBLogLevel().get_log_level(log_level_option)
        if temp_dir_option:
            self.gather_ob_log_temporary_dir = temp_dir_option
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init option failed")
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init config failed")
        self.stdio.print("analyze nodes's log start. Please wait a moment...")
        self.stdio.print('analyze start')
        local_store_parent_dir = os.path.join(self.gather_pack_dir, "obdiag_analyze_pack_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp())))
        self.stdio.verbose("Use {0} as pack dir.".format(local_store_parent_dir))
        analyze_tuples = []

        # When --files is not specified: use GatherComponentLogHandler to collect logs (compressed) first, then analyze locally
        if not self.directly_analyze_files:
            return self.__handle_with_gather(local_store_parent_dir)

        # --files specified: analyze local files only (no SSH)
        DirectoryUtil.mkdir(path=local_store_parent_dir, stdio=self.stdio)
        self.stdio.print("analyze nodes's log start. Please wait a moment...")
        self.stdio.start_loading('analyze start')
        resp, node_results = self.__handle_offline(local_store_parent_dir)
        analyze_tuples = [("127.0.0.1", False, resp["error"], node_results)]
        title, field_names, summary_list, summary_details_list = self.__get_overall_summary(analyze_tuples, True)
        analyze_info_nodes = []
        for summary in summary_list:
            analyze_info_node = {}
            field_names_nu = 0
            for m in field_names:
                analyze_info_node[m] = summary[field_names_nu]
                field_names_nu += 1
                if field_names_nu == len(summary):
                    break
            analyze_info_nodes.append(analyze_info_node)
        table = tabulate.tabulate(summary_list, headers=field_names, tablefmt="grid", showindex=False)
        self.stdio.stop_loading('analyze result success')
        self.stdio.print(title)
        self.stdio.print(table)
        with open(os.path.join(local_store_parent_dir, "result_details.txt"), 'a', encoding='utf-8') as fileobj:
            fileobj.write(u'{}'.format(title + str(table) + "\n\nDetails:\n\n"))
        # build summary details
        summary_details_list_data = []
        for m in range(len(summary_details_list)):
            summary_details_list_data_once = {}
            for n in range(len(field_names)):
                extend = "\n\n" if n == len(field_names) - 1 else "\n"
                with open(os.path.join(local_store_parent_dir, "result_details.txt"), 'a', encoding='utf-8') as fileobj:
                    fileobj.write(u'{}'.format(field_names[n] + ": " + str(summary_details_list[m][n]) + extend))
                summary_details_list_data_once[field_names[n]] = str(summary_details_list[m][n])
            summary_details_list_data.append(summary_details_list_data_once)
        last_info = "For more details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(local_store_parent_dir, "result_details.txt"))
        self.stdio.print(last_info)
        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"result": analyze_info_nodes, "summary_details_list": summary_details_list_data, "store_dir": local_store_parent_dir})

    def __handle_with_gather(self, local_store_parent_dir):
        """
        Use GatherComponentLogHandler to collect logs (compressed tar.gz) first, then extract and analyze locally.
        Reduces network transfer time compared to pulling raw logs.
        """
        DirectoryUtil.mkdir(path=local_store_parent_dir, stdio=self.stdio)
        gather_store_dir = os.path.join(local_store_parent_dir, "gathered_logs")
        DirectoryUtil.mkdir(path=gather_store_dir, stdio=self.stdio)

        self.stdio.print("gather log (compressed) start, then analyze locally...")
        self.stdio.start_loading("gather log start")
        handler = GatherComponentLogHandler()
        handler.init(
            self.context,
            target="observer",
            from_option=self.from_time_str,
            to_option=self.to_time_str,
            since=Util.get_option(self.context.options, 'since'),
            scope=self.scope,
            grep=self.grep_args,
            store_dir=gather_store_dir,
            temp_dir=self.gather_ob_log_temporary_dir,
            is_scene=True,
        )
        gather_result = handler.handle()
        self.stdio.stop_loading("gather succeed" if gather_result.is_success() else "gather failed")

        if not gather_result.is_success():
            self.stdio.error("gather log failed: {0}".format(gather_result.error_data))
            return gather_result

        tar_files = glob.glob(os.path.join(gather_store_dir, "*.tar.gz"))
        if not tar_files:
            self.stdio.warn("No tar.gz files found in gather result dir: {0}".format(gather_store_dir))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="No log tar files gathered, please check gather config or time range")

        self.stdio.verbose("extract {0} tar file(s) to local".format(len(tar_files)))
        for tar_path in tar_files:
            try:
                with tarfile.open(tar_path, 'r:gz') as tar:
                    tar.extractall(path=local_store_parent_dir)
            except Exception as e:
                self.stdio.exception("extract tar failed: {0}, error: {1}".format(tar_path, e))
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="extract gather tar failed: {0}".format(str(e)))

        analyze_tuples = []
        self.stdio.start_loading("analyze log start")
        for name in os.listdir(local_store_parent_dir):
            node_dir = os.path.join(local_store_parent_dir, name)
            if name == "gathered_logs" or not os.path.isdir(node_dir):
                continue
            node_name = self.__parse_node_name_from_gather_dir(name)
            log_files = [f for f in os.listdir(node_dir) if os.path.isfile(os.path.join(node_dir, f))]
            node_results = []
            for log_f in sorted(log_files):
                full_path = os.path.join(node_dir, log_f)
                try:
                    file_result = self.__parse_log_lines(full_path)
                    node_results.append(file_result)
                except Exception as e:
                    self.stdio.verbose("parse log file {0} failed: {1}".format(full_path, e))
            analyze_tuples.append((node_name, False, "", node_results))

        self.stdio.stop_loading("succeed")
        title, field_names, summary_list, summary_details_list = self.__get_overall_summary(analyze_tuples, False)
        analyze_info_nodes = []
        for summary in summary_list:
            analyze_info_node = {}
            field_names_nu = 0
            for m in field_names:
                analyze_info_node[m] = summary[field_names_nu]
                field_names_nu += 1
                if field_names_nu == len(summary):
                    break
            analyze_info_nodes.append(analyze_info_node)
        table = tabulate.tabulate(summary_list, headers=field_names, tablefmt="grid", showindex=False)
        self.stdio.print(title)
        self.stdio.print(table)
        with open(os.path.join(local_store_parent_dir, "result_details.txt"), 'a', encoding='utf-8') as fileobj:
            fileobj.write(u'{}'.format(title + str(table) + "\n\nDetails:\n\n"))
        summary_details_list_data = []
        for m in range(len(summary_details_list)):
            summary_details_list_data_once = {}
            for n in range(len(field_names)):
                extend = "\n\n" if n == len(field_names) - 1 else "\n"
                with open(os.path.join(local_store_parent_dir, "result_details.txt"), 'a', encoding='utf-8') as fileobj:
                    fileobj.write(u'{}'.format(field_names[n] + ": " + str(summary_details_list[m][n]) + extend))
                summary_details_list_data_once[field_names[n]] = str(summary_details_list[m][n])
            summary_details_list_data.append(summary_details_list_data_once)
        last_info = "For more details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(local_store_parent_dir, "result_details.txt"))
        self.stdio.print(last_info)
        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"result": analyze_info_nodes, "summary_details_list": summary_details_list_data, "store_dir": local_store_parent_dir})

    def __parse_node_name_from_gather_dir(self, dir_name):
        """
        Parse node display name from gather tar inner dir name.
        Format: observer_log_10.0.0.1_2881_20250101120000_20250102120000_abc123 -> 10.0.0.1_2881
        """
        match = re.match(r'^observer_log_(.+)_\d+_\d+_[a-z0-9]{6}$', dir_name)
        if match:
            return match.group(1)
        return dir_name

    def __handle_offline(self, local_store_parent_dir):
        """
        Analyze local log files only (--files). No SSH, no remote node.
        """
        resp = {"skip": False, "error": ""}
        node_results = []
        local_store_dir = os.path.join(local_store_parent_dir, "127.0.0.1")
        DirectoryUtil.mkdir(path=local_store_dir, stdio=self.stdio)

        log_list = self.__get_log_name_list_offline()
        if len(log_list) > self.file_number_limit:
            resp["skip"] = True
            resp["error"] = "Too many files {0} > {1}, Please adjust the number of incoming files".format(len(log_list), self.file_number_limit)
            return resp, node_results
        if len(log_list) == 0:
            resp["skip"] = True
            resp["error"] = "No files found"
            return resp, node_results

        self.stdio.print(FileUtil.show_file_list_tabulate("127.0.0.1", log_list, self.stdio))
        self.stdio.start_loading("analyze log start")
        for log_name in log_list:
            self.__pharse_offline_log_file(log_name=log_name, local_store_dir=local_store_dir)
            analyze_log_full_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
            file_result = self.__parse_log_lines(analyze_log_full_path)
            node_results.append(file_result)
        self.stdio.stop_loading("succeed")
        return resp, node_results

    def __get_log_name_list_offline(self):
        """
        :param:
        :return: log_name_list
        """
        log_name_list = []
        if self.analyze_files_list and len(self.analyze_files_list) > 0:
            for path in self.analyze_files_list:
                if os.path.exists(path):
                    if os.path.isfile(path):
                        log_name_list.append(path)
                    else:
                        log_names = FileUtil.find_all_file(path)
                        if len(log_names) > 0:
                            log_name_list.extend(log_names)
        self.stdio.verbose("get log list {}".format(log_name_list))
        return log_name_list

    def __pharse_offline_log_file(self, log_name, local_store_dir):
        """
        Copy or grep local log file to local_store_dir for parsing.
        """
        local_client = LocalClient(context=self.context, node={"ssh_type": "local"})
        local_store_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
        if self.grep_args is not None:
            grep_cmd = "grep -e '{grep_args}' {log_name} >> {local_store_path} ".format(grep_args=self.grep_args, log_name=log_name, local_store_path=local_store_path)
            self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
            local_client.exec_cmd(grep_cmd)
        else:
            download_file(local_client, log_name, local_store_path, self.stdio)

    def __get_observer_ret_code(self, log_line):
        """
        Get the ret code from the observer log
        :param log_line
        :return: ret_code
        """
        prefix = "ret=-"
        idx = log_line.find(prefix)
        if idx < 0:
            return ""
        start = idx + len(prefix)
        if start >= len(log_line):
            return ""
        end = start
        while end < len(log_line):
            c = log_line[end]
            if c < '0' or c > '9':
                break
            end = end + 1
        return "-" + log_line[start:end]

    def __parse_log_lines(self, file_full_path):
        """
        Process the observer's log line by line
        :param file_full_path
        :return: error_dict
        """
        error_dict = {}
        self.crash_error = ""
        self.stdio.verbose("start parse log {0}".format(file_full_path))
        with open(file_full_path, 'r', encoding='utf8', errors='ignore') as file:
            line_num = 0
            for line in file:
                line_num = line_num + 1
                line = line.strip()
                if line:
                    ##新增CRASH ERROR日志过滤
                    if line.find("CRASH ERROR") != -1:
                        ret_code = "CRASH_ERROR"
                        line_time = ""
                        trace_id = ""
                        ## 提取tname
                        tname_pattern = r"tname=([^,]+)"
                        tname_match = re.search(tname_pattern, line)
                        if tname_match:
                            error = tname_match.group(1)
                            if error != self.crash_error and self.crash_error != '':
                                self.crash_error = "{0},{1}".format(self.crash_error, error)
                            else:
                                self.crash_error = "{0}{1}".format("crash thread:", error)
                            self.stdio.print("crash_error:{0}".format(self.crash_error))
                        if error_dict.get(ret_code) is None:
                            error_dict[ret_code] = {"file_name": file_full_path, "count": 1, "first_found_time": line_time, "last_found_time": line_time, "trace_id_list": {trace_id} if len(trace_id) > 0 else {}}
                        else:
                            count = error_dict[ret_code]["count"] + 1
                            error_dict[ret_code] = {"file_name": file_full_path, "count": count, "first_found_time": line_time, "last_found_time": line_time, "trace_id_list": trace_id}
                        continue
                    line_time = self.__get_time_from_ob_log_line(line)
                    if len(line_time) == 0:
                        continue
                    real_level = self.__get_log_level(line)
                    if real_level < self.log_level:
                        continue
                    ret_code = self.__get_observer_ret_code(line)
                    if len(ret_code) > 1:
                        trace_id = self.__get_trace_id(line)
                        if trace_id is None:
                            continue
                        if error_dict.get(ret_code) is None:
                            error_dict[ret_code] = {"file_name": file_full_path, "count": 1, "first_found_time": line_time, "last_found_time": line_time, "trace_id_list": {trace_id} if len(trace_id) > 0 else {}}
                        else:
                            count = error_dict[ret_code]["count"] + 1
                            first_found_time = error_dict[ret_code]["first_found_time"] if error_dict[ret_code]["first_found_time"] < line_time else line_time
                            last_found_time = error_dict[ret_code]["last_found_time"] if error_dict[ret_code]["last_found_time"] > line_time else line_time
                            trace_id_list = list(error_dict[ret_code]["trace_id_list"])
                            if not (trace_id in trace_id_list):
                                trace_id_list.append(trace_id)
                            error_dict[ret_code] = {"file_name": file_full_path, "count": count, "first_found_time": first_found_time, "last_found_time": last_found_time, "trace_id_list": trace_id_list}
        self.stdio.verbose("complete parse log {0}".format(file_full_path))
        return error_dict

    def __get_time_from_ob_log_line(self, log_line):
        """
        Get the time from the observer's log line
        :param log_line
        :return: time_str
        """
        time_str = ""
        if len(log_line) >= 28:
            time_str = log_line[1 : log_line.find(']')]
        return time_str

    def __get_trace_id(self, log_line):
        """
        Get the trace_id from the observer's log line
        :param log_line
        :return: trace_id
        """
        pattern = re.compile(r'\[Y(.*?)\]')
        find = pattern.search(log_line)
        if find and find.group(1):
            return find.group(1).strip('[').strip(']')

    def __get_log_level(self, log_line):
        """
        Get the log level from the observer's log line
        :param log_line
        :return: log level
        """
        level_lits = ["DEBUG ", "TRACE ", "INFO ", "WDIAG ", "WARN ", "EDIAG ", "ERROR ", "FATAL "]
        length = len(log_line)
        if length > 38:
            length = 38
        for level in level_lits:
            idx = log_line[:length].find(level)
            if idx != -1:
                return OBLogLevel().get_log_level(level.rstrip())
        return 0

    def __get_overall_summary(self, node_summary_tuples, is_files=False):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple
        :return: a string indicating the overall summary
        """
        field_names = ["Node", "Status", "FileName", "First Found Time", "ErrorCode", "Message", "Count"]
        t = []
        t_details = []
        field_names_details = field_names
        field_names_details.extend(["Last Found Time", "Cause", "Solution", "Trace_IDS"])
        for tup in node_summary_tuples:
            is_empty = True
            node = tup[0]
            is_err = tup[2]
            node_results = tup[3]
            if is_err:
                is_empty = False
                t.append([node, "Error:" + tup[2] if is_err else "Completed", None, None, None, None])
                t_details.append([node, "Error:" + tup[2] if is_err else "Completed", None, None, None, None, None, None, None, None, None])
            for log_result in node_results:
                for ret_key, ret_value in log_result.items():
                    if ret_key is not None:
                        error_code_info = OB_RET_DICT.get(ret_key, "")
                        message = ""
                        if ret_key == "CRASH_ERROR":
                            message = self.crash_error
                        elif error_code_info == "":
                            continue
                        else:
                            message = error_code_info[1]
                        if len(error_code_info) > 3:
                            is_empty = False
                            t.append([node, "Error:" + tup[2] if is_err else "Completed", ret_value["file_name"], ret_value["first_found_time"], ret_key, message, ret_value["count"]])
                            t_details.append(
                                [
                                    node,
                                    "Error:" + tup[2] if is_err else "Completed",
                                    ret_value["file_name"],
                                    ret_value["first_found_time"],
                                    ret_key,
                                    message,
                                    ret_value["count"],
                                    ret_value["last_found_time"],
                                    error_code_info[2],
                                    error_code_info[3],
                                    str(ret_value["trace_id_list"]),
                                ]
                            )
            if is_empty:
                t.append([node, "PASS", None, None, None, None, None])
                t_details.append([node, "PASS", None, None, None, None, None, None, None, None, None])
        title = "\nAnalyze OceanBase Offline Log Summary:\n" if is_files else "\nAnalyze OceanBase Online Log Summary:\n"
        t.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=False)
        t_details.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=False)
        return title, field_names, t, t_details
