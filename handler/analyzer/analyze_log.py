#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
import os
import re
import tabulate

from handler.base_shell_handler import BaseShellHandler
from common.obdiag_exception import OBDIAGFormatException
from common.constant import const
from common.command import LocalClient, SshClient
from common.ob_log_level import OBLogLevel
from handler.meta.ob_error import OB_RET_DICT
from common.command import download_file, get_logfile_name_list, mkdir, delete_file
from common.ssh import SshHelper
from common.tool import Util
from common.tool import DirectoryUtil
from common.tool import FileUtil
from common.tool import TimeUtils


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
            self.stdio.print('analyze log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.warn('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option is not None:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('analyze log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option is not None:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('Error: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        if grep_option is not None:
            self.grep_args = grep_option
        if scope_option:
            self.scope = scope_option
        if log_level_option:
            self.log_level = OBLogLevel().get_log_level(scope_option)
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        local_store_parent_dir = os.path.join(self.gather_pack_dir, "analyze_pack_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp())))
        self.stdio.verbose("Use {0} as pack dir.".format(local_store_parent_dir))
        analyze_tuples = []

        def handle_from_node(node):
            resp, node_results = self.__handle_from_node(node, local_store_parent_dir)
            analyze_tuples.append((node.get("ip"), False, resp["error"], node_results))

        if self.is_ssh:
            for node in self.nodes:
                handle_from_node(node)
        else:
            local_ip = '127.0.0.1'
            node = self.nodes[0]
            node["ip"] = local_ip
            handle_from_node(node)

        self.stdio.start_loading('analyze result start')
        title, field_names, summary_list, summary_details_list = self.__get_overall_summary(analyze_tuples, self.directly_analyze_files)
        table = tabulate.tabulate(summary_list, headers=field_names, tablefmt="grid", showindex=False)
        self.stdio.stop_loading('analyze result sucess')
        self.stdio.print(title)
        self.stdio.print(table)
        FileUtil.write_append(os.path.join(local_store_parent_dir, "result_details.txt"), title + str(table) + "\n\nDetails:\n\n")

        for m in range(len(summary_details_list)):
            for n in range(len(field_names)):
                extend = "\n\n" if n == len(field_names) - 1 else "\n"
                FileUtil.write_append(os.path.join(local_store_parent_dir, "result_details.txt"), field_names[n] + ": " + str(summary_details_list[m][n]) + extend)
        last_info = "For more details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(local_store_parent_dir, "result_details.txt"))
        self.stdio.print(last_info)
        return analyze_tuples

    def __handle_from_node(self, node, local_store_parent_dir):
        resp = {"skip": False, "error": ""}
        node_results = []
        remote_ip = node.get("ip") if self.is_ssh else '127.0.0.1'
        remote_user = node.get("ssh_username")
        remote_password = node.get("ssh_password")
        remote_port = node.get("ssh_port")
        remote_private_key = node.get("ssh_key_file")
        remote_home_path = node.get("home_path")
        self.stdio.verbose("Sending Collect Shell Command to node {0} ...".format(remote_ip))
        DirectoryUtil.mkdir(path=local_store_parent_dir, stdio=self.stdio)
        if "ssh_type" in node and node["ssh_type"] == "docker":
            local_store_dir = "{0}/docker_{1}".format(local_store_parent_dir, node["container_name"])
        else:
            local_store_dir = "{0}/{1}".format(local_store_parent_dir, remote_ip.replace(".", "_"))
        DirectoryUtil.mkdir(path=local_store_dir, stdio=self.stdio)
        ssh_failed = False
        ssh = None
        try:
            ssh = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key, node, self.stdio)
        except Exception as e:
            self.stdio.error("ssh {0}@{1}: failed, Please check the {2}".format(remote_user, remote_ip, self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)
        if not ssh_failed:
            from_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.from_time_str))
            to_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.to_time_str))
            gather_dir_name = "ob_log_{0}_{1}_{2}".format(ssh.host_ip, from_datetime_timestamp, to_datetime_timestamp)
            gather_dir_full_path = "{0}/{1}".format("/tmp", gather_dir_name)
            mkdir(self.is_ssh, ssh, gather_dir_full_path, self.stdio)

            log_list, resp = self.__handle_log_list(ssh, node, resp)
            if resp["skip"]:
                return resp, node_results
            self.stdio.print(FileUtil.show_file_list_tabulate(remote_ip, log_list, self.stdio))
            for log_name in log_list:
                if self.directly_analyze_files:
                    self.__pharse_offline_log_file(ssh_helper=ssh, log_name=log_name, local_store_dir=local_store_dir)
                    analyze_log_full_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
                else:
                    self.__pharse_log_file(ssh_helper=ssh, node=node, log_name=log_name, gather_path=gather_dir_full_path, local_store_dir=local_store_dir)
                    analyze_log_full_path = "{0}/{1}".format(local_store_dir, log_name)
                self.stdio.start_loading('analyze log start')
                file_result = self.__parse_log_lines(analyze_log_full_path)
                self.stdio.stop_loading('analyze log sucess')
                node_results.append(file_result)
            delete_file(self.is_ssh, ssh, gather_dir_full_path, self.stdio)
            ssh.ssh_close()
        return resp, node_results

    def __handle_log_list(self, ssh, node, resp):
        if self.directly_analyze_files:
            log_list = self.__get_log_name_list_offline()
        else:
            log_list = self.__get_log_name_list(ssh, node)
        if len(log_list) > self.file_number_limit:
            self.stdio.warn("{0} The number of log files is {1}, out of range (0,{2}]".format(node.get("ip"), len(log_list), self.file_number_limit))
            resp["skip"] = (True,)
            resp["error"] = "Too many files {0} > {1}, Please adjust the analyze time range".format(len(log_list), self.file_number_limit)
            if self.directly_analyze_files:
                resp["error"] = "Too many files {0} > {1}, " "Please adjust the number of incoming files".format(len(log_list), self.file_number_limit)
            return log_list, resp
        elif len(log_list) == 0:
            self.stdio.warn("{0} The number of log files is {1}, No files found, " "Please adjust the query limit".format(node.get("ip"), len(log_list)))
            resp["skip"] = (True,)
            resp["error"] = "No files found"
            return log_list, resp
        return log_list, resp

    def __get_log_name_list(self, ssh_helper, node):
        """
        :param ssh_helper:
        :return: log_name_list
        """
        home_path = node.get("home_path")
        log_path = os.path.join(home_path, "log")
        if self.scope == "observer" or self.scope == "rootservice" or self.scope == "election":
            get_oblog = "ls -1 -F %s/*%s.log* | awk -F '/' '{print $NF}'" % (log_path, self.scope)
        else:
            get_oblog = "ls -1 -F %s/observer.log* %s/rootservice.log* %s/election.log* | awk -F '/' '{print $NF}'" % (log_path, log_path, log_path)
        log_name_list = []
        log_files = SshClient(self.stdio).run(ssh_helper, get_oblog) if self.is_ssh else LocalClient(self.stdio).run(get_oblog)
        if log_files:
            log_name_list = get_logfile_name_list(self.is_ssh, ssh_helper, self.from_time_str, self.to_time_str, log_path, log_files, self.stdio)
        else:
            self.stdio.error("Unable to find the log file. Please provide the correct --ob_install_dir, the default is [/home/admin/oceanbase]")
        return log_name_list

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

    def __pharse_log_file(self, ssh_helper, node, log_name, gather_path, local_store_dir):
        """
        :param ssh_helper, log_name, gather_path
        :return:
        """
        home_path = node.get("home_path")
        log_path = os.path.join(home_path, "log")
        local_store_path = "{0}/{1}".format(local_store_dir, log_name)
        if self.grep_args is not None:
            grep_cmd = "grep -e '{grep_args}' {log_dir}/{log_name} >> {gather_path}/{log_name} ".format(grep_args=self.grep_args, gather_path=gather_path, log_name=log_name, log_dir=log_path)
            self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
            SshClient(self.stdio).run(ssh_helper, grep_cmd) if self.is_ssh else LocalClient(self.stdio).run(grep_cmd)
            log_full_path = "{gather_path}/{log_name}".format(log_name=log_name, gather_path=gather_path)
            download_file(self.is_ssh, ssh_helper, log_full_path, local_store_path, self.stdio)
        else:
            real_time_logs = ["observer.log", "rootservice.log", "election.log", "trace.log", "observer.log.wf", "rootservice.log.wf", "election.log.wf", "trace.log.wf"]
            if log_name in real_time_logs:
                cp_cmd = "cp {log_dir}/{log_name} {gather_path}/{log_name} ".format(gather_path=gather_path, log_name=log_name, log_dir=log_path)
                self.stdio.verbose("copy files, run cmd = [{0}]".format(cp_cmd))
                SshClient(self.stdio).run(ssh_helper, cp_cmd) if self.is_ssh else LocalClient(self.stdio).run(cp_cmd)
                log_full_path = "{gather_path}/{log_name}".format(log_name=log_name, gather_path=gather_path)
                download_file(self.is_ssh, ssh_helper, log_full_path, local_store_path, self.stdio)
            else:
                log_full_path = "{log_dir}/{log_name}".format(log_name=log_name, log_dir=log_path)
                download_file(self.is_ssh, ssh_helper, log_full_path, local_store_path, self.stdio)

    def __pharse_offline_log_file(self, ssh_helper, log_name, local_store_dir):
        """
        :param ssh_helper, log_name
        :return:
        """
        local_store_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
        if self.grep_args is not None:
            grep_cmd = "grep -e '{grep_args}' {log_name} >> {local_store_path} ".format(grep_args=self.grep_args, log_name=log_name, local_store_path=local_store_path)
            self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
            SshClient(self.stdio).run(ssh_helper, grep_cmd) if self.is_ssh else LocalClient(self.stdio).run(grep_cmd)
        else:
            download_file(self.is_ssh, ssh_helper, log_name, local_store_path, self.stdio)

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
        self.stdio.verbose("start parse log {0}".format(file_full_path))
        with open(file_full_path, 'r', encoding='utf8', errors='ignore') as file:
            line_num = 0
            for line in file:
                line_num = line_num + 1
                line = line.strip()
                if line:
                    line_time = self.__get_time_from_ob_log_line(line)
                    if len(line_time) == 0:
                        continue
                    real_level = self.__get_log_level(line)
                    if real_level < self.log_level:
                        continue
                    ret_code = self.__get_observer_ret_code(line)
                    if len(ret_code) > 1:
                        trace_id = self.__get_trace_id(line)
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

    @staticmethod
    def __get_overall_summary(node_summary_tuples, is_files=False):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple
        :return: a string indicating the overall summary
        """
        field_names = ["Node", "Status", "FileName", "ErrorCode", "Message", "Count"]
        t = []
        t_details = []
        field_names_details = field_names
        field_names_details.extend(["Cause", "Solution", "First Found Time", "Last Found Time", "Trace_IDS"])
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
                        if len(error_code_info) > 3:
                            is_empty = False
                            t.append([node, "Error:" + tup[2] if is_err else "Completed", ret_value["file_name"], ret_key, error_code_info[1], ret_value["count"]])
                            t_details.append(
                                [
                                    node,
                                    "Error:" + tup[2] if is_err else "Completed",
                                    ret_value["file_name"],
                                    ret_key,
                                    error_code_info[1],
                                    ret_value["count"],
                                    error_code_info[2],
                                    error_code_info[3],
                                    ret_value["first_found_time"],
                                    ret_value["last_found_time"],
                                    str(ret_value["trace_id_list"]),
                                ]
                            )
            if is_empty:
                t.append([node, "\033[32mPASS\033[0m", None, None, None, None])
                t_details.append([node, "\033[32mPASS\033[0m", None, None, None, None, None, None, None, None, None])
        title = "\nAnalyze OceanBase Offline Log Summary:\n" if is_files else "\nAnalyze OceanBase Online Log Summary:\n"
        t.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=False)
        t_details.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=False)
        return title, field_names, t, t_details
