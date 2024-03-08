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
import threading
import uuid
import tabulate

from handler.base_shell_handler import BaseShellHandler
from common.logger import logger
from common.obdiag_exception import OBDIAGFormatException
from common.obdiag_exception import OBDIAGInvalidArgs
from common.constant import const
from common.command import LocalClient, SshClient, delete_file
from common.ob_log_level import OBLogLevel
from handler.meta.ob_error import OB_RET_DICT
from utils.file_utils import mkdir_if_not_exist, parse_size, find_all_file
from utils.file_utils import write_result_append_to_file, show_file_list_tabulate
from common.command import download_file, get_logfile_name_list, mkdir
from utils.shell_utils import SshHelper
from utils.time_utils import parse_time_str
from utils.time_utils import parse_time_length_to_sec
from utils.time_utils import timestamp_to_filename_time
from utils.time_utils import datetime_to_timestamp
from utils.utils import display_trace, get_localhost_inner_ip


class AnalyzeLogHandler(BaseShellHandler):
    def __init__(self, nodes, gather_pack_dir, gather_timestamp, common_config):
        super(AnalyzeLogHandler, self).__init__(nodes)
        self.directly_analyze_files = False
        self.analyze_files_list = []
        self.is_ssh = True
        self.gather_timestamp = gather_timestamp
        self.gather_ob_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.gather_pack_dir = gather_pack_dir
        self.ob_log_dir = None
        self.from_time_str = None
        self.to_time_str = None
        self.grep_args = None
        self.scope = None
        self.zip_encrypt = False
        self.log_level = OBLogLevel.WARN
        self.config_path = const.DEFAULT_CONFIG_PATH
        if common_config is None:
            self.file_number_limit = 20
            self.file_size_limit = 2 * 1024 * 1024
        else:
            self.file_number_limit = int(common_config["file_number_limit"])
            self.file_size_limit = int(parse_size(common_config["file_size_limit"]))

    def handle(self, args):
        if not self.__check_valid_and_parse_args(args):
            return
        local_store_parent_dir = os.path.join(self.gather_pack_dir,
                                              "analyze_pack_{0}".format(timestamp_to_filename_time(
                                                  self.gather_timestamp)))
        logger.info("Use {0} as pack dir.".format(local_store_parent_dir))
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

        title, field_names, summary_list, summary_details_list = self.__get_overall_summary(analyze_tuples, self.directly_analyze_files)
        table = tabulate.tabulate(summary_list, headers=field_names, tablefmt="grid", showindex=False)
        print(title)
        print(table)
        write_result_append_to_file(os.path.join(local_store_parent_dir, "result_details.txt"), title + str(table) + "\n\nDetails:\n\n")
        
        for m in range(len(summary_details_list)):
            for n in range(len(field_names)):
                extend = "\n\n" if n == len(field_names) -1 else "\n"
                write_result_append_to_file(os.path.join(local_store_parent_dir, "result_details.txt"), field_names[n] + ": " + str(summary_details_list[m][n]) + extend)
        last_info = "For more details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(local_store_parent_dir, "result_details.txt"))
        print(last_info)
        display_trace(uuid.uuid3(uuid.NAMESPACE_DNS, str(os.getpid())))
        return analyze_tuples

    def __handle_from_node(self, node, local_store_parent_dir):
        resp = {
            "skip": False,
            "error": ""
        }
        node_results = []
        remote_ip = node.get("ip") if self.is_ssh else '127.0.0.1'
        remote_user = node.get("user")
        remote_password = node.get("password")
        remote_port = node.get("port")
        remote_private_key = node.get("private_key")
        remote_home_path = node.get("home_path")
        logger.info("Sending Collect Shell Command to node {0} ...".format(remote_ip))
        mkdir_if_not_exist(local_store_parent_dir)
        if "ssh_type" in node and node["ssh_type"]=="docker":
            local_store_dir= "{0}/docker_{1}".format(local_store_parent_dir, node["container_name"])
        else:
            local_store_dir = "{0}/{1}".format(local_store_parent_dir, remote_ip.replace(".", "_"))
        mkdir_if_not_exist(local_store_dir)
        ssh_failed = False
        ssh = None
        try:
            ssh = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key,node)
        except Exception as e:
            logger.error("ssh {0}@{1}: failed, Please check the {2}".format(
                remote_user, 
                remote_ip, 
                self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)
        if not ssh_failed:
            from_datetime_timestamp = timestamp_to_filename_time(datetime_to_timestamp(self.from_time_str))
            to_datetime_timestamp = timestamp_to_filename_time(datetime_to_timestamp(self.to_time_str))
            gather_dir_name = "ob_log_{0}_{1}_{2}".format(ssh.host_ip, from_datetime_timestamp, to_datetime_timestamp)
            gather_dir_full_path = "{0}/{1}".format("/tmp", gather_dir_name)
            mkdir(self.is_ssh, ssh, gather_dir_full_path)

            log_list, resp = self.__handle_log_list(ssh, node, resp)
            if resp["skip"]:
                return resp, node_results
            print(show_file_list_tabulate(remote_ip, log_list))
            for log_name in log_list:
                if self.directly_analyze_files:
                    self.__pharse_offline_log_file(ssh_helper=ssh, log_name=log_name, local_store_dir=local_store_dir)
                    analyze_log_full_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
                else:
                    self.__pharse_log_file(ssh_helper=ssh, node=node, log_name=log_name,
                                        gather_path=gather_dir_full_path,
                                        local_store_dir=local_store_dir)
                    analyze_log_full_path = "{0}/{1}".format(local_store_dir, log_name)
                file_result = self.__parse_log_lines(analyze_log_full_path)
                node_results.append(file_result)
            delete_file(self.is_ssh, ssh, gather_dir_full_path)
            ssh.ssh_close()
        return resp, node_results

    def __handle_log_list(self, ssh, node, resp):
        if self.directly_analyze_files:
            log_list = self.__get_log_name_list_offline()
        else:
            log_list = self.__get_log_name_list(ssh, node)
        if len(log_list) > self.file_number_limit:
            logger.warn("{0} The number of log files is {1}, out of range (0,{2}]".format(node.get("ip"), len(log_list),
                                                                                          self.file_number_limit))
            resp["skip"] = True,
            resp["error"] = "Too many files {0} > {1}, Please adjust the analyze time range".format(len(log_list),
                                                                                                    self.file_number_limit)
            if self.directly_analyze_files:
                resp["error"] = "Too many files {0} > {1}, " \
                                "Please adjust the number of incoming files".format(len(log_list),
                                                                                    self.file_number_limit)
            return log_list, resp
        elif len(log_list) == 0:
            logger.warn(
                "{0} The number of log files is {1}, No files found, "
                "Please adjust the query limit".format(node.get("ip"), len(log_list)))
            resp["skip"] = True,
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
            get_oblog = "ls -1 -F %s/observer.log* %s/rootservice.log* %s/election.log* | awk -F '/' '{print $NF}'" % \
                        (log_path, log_path, log_path)
        log_name_list = []
        log_files = SshClient().run(ssh_helper, get_oblog) if self.is_ssh else LocalClient().run(get_oblog)
        if log_files:
            log_name_list = get_logfile_name_list(self.is_ssh, ssh_helper, self.from_time_str, self.to_time_str,
                                              log_path, log_files)
        else:
            logger.error("Unable to find the log file. Please provide the correct --ob_install_dir, the default is [/home/admin/oceanbase]")
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
                        log_names = find_all_file(path)
                        if len(log_names) > 0:
                            log_name_list.extend(log_names)
        logger.info("get log list {}".format(log_name_list))
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
            grep_cmd = "grep -e '{grep_args}' {log_dir}/{log_name} >> {gather_path}/{log_name} ".format(
                grep_args=self.grep_args,
                gather_path=gather_path,
                log_name=log_name,
                log_dir=log_path)
            logger.debug("grep files, run cmd = [{0}]".format(grep_cmd))
            SshClient().run(ssh_helper, grep_cmd) if self.is_ssh else LocalClient().run(grep_cmd)
            log_full_path = "{gather_path}/{log_name}".format(
                log_name=log_name,
                gather_path=gather_path
            )
            download_file(self.is_ssh, ssh_helper, log_full_path, local_store_path)
        else:
            real_time_logs = ["observer.log", "rootservice.log", "election.log", "trace.log", "observer.log.wf", "rootservice.log.wf", "election.log.wf", "trace.log.wf"]
            if log_name in real_time_logs:
                cp_cmd = "cp {log_dir}/{log_name} {gather_path}/{log_name} ".format(
                    gather_path=gather_path,
                    log_name=log_name,
                    log_dir=log_path)
                logger.debug("copy files, run cmd = [{0}]".format(cp_cmd))
                SshClient().run(ssh_helper, cp_cmd) if self.is_ssh else LocalClient().run(cp_cmd)
                log_full_path = "{gather_path}/{log_name}".format(log_name=log_name, gather_path=gather_path)
                download_file(self.is_ssh, ssh_helper, log_full_path, local_store_path)
            else:
                log_full_path = "{log_dir}/{log_name}".format(log_name=log_name, log_dir=log_path)
                download_file(self.is_ssh, ssh_helper, log_full_path, local_store_path)

    def __pharse_offline_log_file(self, ssh_helper, log_name, local_store_dir):
        """
        :param ssh_helper, log_name
        :return:
        """
        local_store_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
        if self.grep_args is not None:
            grep_cmd = "grep -e '{grep_args}' {log_name} >> {local_store_path} ".format(
                grep_args=self.grep_args,
                log_name=log_name,
                local_store_path=local_store_path)
            logger.info("grep files, run cmd = [{0}]".format(grep_cmd))
            SshClient().run(ssh_helper, grep_cmd) if self.is_ssh else LocalClient().run(grep_cmd)
        else:
            download_file(self.is_ssh, ssh_helper, log_name, local_store_path)

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
        logger.info("start parse log {0}".format(file_full_path))
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
                            error_dict[ret_code] = {
                                "file_name": file_full_path,
                                "count": 1,
                                "first_found_time": line_time,
                                "last_found_time": line_time,
                                "trace_id_list": {trace_id} if len(trace_id) > 0 else {}
                            }
                        else:
                            count = error_dict[ret_code]["count"] + 1
                            first_found_time = error_dict[ret_code]["first_found_time"] if error_dict[ret_code][
                                                                                               "first_found_time"] < line_time else line_time
                            last_found_time = error_dict[ret_code]["last_found_time"] if error_dict[ret_code][
                                                                                             "last_found_time"] > line_time else line_time
                            trace_id_list = list(error_dict[ret_code]["trace_id_list"])
                            if not (trace_id in trace_id_list):
                                trace_id_list.append(trace_id)
                            error_dict[ret_code] = {
                                "file_name": file_full_path,
                                "count": count,
                                "first_found_time": first_found_time,
                                "last_found_time": last_found_time,
                                "trace_id_list": trace_id_list
                            }
        logger.info("complete parse log {0}".format(file_full_path))
        return error_dict

    def __get_time_from_ob_log_line(self, log_line):
        """
        Get the time from the observer's log line
        :param log_line
        :return: time_str
        """
        time_str = ""
        if len(log_line) >= 28:
            time_str = log_line[1: log_line.find(']')]
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


    def __check_valid_and_parse_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        if getattr(args, "files") is not None:
            self.directly_analyze_files = True
            self.analyze_files_list = getattr(args, "files")
            self.is_ssh = False
        # to timestamp must be larger than from timestamp, and be valid
        if getattr(args, "from") is not None and getattr(args, "to") is not None:
            try:
                from_timestamp = parse_time_str(getattr(args, "from"))
                to_timestamp = parse_time_str(getattr(args, "to"))
                self.from_time_str = getattr(args, "from")
                self.to_time_str = getattr(args, "to")
            except OBDIAGFormatException:
                logger.error("Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. " \
                             "from_datetime={0}, to_datetime={1}".format(getattr(args, "from"), getattr(args, "to")))
                return False
            if to_timestamp <= from_timestamp:
                logger.error("Error: from datetime is larger than to datetime, please check.")
                return False
        else:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if args.since is not None:
                self.from_time_str = (now_time - datetime.timedelta(
                    seconds=parse_time_length_to_sec(args.since))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
        # 2: store_dir must exist, else create directory.
        if getattr(args, "store_dir") is not None:
            if not os.path.exists(os.path.abspath(getattr(args, "store_dir"))):
                logger.warn("Error: args --store_dir [{0}] incorrect: No such directory, Now create it".format(os.path.abspath(getattr(args, "store_dir"))))
                os.makedirs(os.path.abspath(getattr(args, "store_dir")))
            self.gather_pack_dir = os.path.abspath(getattr(args, "store_dir"))

        if getattr(args, "grep") is not None:
            self.grep_args = ' '.join(getattr(args, "grep"))
        if getattr(args, "scope") is not None:
            self.scope = getattr(args, "scope")[0]
        if getattr(args, "log_level") is not None:
            self.log_level = OBLogLevel().get_log_level(getattr(args, "log_level")[0])
        return True

    @staticmethod
    def __get_overall_summary(node_summary_tuples, is_files=False):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple
        :return: a string indicating the overall summary
        """
        field_names = [
            "Node", "Status", "FileName", "ErrorCode",
            "Message", "Count"
        ]
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
                            t.append([node,
                                           "Error:" + tup[2] if is_err else "Completed",
                                           ret_value["file_name"],
                                           ret_key,
                                           error_code_info[1],
                                           ret_value["count"]
                                           ])
                            t_details.append([node,
                                           "Error:" + tup[2] if is_err else "Completed",
                                           ret_value["file_name"],
                                           ret_key,
                                           error_code_info[1],
                                           ret_value["count"],
                                           error_code_info[2],
                                           error_code_info[3],
                                           ret_value["first_found_time"],
                                           ret_value["last_found_time"],
                                           str(ret_value["trace_id_list"])
                                           ])
            if is_empty:
                t.append([node, "\033[32mPASS\033[0m", None, None, None, None])
                t_details.append([node, "\033[32mPASS\033[0m", None, None, None, None, None, None, None, None, None])
        title = "\nAnalyze OceanBase Offline Log Summary:\n" if is_files else "\nAnalyze OceanBase Online Log Summary:\n"
        t.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=False)
        t_details.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=False)
        return title, field_names, t, t_details