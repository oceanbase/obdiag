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
@time: 2022/6/23
@file: gather_log.py
@desc:
"""
import datetime
import os
import threading
import time
import uuid

import tabulate

from handler.base_shell_handler import BaseShellHandler
from common.logger import logger
from common.obdiag_exception import OBDIAGFormatException
from common.obdiag_exception import OBDIAGInvalidArgs
from common.constant import const
from common.command import LocalClient, SshClient
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file, parse_size, show_file_size_tabulate
from common.command import get_file_size, download_file, is_empty_dir, rm_rf_file, get_logfile_name_list, mkdir, delete_empty_file, zip_encrypt_dir, zip_dir
from utils.shell_utils import SshHelper
from utils.password_util import gen_password
from utils.time_utils import parse_time_str
from utils.time_utils import parse_time_length_to_sec
from utils.time_utils import timestamp_to_filename_time
from utils.time_utils import datetime_to_timestamp
from utils.utils import get_localhost_inner_ip, display_trace


class GatherLogHandler(BaseShellHandler):
    def __init__(self, nodes, gather_pack_dir, gather_timestamp=None, common_config=None, is_scene=False):
        super(GatherLogHandler, self).__init__(nodes)
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
        self.is_scene = is_scene
        self.config_path = const.DEFAULT_CONFIG_PATH
        if common_config is None:
            self.file_number_limit = 20
            self.file_size_limit = 2 * 1024 * 1024 * 1024
        else:
            self.file_number_limit = int(common_config["file_number_limit"])
            self.file_size_limit = int(parse_size(common_config["file_size_limit"]))

    def handle(self, args):
        # check args first
        if not self.__check_valid_and_parse_args(args):
            return
        # example of the format of pack dir for this command: {gather_pack_dir}/gather_pack_20190610123344
        if self.is_scene:
            pack_dir_this_command = self.gather_pack_dir
        else:
            pack_dir_this_command = os.path.join(self.gather_pack_dir, "gather_pack_{0}".format(timestamp_to_filename_time(self.gather_timestamp)))
            mkdir_if_not_exist(pack_dir_this_command)
        logger.info("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []
        gather_pack_path_dict = {}

        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(args, pack_dir_this_command, node)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            gather_tuples.append((node.get("ip"), False, resp["error"],
                                  file_size,
                                  resp["zip_password"],
                                  int(time.time() - st),
                                  resp["gather_pack_path"]))

        if self.is_ssh:
            for node in self.nodes:
                handle_from_node(node)
        else:
            local_ip = get_localhost_inner_ip()
            node = self.nodes[0]
            node["ip"] = local_ip
            for node in self.nodes:
                handle_from_node(node)

        summary_tuples = self.__get_overall_summary(gather_tuples, self.zip_encrypt)
        print(summary_tuples)
        display_trace(uuid.uuid3(uuid.NAMESPACE_DNS, str(os.getpid())))
        # Persist the summary results to a file
        write_result_append_to_file(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)

        last_info = "For result details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(pack_dir_this_command, "result_summary.txt"))
        print(last_info)

        # When using encryption mode, record the account and password information into the file
        return gather_tuples, gather_pack_path_dict

    def __handle_from_node(self, args, pack_dir_this_command, node):
        resp = {
            "skip": False,
            "error": "",
            "zip_password": "",
            "gather_pack_path": ""
        }
        remote_ip = node.get("ip") if self.is_ssh else get_localhost_inner_ip()
        remote_user = node.get("user")
        remote_password = node.get("password")
        remote_port = node.get("port")
        remote_private_key = node.get("private_key")
        remote_home_path = node.get("home_path")
        ssh_failed = False
        logger.info("Sending Collect Shell Command to node {0} ...".format(remote_ip))
        if "ssh_type" in node and node["ssh_type"] == "docker":
            local_store_dir = "{0}/docker_{1}".format(pack_dir_this_command, node["container_name"])
        else:
            local_store_dir = "{0}/{1}".format(pack_dir_this_command, remote_ip)
        try:
            ssh = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key, node)
        except Exception as e:
            logger.error("ssh {0}@{1}: failed, Please check the {2}".format(
                remote_user, 
                remote_ip, 
                self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)

        if not ssh_failed:   
            # transform timestamp(in us) to yyyymmddhhmmss (filename_time style)
            from_datetime_timestamp = timestamp_to_filename_time(datetime_to_timestamp(self.from_time_str))
            to_datetime_timestamp = timestamp_to_filename_time(datetime_to_timestamp(self.to_time_str))
            gather_dir_name = "ob_log_{0}_{1}_{2}".format(ssh.host_ip, from_datetime_timestamp, to_datetime_timestamp)
            gather_dir_full_path = "{0}/{1}".format("/tmp", gather_dir_name)
            mkdir(self.is_ssh, ssh, gather_dir_full_path)

            log_list, resp = self.__handle_log_list(ssh, node, resp)
            if resp["skip"]:
                return resp
            for log_name in log_list:
                self.__pharse_log(ssh_helper=ssh, log_name=log_name, home_path=remote_home_path, gather_path=gather_dir_full_path)
            delete_empty_file(self.is_ssh, ssh, gather_dir_full_path)

            is_empty = is_empty_dir(self.is_ssh, ssh, gather_dir_full_path)
            if is_empty:
                resp["error"] = "Empty file"
                resp["zip_password"] = ""
                rm_rf_file(self.is_ssh, ssh, gather_dir_full_path)
            else:
                self.__handle_zip_file(node.get("ip"), ssh, resp, gather_dir_name, pack_dir_this_command)
            ssh.ssh_close()
        return resp

    def __handle_log_list(self, ssh, node, resp):
        log_list = self.__get_log_name(ssh, node)
        ip = node.get("ip")
        if len(log_list) > self.file_number_limit:
            logger.warn(
                "{0} The number of log files is {1}, out of range (0,{2}], "
                "Please adjust the query limit".format(ip, len(log_list), self.file_number_limit))
            resp["skip"] = True,
            resp["error"] = "Too many files {0} > {1}".format(len(log_list), self.file_number_limit)
            return log_list, resp
        elif len(log_list) <= 0:
            logger.warn(
                "{0} The number of log files is {1}, The time range for file gather from {2} to {3}, and no eligible files were found"
                "Please adjust the query time limit".format(ip, len(log_list), self.from_time_str, self.to_time_str))
            resp["skip"] = True,
            resp["error"] = "No files found"
            return log_list, resp
        return log_list, resp

    def __get_log_name(self, ssh_helper, node):
        """
        通过传入的from to的时间来过滤一遍文件列表，提取出初步满足要求的文件列表
        :param ssh_helper:
        :return: list
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
            log_name_list = get_logfile_name_list(self.is_ssh, ssh_helper, self.from_time_str, self.to_time_str, log_path, log_files)
        else:
            logger.error("Unable to find the log file. Please provide the correct home_path, the default is [/root/observer]")
        return log_name_list

    def __pharse_log(self, ssh_helper, home_path, log_name, gather_path):
        """
        处理传入的日志文件，将满足条件的日志文件归集到一起
        :param ssh_helper, log_name, gather_path
        :return:
        """
        log_path = os.path.join(home_path, "log")
        if self.grep_args is not None:
            grep_cmd = "grep -e '{grep_args}' {log_dir}/{log_name} >> {gather_path}/{log_name} ".format(
                grep_args=self.grep_args,
                gather_path=gather_path,
                log_name=log_name,
                log_dir=log_path)
            logger.debug("grep files, run cmd = [{0}]".format(grep_cmd))
            SshClient().run(ssh_helper, grep_cmd) if self.is_ssh else LocalClient().run(grep_cmd)
        else:
            cp_cmd = "cp {log_dir}/{log_name} {gather_path}/{log_name} ".format(
                gather_path=gather_path,
                log_name=log_name,
                log_dir=log_path)
            logger.debug("copy files, run cmd = [{0}]".format(cp_cmd))
            SshClient().run(ssh_helper, cp_cmd) if self.is_ssh else LocalClient().run(cp_cmd)

    def __handle_zip_file(self, ip, ssh, resp, gather_dir_name, pack_dir_this_command):
        zip_password = ""
        gather_dir_full_path = "{0}/{1}".format(self.gather_ob_log_temporary_dir, gather_dir_name)
        if self.zip_encrypt:
            zip_password = gen_password(16)
            zip_encrypt_dir(self.is_ssh, ssh, zip_password, self.gather_ob_log_temporary_dir, gather_dir_name)
        else:
            zip_dir(self.is_ssh, ssh, self.gather_ob_log_temporary_dir, gather_dir_name)
        gather_package_dir = "{0}.zip".format(gather_dir_full_path)

        gather_log_file_size = get_file_size(self.is_ssh, ssh, gather_package_dir)
        print(show_file_size_tabulate(ip, gather_log_file_size))
        local_store_path = ""
        if int(gather_log_file_size) < self.file_size_limit:
            local_store_path = pack_dir_this_command + "/{0}.zip".format(gather_dir_name)
            download_file(self.is_ssh, ssh, gather_package_dir, local_store_path)
            resp["error"] = ""
            resp["zip_password"] = zip_password
        else:
            resp["error"] = "File too large"
            resp["zip_password"] = ""
        rm_rf_file(self.is_ssh, ssh, gather_package_dir)
        resp["gather_pack_path"] = local_store_path

        logger.debug(
            "Collect pack gathered from node {0}: stored in {1}".format(ip, gather_package_dir))
        return resp

    def __check_valid_and_parse_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        # 1: to timestamp must be larger than from timestamp, and be valid
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

        if hasattr(args, "grep") and args.grep is not None:
            self.grep_args = getattr(args, "grep")
        if hasattr(args, "scope") and args.scope is not None:
            self.scope = getattr(args, "scope")[0]
        if hasattr(args, "encrypt") and args.encrypt[0] == "true":
            self.zip_encrypt = True
        return True

    @staticmethod
    def __get_overall_summary(node_summary_tuple, is_zip_encrypt):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple: (node, is_err, err_msg, size, consume_time, node_summary) for each node
        :return: a string indicating the overall summary
        """
        summary_tab = []
        field_names = ["Node", "Status", "Size"]
        if is_zip_encrypt:
            field_names.append("Password")
        field_names.append("Time")
        field_names.append("PackPath")
        for tup in node_summary_tuple:
            node = tup[0]
            is_err = tup[2]
            file_size = tup[3]
            consume_time = tup[5]
            pack_path = tup[6]
            try:
                format_file_size = size_format(file_size, output_str=True)
            except:
                format_file_size = size_format(0, output_str=True)
            if is_zip_encrypt:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed",
                                    format_file_size, tup[4], "{0} s".format(int(consume_time)), pack_path))
            else:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed",
                                    format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather Ob Log Summary:\n" + \
               tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
