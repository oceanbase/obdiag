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
@time: 2023/01/11
@file: gather_obadmin.py
@desc:
"""
import os
import threading
import time
import datetime
import uuid

import tabulate
from common.logger import logger
from common.obdiag_exception import OBDIAGInvalidArgs, OBDIAGFormatException
from common.constant import const
from common.command import LocalClient, SshClient, is_empty_dir
from handler.base_shell_handler import BaseShellHandler
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file, parse_size, \
    show_file_size_tabulate
from common.command import download_file, rm_rf_file, get_file_size, zip_encrypt_dir, zip_dir, get_observer_version
from utils.password_util import gen_password
from utils.shell_utils import SshHelper
from utils.time_utils import parse_time_str, parse_time_length_to_sec
from utils.time_utils import timestamp_to_filename_time
from utils.version_utils import compare_versions_lower
from utils.utils import get_localhost_inner_ip, display_trace


class GatherObAdminHandler(BaseShellHandler):
    def __init__(self, nodes, gather_pack_dir, gather_timestamp, mode, common_config):
        super(GatherObAdminHandler, self).__init__(nodes)
        self.is_ssh = True
        self.gather_timestamp = gather_timestamp
        self.gather_ob_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        self.ob_install_dir = None
        self.ob_admin_mode = mode
        self.slog_dir = None
        self.clog_dir = None
        self.from_time_str = None
        self.to_time_str = None
        self.grep_args = None
        self.zip_encrypt = False
        self.obadmin_install_dir = None
        if common_config is None:
            self.file_size_limit = 2 * 1024 * 1024
        else:
            self.file_size_limit = int(parse_size(common_config["file_size_limit"]))

    def handle(self, args):
        if not self.__check_valid_args(args):
            raise OBDIAGInvalidArgs("Invalid args, args={0}".format(args))

        pack_dir_this_command = os.path.join(self.local_stored_path,
                                             "gather_pack_{0}".format(timestamp_to_filename_time(
                                                 self.gather_timestamp)))
        logger.info("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []

        def handle_from_node(ip, user, password, port, private_key):
            st = time.time()
            resp = self.__handle_from_node(args, ip, user, password, port, private_key, pack_dir_this_command)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            gather_tuples.append((ip, False, resp["error"],
                                  file_size,
                                  resp["zip_password"],
                                  int(time.time() - st),
                                  resp["gather_pack_path"]))

        if self.is_ssh:
            node_threads = [threading.Thread(None, handle_from_node, args=(
                node["ip"],
                node["user"],
                node["password"],
                node["port"],
                node["private_key"])) for node in self.nodes]
        else:
            node_threads = [threading.Thread(None, handle_from_node, args=(get_localhost_inner_ip(), "", "", "", ""))]

        list(map(lambda x: x.start(), node_threads))
        list(map(lambda x: x.join(timeout=const.GATHER_THREAD_TIMEOUT), node_threads))

        if self.ob_admin_mode == "slog":
            mode = "slog"
        else:
            mode = "clog"
        summary_tuples = self.__get_overall_summary(gather_tuples, mode, self.zip_encrypt)
        print(summary_tuples)
        display_trace(uuid.uuid3(uuid.NAMESPACE_DNS, str(os.getpid())))
        # Persist the summary results to a file
        write_result_append_to_file(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)

    def __handle_from_node(self, args, ip, user, password, port, private_key, local_stored_path):
        resp = {
            "skip": False,
            "error": "",
            "gather_pack_path": ""
        }
        remote_ip = ip if self.is_ssh else get_localhost_inner_ip()
        remote_user = user
        remote_password = password
        remote_port = port
        remote_private_key = private_key
        logger.info(
            "Sending Collect Shell Command to node {0} ...".format(remote_ip))
        mkdir_if_not_exist(local_stored_path)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        if self.ob_admin_mode == "slog":
            remote_dir_name = "slog_{0}_{1}".format(ip, now_time)
        else:
            remote_dir_name = "clog_{0}_{1}".format(ip, now_time)
        remote_dir_full_path = "/tmp/{0}".format(remote_dir_name)
        ssh_failed = False
        try:
            ssh_helper = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key)
        except Exception as e:
            config_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            logger.error("ssh {0}@{1}: failed, Please check the {2}/conf/config.yml file".format(
                remote_user, 
                remote_ip, 
                config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}/conf/config.yml".format(config_path)
        if not ssh_failed:
            mkdir_cmd = "mkdir -p {0}".format(remote_dir_full_path)
            SshClient().run(ssh_helper, mkdir_cmd) if self.is_ssh else LocalClient().run(mkdir_cmd)
            ob_version = get_observer_version(self.is_ssh, ssh_helper, self.ob_install_dir)
            if (ob_version != "" and not compare_versions_lower(ob_version, const.MAX_OB_VERSION_SUPPORT_GATHER_OBADMIN)) or ob_version == "":
                logger.info("This version {0} does not support gather clog/slog . The max supported version less than {1}".
                            format(ob_version, const.MAX_OB_VERSION_SUPPORT_GATHER_OBADMIN))
                resp["error"] = "{0} not support gather clog/slog".format(ob_version)
                resp["gather_pack_path"] = "{0}".format(local_stored_path)
                resp["zip_password"] = ""
                return resp
            log_list, resp = self.__handle_log_list(ssh_helper, ip, resp)
            for slog in log_list:
                self.__gather_log_info(ssh_helper, slog, remote_dir_full_path)

            self.__mv_log(ssh_helper, remote_dir_full_path)
            if is_empty_dir(self.is_ssh, ssh_helper, "/tmp/{0}".format(remote_dir_name)):
                resp["error"] = "gather failed, folder is empty"
                resp["zip_password"] = ""
            else:
                resp = self.__handle_zip_file(ip, ssh_helper, resp, remote_dir_name, local_stored_path)
                rm_rf_file(self.is_ssh, ssh_helper, remote_dir_full_path)
        return resp

    def __handle_log_list(self, ssh, ip, resp):
        log_list = self.__get_log_name(ssh)
        if len(log_list) > 20:
            logger.warn(
                "{0} The number of log files is {1}, out of range (0,20], "
                "Please adjust the query limit".format(ip, len(log_list)))
            resp["skip"] = True,
            resp["error"] = "Too many files {0} > 20".format(len(log_list))
            return log_list, resp
        elif len(log_list) <= 0:
            logger.warn(
                "{0} The number of log files is {1}, out of range (0,20], "
                "Please adjust the query limit".format(ip, len(log_list)))
            resp["skip"] = True,
            resp["error"] = "No files found"
            return log_list, resp
        return log_list, resp

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
        local_path = ""
        if int(gather_log_file_size) < self.file_size_limit:
            local_path = download_file(self.is_ssh, ssh, gather_package_dir, pack_dir_this_command)
            resp["error"] = ""
            resp["zip_password"] = zip_password
        else:
            resp["error"] = "File too large"
            resp["zip_password"] = ""
        rm_rf_file(self.is_ssh, ssh, gather_package_dir)
        resp["gather_pack_path"] = local_path

        logger.debug(
            "Collect pack gathered from node {0}: stored in {1}".format(ip, gather_package_dir))
        return resp

    def __get_log_name(self, ssh_helper):
        """
        通过传入的from to的时间来过滤一遍slog文件列表，提取出文件创建的时间
        :param ssh_helper:
        :return: list
        """
        if self.ob_admin_mode == "slog":
            get_log = "ls -l SLOG_DIR --time-style '+.%Y%m%d%H%M%S' | awk '{print $7,$6}'".replace("SLOG_DIR",
                                                                                                   self.slog_dir)
        else:
            get_log = "ls -l CLOG_DIR --time-style '+.%Y%m%d%H%M%S' | awk '{print $7,$6}'".replace("CLOG_DIR", self.clog_dir)
        log_files =  SshClient().run(ssh_helper, get_log) if self.is_ssh else LocalClient().run(get_log)
        log_name_list = []
        for file_name in log_files.split('\n'):
            if file_name == "":
                logger.warn("existing file name is empty")
                continue
            log_name_fields = file_name.split(".")
            if bytes.isdigit(log_name_fields[-1].encode("utf-8")) and len(log_name_fields[-1]) == 14:
                log_time = datetime.datetime.strptime(log_name_fields[-1], "%Y%m%d%H%M%S")
                from_time = datetime.datetime.strptime(self.from_time_str, "%Y-%m-%d %H:%M:%S")
                to_time = datetime.datetime.strptime(self.to_time_str, "%Y-%m-%d %H:%M:%S")
                if (log_time > from_time) and (log_time < to_time):
                    log_name_list.append(str(log_name_fields[0]).rstrip())
        if len(log_name_list):
            logger.info("Find the qualified log file {0} on Server [{1}], "
                        "wait for the next step".format(log_name_list, ssh_helper.host_ip))
        else:
            logger.warn("Failed to find the qualified log file on Server [{0}], "
                        "please check whether the input parameters are correct. ".format(ssh_helper.host_ip))
        return log_name_list

    def __gather_log_info(self, ssh_helper, log_name, remote_dir):
        if self.ob_admin_mode == "slog":
            cmd = "export LD_LIBRARY_PATH={ob_install_dir}/lib && cd {store_dir} && {obadmin_dir}/ob_admin slog_tool -f {slog_name}".format(
                ob_install_dir=self.ob_install_dir,
                store_dir=remote_dir,
                obadmin_dir=self.obadmin_install_dir,
                slog_name=log_name)
        else:
            cmd = "export LD_LIBRARY_PATH={ob_install_dir}/lib && cd {store_dir} && {obadmin_dir}/ob_admin clog_tool dump_all {clog_name}".format(
                ob_install_dir=self.ob_install_dir,
                store_dir=remote_dir,
                obadmin_dir=self.obadmin_install_dir,
                clog_name=log_name,
                )
        logger.info("gather obadmin info, run cmd = [{0}]".format(cmd))
        SshClient().run(ssh_helper, cmd) if self.is_ssh else LocalClient().run(cmd)

    def __mv_log(self, ssh_helper, remote_dir):
        if self.ob_admin_mode == "slog":
            cmd = "cd {remote_dir} && mv ob_admin.log ob_admin_slog.log".format(remote_dir=remote_dir)
        else:
            cmd = "cd {remote_dir} && mv ob_admin.log ob_admin_clog.log".format(remote_dir=remote_dir)
        logger.info("mv log info, run cmd = [{0}]".format(cmd))
        SshClient().run(ssh_helper, cmd) if self.is_ssh else LocalClient().run(cmd)

    def __check_valid_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        if self.ob_admin_mode == "clog":
            if getattr(args, "clog_dir") is not None:
                self.clog_dir = getattr(args, "clog_dir")[0]
        if self.ob_admin_mode == "slog":
            if getattr(args, "slog_dir") is not None:
                self.slog_dir = getattr(args, "slog_dir")[0]
        # 1: store_dir must exist, else return "No such file or directory".
        if getattr(args, "store_dir") is not None:
            if not os.path.exists(os.path.abspath(getattr(args, "store_dir"))):
                logger.error("Error: Set store dir {0} failed: No such directory."
                             .format(os.path.abspath(getattr(args, "store_dir"))))
                return False
            else:
                self.local_stored_path = os.path.abspath(getattr(args, "store_dir"))

        if getattr(args, "ob_install_dir") is not None:
            self.ob_install_dir = getattr(args, "ob_install_dir")
        else:
            self.ob_install_dir = const.OB_INSTALL_DIR_DEFAULT
        if getattr(args, "encrypt")[0] == "true":
            self.zip_encrypt = True
        # 3: to timestamp must be larger than from timestamp, and be valid
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
        elif (getattr(args, "from") is None or getattr(args, "to") is None) and args.since is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(
                seconds=parse_time_length_to_sec(args.since))).strftime('%Y-%m-%d %H:%M:%S')
        self.obadmin_install_dir = self.ob_install_dir + "/bin"
        return True

    @staticmethod
    def __get_overall_summary(node_summary_tuple, mode, is_zip_encrypt):
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
        if mode == "slog":
            return "\nGather slog Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid",
                                                                  showindex=False)
        else:
            return "\nGather clog Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid",
                                                                  showindex=False)
