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
@time: 2023/01/31
@file: gather_obproxy_log.py
@desc:
"""
import datetime
import os
import time

import tabulate

from handler.base_shell_handler import BaseShellHandler
from common.obdiag_exception import OBDIAGFormatException
from common.command import LocalClient, SshClient
from common.constant import const
from common.command import get_file_size, download_file, is_empty_dir, get_logfile_name_list, mkdir, delete_empty_file, rm_rf_file, zip_encrypt_dir, zip_dir
from common.ssh import SshHelper
from common.tool import Util
from common.tool import DirectoryUtil
from common.tool import FileUtil
from common.tool import NetUtils
from common.tool import TimeUtils


class GatherObProxyLogHandler(BaseShellHandler):
    def __init__(self, context, gather_pack_dir='./', is_scene=False):
        super(GatherObProxyLogHandler, self).__init__()
        self.pack_dir_this_command = ""
        self.context = context
        self.stdio = context.stdio
        self.is_ssh = True
        self.gather_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.gather_pack_dir = gather_pack_dir
        self.log_dir = None
        self.from_time_str = None
        self.to_time_str = None
        self.grep_args = None
        self.scope = None
        self.zip_encrypt = False
        self.is_scene = is_scene
        self.config_path = const.DEFAULT_CONFIG_PATH
        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        self.nodes = self.context.obproxy_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.nodes, self.stdio)
        if new_nodes:
            self.nodes = new_nodes
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
        encrypt_option = Util.get_option(options, 'encrypt')
        scope_option = Util.get_option(options, 'scope')
        if self.context.get_variable("gather_from", None):
            from_option = self.context.get_variable("gather_from")
        if self.context.get_variable("gather_to", None):
            to_option = self.context.get_variable("gather_to")
        if self.context.get_variable("gather_since", None):
            since_option = self.context.get_variable("gather_since")
        if self.context.get_variable("store_dir", None):
            store_dir_option = self.context.get_variable("store_dir")
        if self.context.get_variable("gather_scope", None):
            scope_option = self.context.get_variable("gather_scope")
        if self.context.get_variable("gather_grep", None):
            grep_option = self.context.get_variable("gather_grep")

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
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.warn('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option is not None:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('warn: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        if scope_option:
            self.scope = scope_option
        if encrypt_option == "true":
            self.zip_encrypt = True
        if grep_option:
            self.grep_args = grep_option
        self.stdio.verbose("grep_args:{0}".format(grep_option))
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        if self.is_scene:
            pack_dir_this_command = self.gather_pack_dir
        else:
            pack_dir_this_command = os.path.join(self.gather_pack_dir, "gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
            DirectoryUtil.mkdir(path=pack_dir_this_command, stdio=self.stdio)
        self.stdio.verbose("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []

        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(node, pack_dir_this_command)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            gather_tuples.append((node.get("ip"), False, resp["error"], file_size, resp["zip_password"], int(time.time() - st), resp["gather_pack_path"]))

        if self.is_ssh:
            for node in self.nodes:
                handle_from_node(node)
        else:
            local_ip = NetUtils.get_inner_ip()
            node = self.nodes[0]
            node["ip"] = local_ip
            for node in self.nodes:
                handle_from_node(node)

        summary_tuples = self.__get_overall_summary(gather_tuples, self.zip_encrypt)
        self.stdio.print(summary_tuples)
        self.pack_dir_this_command = pack_dir_this_command
        FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)
        last_info = "For result details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(pack_dir_this_command, "result_summary.txt"))
        return True

    def __handle_from_node(self, node, pack_dir_this_command):
        resp = {"skip": False, "error": "", "zip_password": "", "gather_pack_path": ""}
        remote_ip = node.get("ip") if self.is_ssh else NetUtils.get_inner_ip()
        remote_user = node.get("ssh_username")
        remote_password = node.get("ssh_password")
        remote_port = node.get("ssh_port")
        remote_private_key = node.get("ssh_key_file")
        remote_home_path = node.get("home_path")
        ssh_failed = False
        self.stdio.verbose("Sending Collect Shell Command to node {0} ...".format(remote_ip))
        DirectoryUtil.mkdir(path=pack_dir_this_command, stdio=self.stdio)
        try:
            ssh = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key, node, self.stdio)
        except Exception as e:
            self.stdio.exception("ssh {0}@{1}: failed, Please check the {2}".format(remote_user, remote_ip, self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)
        if not ssh_failed:
            from_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.from_time_str))
            to_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.to_time_str))
            gather_dir_name = "obproxy_log_{0}_{1}_{2}".format(ssh.host_ip, from_datetime_timestamp, to_datetime_timestamp)
            gather_dir_full_path = "{0}/{1}".format("/tmp", gather_dir_name)
            mkdir(self.is_ssh, ssh, gather_dir_full_path, self.stdio)

            log_list, resp = self.__handle_log_list(ssh, node, resp)
            if resp["skip"]:
                return resp
            for log_name in log_list:
                self.__pharse_log(ssh_helper=ssh, log_name=log_name, home_path=remote_home_path, gather_path=gather_dir_full_path)
            delete_empty_file(self.is_ssh, ssh, gather_dir_full_path, self.stdio)

            is_empty = is_empty_dir(self.is_ssh, ssh, gather_dir_full_path, self.stdio)
            if is_empty:
                resp["error"] = "Empty file"
                resp["zip_password"] = ""
                rm_rf_file(self.is_ssh, ssh, gather_dir_full_path, self.stdio)
            else:
                self.__handle_zip_file(remote_ip, ssh, resp, gather_dir_name, pack_dir_this_command)
            ssh.ssh_close()
        return resp

    def __handle_log_list(self, ssh, node, resp):
        log_list = self.__get_log_name(ssh, node)
        ip = node.get("ip")
        if len(log_list) > self.file_number_limit:
            self.stdio.warn("{0} The number of log files is {1}, out of range (0,{2}], " "Please adjust the query limit".format(ip, len(log_list), self.file_number_limit))
            resp["skip"] = (True,)
            resp["error"] = "Too many files {0} > {1}".format(len(log_list), self.file_number_limit)
            return log_list, resp
        elif len(log_list) <= 0:
            self.stdio.warn("{0} The number of log files is {1}, The time range for file gather from {2} to {3}, and no eligible files were found." " Please adjust the query time limit.".format(ip, len(log_list), self.from_time_str, self.to_time_str))
            resp["skip"] = (True,)
            resp["error"] = "No files found"
            return log_list, resp
        return log_list, resp

    def __get_log_name(self, ssh_helper, node):
        home_path = node.get("home_path")
        log_path = os.path.join(home_path, "log")
        if self.scope == "obproxy" or self.scope == "obproxy_stat" or self.scope == "obproxy_digest" or self.scope == "obproxy_limit" or self.scope == "obproxy_slow" or self.scope == "obproxy_diagnosis" or self.scope == "obproxy_error":
            get_obproxy_log = "ls -1 -F %s/*%s.*log* | awk -F '/' '{print $NF}'" % (log_path, self.scope)
        else:
            get_obproxy_log = "ls -1 -F %s/obproxy.*log* %s/obproxy_error.*log* %s/obproxy_stat.*log* %s/obproxy_digest.*log* %s/obproxy_limit.*log* %s/obproxy_slow.*log* %s/obproxy_diagnosis.*log*| awk -F '/' '{print $NF}'" % (
                log_path,
                log_path,
                log_path,
                log_path,
                log_path,
                log_path,
                log_path,
            )
        if self.is_ssh:
            log_files = SshClient(self.stdio).run(ssh_helper, get_obproxy_log).strip()
        else:
            log_files = LocalClient(self.stdio).run(get_obproxy_log).strip()
        log_name_list = []
        if log_files:
            log_name_list = get_logfile_name_list(self.is_ssh, ssh_helper, self.from_time_str, self.to_time_str, log_path, log_files, self.stdio)
        else:
            self.stdio.error("Unable to find the log file. Please provide the correct home_path config and check obproxy {0} log exist".format(log_path))
        return log_name_list

    def __pharse_log(self, ssh_helper, home_path, log_name, gather_path):
        """
        处理传入的日志文件，将满足条件的日志文件归集到一起
        :param ssh_helper, log_name, gather_path
        :return:
        """
        log_path = os.path.join(home_path, "log")
        if self.grep_args is not None:
            grep_cmd = ""
            if type(self.grep_args) == str:
                grep_cmd = "grep -e '{grep_args}' {log_dir}/{log_name} >> {gather_path}/{log_name} ".format(grep_args=self.grep_args, gather_path=gather_path, log_name=log_name, log_dir=log_path)
            elif type(self.grep_args) == list and len(self.grep_args) > 0:
                grep_litter_cmd = ""
                for grep_arg in self.grep_args:
                    if type(grep_arg) != str:
                        self.stdio.error('The grep args must be string or list of strings, but got {0}'.format(type(grep_arg)))
                        raise Exception('The grep args must be string or list of strings, but got {0}'.format(type(grep_arg)))
                    elif grep_arg == "":
                        self.stdio.warn('The grep args must be string or list of strings, but got ""')
                        continue
                    grep_litter_cmd += "| grep -e '{0}'".format(grep_arg)

                grep_cmd = "cat {log_dir}/{log_name} {grep_args} >> {gather_path}/{log_name} ".format(grep_args=grep_litter_cmd, gather_path=gather_path, log_name=log_name, log_dir=log_path)
            self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
            SshClient(self.stdio).run(ssh_helper, grep_cmd) if self.is_ssh else LocalClient(self.stdio).run(grep_cmd)
        else:
            cp_cmd = "cp {log_dir}/{log_name} {gather_path}/{log_name} ".format(gather_path=gather_path, log_name=log_name, log_dir=log_path)
            self.stdio.verbose("copy files, run cmd = [{0}]".format(cp_cmd))
            SshClient(self.stdio).run(ssh_helper, cp_cmd) if self.is_ssh else LocalClient(self.stdio).run(cp_cmd)

    def __handle_zip_file(self, ip, ssh, resp, gather_dir_name, pack_dir_this_command):
        zip_password = ""
        gather_dir_full_path = "{0}/{1}".format(self.gather_log_temporary_dir, gather_dir_name)
        if self.zip_encrypt:
            zip_password = Util.gen_password(16)
            zip_encrypt_dir(self.is_ssh, ssh, zip_password, self.gather_log_temporary_dir, gather_dir_name, self.stdio)
        else:
            zip_dir(self.is_ssh, ssh, self.gather_log_temporary_dir, gather_dir_name, self.stdio)
        gather_package_dir = "{0}.zip".format(gather_dir_full_path)

        gather_log_file_size = get_file_size(self.is_ssh, ssh, gather_package_dir, self.stdio)
        self.stdio.print(FileUtil.show_file_size_tabulate(ip, gather_log_file_size))
        local_path = ""
        if int(gather_log_file_size) < self.file_size_limit:
            local_store_path = pack_dir_this_command + "/{0}.zip".format(gather_dir_name)
            local_path = download_file(self.is_ssh, ssh, gather_package_dir, local_store_path, self.stdio)
            resp["error"] = ""
            resp["zip_password"] = zip_password
        else:
            resp["error"] = "File too large"
            resp["zip_password"] = ""
        rm_rf_file(self.is_ssh, ssh, gather_package_dir, self.stdio)
        resp["gather_pack_path"] = local_path
        self.stdio.verbose("Collect pack gathered from node {0}: stored in {1}".format(ip, gather_package_dir))
        return resp

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
                format_file_size = FileUtil.size_format(num=file_size, output_str=True)
            except:
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            if is_zip_encrypt:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, tup[4], "{0} s".format(int(consume_time)), pack_path))
            else:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather ObProxy Log Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
