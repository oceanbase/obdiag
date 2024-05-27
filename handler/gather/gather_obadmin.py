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
import time
import datetime

import tabulate
from common.obdiag_exception import OBDIAGFormatException
from common.constant import const
from common.command import LocalClient, SshClient, is_empty_dir
from handler.base_shell_handler import BaseShellHandler
from common.command import download_file, rm_rf_file, get_file_size, zip_encrypt_dir, zip_dir, get_observer_version
from common.ssh import SshHelper
from common.tool import TimeUtils
from common.tool import StringUtils
from common.tool import Util
from common.tool import DirectoryUtil
from common.tool import FileUtil
from common.tool import NetUtils


class GatherObAdminHandler(BaseShellHandler):
    def __init__(self, context, gather_pack_dir='./', is_scene=False):
        super(GatherObAdminHandler, self).__init__()
        self.context = context
        self.stdio = context.stdio
        self.is_ssh = True
        self.gather_ob_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        self.from_time_str = None
        self.to_time_str = None
        self.grep_args = None
        self.zip_encrypt = False
        self.config_path = const.DEFAULT_CONFIG_PATH
        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        self.nodes = self.context.cluster_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.nodes, self.stdio)
        if new_nodes:
            self.nodes = new_nodes
        self.inner_config = self.context.inner_config
        self.ob_admin_mode = 'clog'
        if self.context.get_variable("gather_obadmin_mode", None):
            self.ob_admin_mode = self.context.get_variable("gather_obadmin_mode")
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
        encrypt_option = Util.get_option(options, 'encrypt')
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
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option, self.stdio))).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.warn('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option is not None:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option, self.stdio))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('Error: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_path = os.path.abspath(store_dir_option)
        if encrypt_option == "true":
            self.zip_encrypt = True
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        pack_dir_this_command = os.path.join(self.local_stored_path, "gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
        self.stdio.verbose("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []

        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(pack_dir_this_command, node)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            gather_tuples.append((node.get("ip"), False, resp["error"], file_size, resp["zip_password"], int(time.time() - st), resp["gather_pack_path"]))

        if self.is_ssh:
            for node in self.nodes:
                handle_from_node(node)
        else:
            local_ip = NetUtils.get_inner_ip(self.stdio)
            node = self.nodes[0]
            node["ip"] = local_ip
            for node in self.nodes:
                handle_from_node(node)

        if self.ob_admin_mode == "slog":
            mode = "slog"
        else:
            mode = "clog"
        summary_tuples = self.__get_overall_summary(gather_tuples, mode, self.zip_encrypt)
        self.stdio.print(summary_tuples)
        # Persist the summary results to a file
        FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)

        last_info = "For result details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(pack_dir_this_command, "result_summary.txt"))

    def __handle_from_node(self, local_stored_path, node):
        resp = {"skip": False, "error": "", "gather_pack_path": ""}
        remote_ip = node.get("ip") if self.is_ssh else NetUtils.get_inner_ip()
        remote_user = node.get("ssh_username")
        remote_password = node.get("ssh_password")
        remote_port = node.get("ssh_port")
        remote_private_key = node.get("ssh_key_file")
        self.stdio.verbose("Sending Collect Shell Command to node {0} ...".format(remote_ip))
        DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        if self.ob_admin_mode == "slog":
            remote_dir_name = "slog_{0}_{1}".format(remote_ip, now_time)
        else:
            remote_dir_name = "clog_{0}_{1}".format(remote_ip, now_time)
        remote_dir_full_path = "/tmp/{0}".format(remote_dir_name)
        ssh_failed = False
        try:
            ssh_helper = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key, node, self.stdio)
        except Exception as e:
            self.stdio.error("ssh {0}@{1}: failed, Please check the {2}".format(remote_user, remote_ip, self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)
        if not ssh_failed:
            mkdir_cmd = "mkdir -p {0}".format(remote_dir_full_path)
            SshClient(self.stdio).run(ssh_helper, mkdir_cmd) if self.is_ssh else LocalClient(self.stdio).run(mkdir_cmd)
            ob_version = get_observer_version(self.is_ssh, ssh_helper, node.get("home_path"), self.stdio)
            if (ob_version != "" and not StringUtils.compare_versions_lower(ob_version, const.MAX_OB_VERSION_SUPPORT_GATHER_OBADMIN, self.stdio)) or ob_version == "":
                self.stdio.verbose("This version {0} does not support gather clog/slog . The max supported version less than {1}".format(ob_version, const.MAX_OB_VERSION_SUPPORT_GATHER_OBADMIN))
                resp["error"] = "{0} not support gather clog/slog".format(ob_version)
                resp["gather_pack_path"] = "{0}".format(local_stored_path)
                resp["zip_password"] = ""
                return resp
            log_list, resp = self.__handle_log_list(ssh_helper, remote_ip, resp)
            for slog in log_list:
                self.__gather_log_info(ssh_helper, node, slog, remote_dir_full_path)

            self.__mv_log(ssh_helper, remote_dir_full_path)
            if is_empty_dir(self.is_ssh, ssh_helper, "/tmp/{0}".format(remote_dir_name), self.stdio):
                resp["error"] = "gather failed, folder is empty"
                resp["zip_password"] = ""
            else:
                resp = self.__handle_zip_file(remote_ip, ssh_helper, resp, remote_dir_name, local_stored_path)
                rm_rf_file(self.is_ssh, ssh_helper, remote_dir_full_path, self.stdio)
        return resp

    def __handle_log_list(self, ssh, node, resp):
        log_list = self.__get_log_name(ssh, node)
        if len(log_list) > 20:
            self.stdio.warn("{0} The number of log files is {1}, out of range (0,20], " "Please adjust the query limit".format(node.get("ip"), len(log_list)))
            resp["skip"] = (True,)
            resp["error"] = "Too many files {0} > 20".format(len(log_list))
            return log_list, resp
        elif len(log_list) <= 0:
            self.stdio.warn("{0} The number of log files is {1}, out of range (0,20], " "Please adjust the query limit".format(node.get("ip"), len(log_list)))
            resp["skip"] = (True,)
            resp["error"] = "No files found"
            return log_list, resp
        return log_list, resp

    def __handle_zip_file(self, ip, ssh, resp, gather_dir_name, pack_dir_this_command):
        zip_password = ""
        gather_dir_full_path = "{0}/{1}".format(self.gather_ob_log_temporary_dir, gather_dir_name)
        if self.zip_encrypt:
            zip_password = Util.gen_password(16)
            zip_encrypt_dir(self.is_ssh, ssh, zip_password, self.gather_ob_log_temporary_dir, gather_dir_name, self.stdio)
        else:
            zip_dir(self.is_ssh, ssh, self.gather_ob_log_temporary_dir, gather_dir_name, self.stdio)
        gather_package_dir = "{0}.zip".format(gather_dir_full_path)
        gather_log_file_size = get_file_size(self.is_ssh, ssh, gather_package_dir, self.stdio)
        self.stdio.print(FileUtil.show_file_size_tabulate(ip, gather_log_file_size, self.stdio))
        local_path = ""
        if int(gather_log_file_size) < self.file_size_limit:
            local_path = download_file(self.is_ssh, ssh, gather_package_dir, pack_dir_this_command, self.stdio)
            resp["error"] = ""
            resp["zip_password"] = zip_password
        else:
            resp["error"] = "File too large"
            resp["zip_password"] = ""
        rm_rf_file(self.is_ssh, ssh, gather_package_dir, self.stdio)
        resp["gather_pack_path"] = local_path

        self.stdio.verbose("Collect pack gathered from node {0}: stored in {1}".format(ip, gather_package_dir))
        return resp

    def __get_log_name(self, ssh_helper, node):
        """
        通过传入的from to的时间来过滤一遍slog文件列表，提取出文件创建的时间
        :param ssh_helper:
        :return: list
        """
        slog_dir = os.path.join(node.get("data_dir"), "/slog")
        clog_dir = os.path.join(node.get("data_dir"), "/clog")
        if self.ob_admin_mode == "slog":
            get_log = "ls -l SLOG_DIR --time-style '+.%Y%m%d%H%M%S' | awk '{print $7,$6}'".replace("SLOG_DIR", slog_dir)
        else:
            get_log = "ls -l CLOG_DIR --time-style '+.%Y%m%d%H%M%S' | awk '{print $7,$6}'".replace("CLOG_DIR", clog_dir)
        log_files = SshClient(self.stdio).run(ssh_helper, get_log) if self.is_ssh else LocalClient(self.stdio).run(get_log)
        log_name_list = []
        for file_name in log_files.split('\n'):
            if file_name == "":
                self.stdio.verbose("existing file name is empty")
                continue
            log_name_fields = file_name.split(".")
            if bytes.isdigit(log_name_fields[-1].encode("utf-8")) and len(log_name_fields[-1]) == 14:
                log_time = datetime.datetime.strptime(log_name_fields[-1], "%Y%m%d%H%M%S")
                from_time = datetime.datetime.strptime(self.from_time_str, "%Y-%m-%d %H:%M:%S")
                to_time = datetime.datetime.strptime(self.to_time_str, "%Y-%m-%d %H:%M:%S")
                if (log_time > from_time) and (log_time < to_time):
                    log_name_list.append(str(log_name_fields[0]).rstrip())
        if len(log_name_list):
            self.stdio.verbose("Find the qualified log file {0} on Server [{1}], " "wait for the next step".format(log_name_list, ssh_helper.get_name()))
        else:
            self.stdio.warn("No found the qualified log file on Server [{0}]".format(ssh_helper.get_name()))
        return log_name_list

    def __gather_log_info(self, ssh_helper, node, log_name, remote_dir):
        home_path = node.get("home_path")
        obadmin_install_dir = os.path.join(home_path, "/bin")
        if self.ob_admin_mode == "slog":
            cmd = "export LD_LIBRARY_PATH={ob_install_dir}/lib && cd {store_dir} && {obadmin_dir}/ob_admin slog_tool -f {slog_name}".format(ob_install_dir=home_path, store_dir=remote_dir, obadmin_dir=obadmin_install_dir, slog_name=log_name)
        else:
            cmd = "export LD_LIBRARY_PATH={ob_install_dir}/lib && cd {store_dir} && {obadmin_dir}/ob_admin clog_tool dump_all {clog_name}".format(
                ob_install_dir=home_path,
                store_dir=remote_dir,
                obadmin_dir=obadmin_install_dir,
                clog_name=log_name,
            )
        self.stdio.verbose("gather obadmin info, run cmd = [{0}]".format(cmd))
        SshClient(self.stdio).run(ssh_helper, cmd) if self.is_ssh else LocalClient(self.stdio).run(cmd)

    def __mv_log(self, ssh_helper, remote_dir):
        if self.ob_admin_mode == "slog":
            cmd = "cd {remote_dir} && mv ob_admin.log ob_admin_slog.log".format(remote_dir=remote_dir)
        else:
            cmd = "cd {remote_dir} && mv ob_admin.log ob_admin_clog.log".format(remote_dir=remote_dir)
        self.stdio.verbose("mv log info, run cmd = [{0}]".format(cmd))
        SshClient(self.stdio).run(ssh_helper, cmd) if self.is_ssh else LocalClient(self.stdio).run(cmd)

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
                format_file_size = FileUtil.size_format(num=file_size, output_str=True)
            except:
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            if is_zip_encrypt:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, tup[4], "{0} s".format(int(consume_time)), pack_path))
            else:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        if mode == "slog":
            return "\nGather slog Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
        else:
            return "\nGather clog Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
