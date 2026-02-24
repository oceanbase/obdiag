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
@time: 2023/01/11
@file: gather_obadmin.py
@desc:
"""
import os
import time
import datetime

# Removed tabulate import - now using BaseHandler._generate_summary_table
from src.common.base_handler import BaseHandler
from src.common.obdiag_exception import OBDIAGFormatException
from src.common.constant import const
from src.common.ssh_client.ssh import SshClient
from src.common.command import is_empty_dir, download_file, rm_rf_file, get_file_size, get_observer_version, tar_gz_dir
from src.common.tool import TimeUtils
from src.common.tool import StringUtils
from src.common.tool import Util
from src.common.tool import DirectoryUtil
from src.common.tool import FileUtil
from src.common.tool import NetUtils
from src.common.result_type import ObdiagResult


class GatherObAdminHandler(BaseHandler):
    def _init(self, gather_pack_dir='./', is_scene=False, **kwargs):
        """Subclass initialization"""
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

        # Initialize config
        self.nodes = self.context.cluster_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.nodes, self.stdio)
        if new_nodes:
            self.nodes = new_nodes

        self.ob_admin_mode = 'clog'
        if self.context.get_variable("gather_obadmin_mode", None):
            self.ob_admin_mode = self.context.get_variable("gather_obadmin_mode")

        # Use ConfigAccessor if available
        if self.config:
            self._file_size_limit = self.config.gather_file_size_limit
            self.config_path = self.config.basic_config_path
        else:
            # Fallback to direct config access
            if self.context.inner_config is None:
                self._file_size_limit = 2 * 1024 * 1024 * 1024
            else:
                basic_config = self.context.inner_config['obdiag']['basic']
                self._file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
                self.config_path = basic_config['config_path']

        # Initialize options
        from_option = self._get_option('from')
        to_option = self._get_option('to')
        since_option = self._get_option('since')
        store_dir_option = self._get_option('store_dir')
        encrypt_option = self._get_option('encrypt')

        if from_option is not None and to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(from_option)
                to_timestamp = TimeUtils.parse_time_str(to_option)
                self.from_time_str = from_option
                self.to_time_str = to_option
            except OBDIAGFormatException:
                raise ValueError(f'Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. from_datetime={from_option}, to_datetime={to_option}')
            if to_timestamp <= from_timestamp:
                raise ValueError('Error: from datetime is larger than to datetime, please check.')
        elif (from_option is None or to_option is None) and since_option is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option, self.stdio))).strftime('%Y-%m-%d %H:%M:%S')
            self._log_info(f'gather from_time: {self.from_time_str}, to_time: {self.to_time_str}')
        else:
            self._log_info('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option is not None:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option, self.stdio))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self._log_info(f'gather from_time: {self.from_time_str}, to_time: {self.to_time_str}')

        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self._log_warn(f'args --store_dir [{os.path.abspath(store_dir_option)}] incorrect: No such directory, Now create it')
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_path = os.path.abspath(store_dir_option)

        if encrypt_option == "true":
            self.zip_encrypt = True

    def handle(self) -> ObdiagResult:
        """Main handle logic"""
        self._validate_initialized()

        try:
            pack_dir_this_command = os.path.join(self.local_stored_path, f"obdiag_gather_{TimeUtils.timestamp_to_filename_time(self.gather_timestamp)}")
            self._log_verbose(f"Use {pack_dir_this_command} as pack dir.")
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
                handle_from_node(node)

            if self.ob_admin_mode == "slog":
                mode = "slog"
            else:
                mode = "clog"
            summary_tuples = self.__get_overall_summary(gather_tuples, mode, self.zip_encrypt)
            self._log_info(summary_tuples)
            # Persist the summary results to a file
            FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)

            last_info = f"For result details, please run cmd \033[32m' cat {os.path.join(pack_dir_this_command, 'result_summary.txt')} '\033[0m\n"
            self._log_info(last_info)
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

        except Exception as e:
            return self._handle_error(e)

    def __handle_from_node(self, local_stored_path, node):
        resp = {"skip": False, "error": "", "gather_pack_path": ""}
        remote_ip = node.get("ip") if self.is_ssh else NetUtils.get_inner_ip()
        remote_user = node.get("ssh_username")
        remote_password = node.get("ssh_password")
        remote_port = node.get("ssh_port")
        remote_private_key = node.get("ssh_key_file")
        self._log_verbose(f"Sending Collect Shell Command to node {remote_ip} ...")
        DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        if self.ob_admin_mode == "slog":
            remote_dir_name = f"slog_{remote_ip}_{now_time}"
        else:
            remote_dir_name = f"clog_{remote_ip}_{now_time}"
        remote_dir_full_path = f"/tmp/{remote_dir_name}"
        ssh_failed = False
        ssh_client = None
        try:
            ssh_client = SshClient(self.context, node)
        except Exception as e:
            self._log_error(f"ssh {remote_user}@{remote_ip}: failed, Please check the {self.config_path}")
            resp["skip"] = True
            resp["error"] = "Please check the node conf."
            return resp
        if not ssh_failed:
            mkdir_cmd = f"mkdir -p {remote_dir_full_path}"
            ssh_client.exec_cmd(mkdir_cmd)
            ob_version = get_observer_version(self.context)
            if (ob_version != "" and not StringUtils.compare_versions_lower(ob_version, const.MAX_OB_VERSION_SUPPORT_GATHER_OBADMIN, self.stdio)) or ob_version == "":
                self._log_verbose(f"This version {ob_version} does not support gather clog/slog . The max supported version less than {const.MAX_OB_VERSION_SUPPORT_GATHER_OBADMIN}")
                resp["error"] = f"{ob_version} not support gather clog/slog"
                resp["gather_pack_path"] = f"{local_stored_path}"
                resp["zip_password"] = ""
                return resp
            log_list, resp = self.__handle_log_list(ssh_client, remote_ip, resp)
            for slog in log_list:
                self.__gather_log_info(ssh_client, node, slog, remote_dir_full_path)

            self.__mv_log(ssh_client, remote_dir_full_path)
            if is_empty_dir(ssh_client, f"/tmp/{remote_dir_name}", self.stdio):
                resp["error"] = "gather failed, folder is empty"
                resp["zip_password"] = ""
            else:
                resp = self.__handle_zip_file(ssh_client, resp, remote_dir_name, pack_dir_this_command)
                rm_rf_file(ssh_client, remote_dir_full_path, self.stdio)
        return resp

    def __handle_log_list(self, ssh, node, resp):
        log_list = self.__get_log_name(ssh, node)
        if len(log_list) > 20:
            self._log_warn(f"{node.get('ip')} The number of log files is {len(log_list)}, out of range (0,20], Please adjust the query limit")
            resp["skip"] = (True,)
            resp["error"] = f"Too many files {len(log_list)} > 20"
            return log_list, resp
        elif len(log_list) <= 0:
            self._log_warn(f"{node.get('ip')} The number of log files is {len(log_list)}, out of range (0,20], Please adjust the query limit")
            resp["skip"] = (True,)
            resp["error"] = "No files found"
            return log_list, resp
        return log_list, resp

    def __handle_zip_file(self, ssh_client, resp, gather_dir_name, pack_dir_this_command):
        zip_password = ""
        gather_dir_full_path = f"{self.gather_ob_log_temporary_dir}/{gather_dir_name}"
        if self.zip_encrypt:
            zip_password = Util.gen_password(16)
        tar_gz_dir(ssh_client, self.gather_ob_log_temporary_dir, gather_dir_name, self.stdio)
        gather_package_dir = f"{gather_dir_full_path}.tar.gz"
        gather_log_file_size = get_file_size(ssh_client, gather_package_dir, self.stdio)
        self._log_info(FileUtil.show_file_size_tabulate(ssh_client, gather_log_file_size, self.stdio))
        local_path = ""
        if int(gather_log_file_size) < self._file_size_limit:
            local_store_tar_gz_file = f"{pack_dir_this_command}/{gather_dir_name}.tar.gz"
            download_file(ssh_client, gather_package_dir, local_store_tar_gz_file, self.stdio)
            local_path = f"{pack_dir_this_command}/{gather_dir_name}.zip"
            FileUtil.tar_gz_to_zip(pack_dir_this_command, local_store_tar_gz_file, local_path, zip_password, self.stdio)
            resp["error"] = ""
            resp["zip_password"] = zip_password
        else:
            resp["error"] = "File too large"
            resp["zip_password"] = ""
        rm_rf_file(ssh_client, gather_package_dir, self.stdio)
        resp["gather_pack_path"] = local_path

        self._log_verbose(f"Collect pack gathered from node {ssh_client.get_name()}: stored in {gather_package_dir}")
        return resp

    def __get_log_name(self, ssh_client, node):
        """
        通过传入的from to的时间来过滤一遍slog文件列表，提取出文件创建的时间
        :param ssh_client:
        :return: list
        """
        slog_dir = os.path.join(node.get("data_dir"), "/slog")
        clog_dir = os.path.join(node.get("data_dir"), "/clog")
        if self.ob_admin_mode == "slog":
            get_log = f"ls -l {slog_dir} --time-style '+.%Y%m%d%H%M%S' | awk '{{print $7,$6}}'"
        else:
            get_log = f"ls -l {clog_dir} --time-style '+.%Y%m%d%H%M%S' | awk '{{print $7,$6}}'"
        log_files = ssh_client.exec_cmd(get_log)
        log_name_list = []
        for file_name in log_files.split('\n'):
            if file_name == "":
                self._log_verbose("existing file name is empty")
                continue
            log_name_fields = file_name.split(".")
            if bytes.isdigit(log_name_fields[-1].encode("utf-8")) and len(log_name_fields[-1]) == 14:
                log_time = datetime.datetime.strptime(log_name_fields[-1], "%Y%m%d%H%M%S")
                from_time = datetime.datetime.strptime(self.from_time_str, "%Y-%m-%d %H:%M:%S")
                to_time = datetime.datetime.strptime(self.to_time_str, "%Y-%m-%d %H:%M:%S")
                if (log_time > from_time) and (log_time < to_time):
                    log_name_list.append(str(log_name_fields[0]).rstrip())
        if len(log_name_list):
            self._log_verbose(f"Find the qualified log file {log_name_list} on Server [{ssh_client.get_name()}], wait for the next step")
        else:
            self._log_warn(f"No found the qualified log file on Server [{ssh_client.get_name()}]")
        return log_name_list

    def __gather_log_info(self, ssh_client, node, log_name, remote_dir):
        home_path = node.get("home_path")
        obadmin_install_dir = os.path.join(home_path, "/bin")
        if self.ob_admin_mode == "slog":
            cmd = f"export LD_LIBRARY_PATH={home_path}/lib && cd {remote_dir} && {obadmin_install_dir}/ob_admin slog_tool -f {log_name}"
        else:
            cmd = f"export LD_LIBRARY_PATH={home_path}/lib && cd {remote_dir} && {obadmin_install_dir}/ob_admin clog_tool dump_all {log_name}"
        self._log_verbose(f"gather obadmin info, run cmd = [{cmd}]")
        ssh_client.exec_cmd(cmd)

    def __mv_log(self, ssh_client, remote_dir):
        if self.ob_admin_mode == "slog":
            cmd = f"cd {remote_dir} && mv ob_admin.log ob_admin_slog.log"
        else:
            cmd = f"cd {remote_dir} && mv ob_admin.log ob_admin_clog.log"
        self._log_verbose(f"mv log info, run cmd = [{cmd}]")
        ssh_client.exec_cmd(cmd)

    def __get_overall_summary(self, node_summary_tuple, mode, is_zip_encrypt):
        """
        Generate overall summary from gather tuples using BaseHandler template method.
        :param node_summary_tuple: List of tuples (node, is_err, error_msg, file_size, password, consume_time, pack_path)
        :param mode: "slog" or "clog"
        :param is_zip_encrypt: Whether zip encryption is enabled
        :return: Formatted summary table string
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
            except Exception as e:
                self._log_verbose("Failed to format file size {0}: {1}".format(file_size, e))
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            if is_zip_encrypt:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, tup[4], "{0} s".format(int(consume_time)), pack_path))
            else:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        # Use BaseHandler template method
        title = "Gather slog Summary" if mode == "slog" else "Gather clog Summary"
        return self._generate_summary_table(field_names, summary_tab, title)
