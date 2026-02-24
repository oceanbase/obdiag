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
@time: 2020/7/14
@file: gather_sysstat.py
@desc:
"""
import os
import time
import datetime

# Removed tabulate import - now using BaseHandler._generate_summary_table
from src.common.base_handler import BaseHandler
from src.common.constant import const
from src.common.ssh_client.ssh import SshClient
from src.common.command import get_file_size, download_file, mkdir
from src.common.tool import Util
from src.common.tool import DirectoryUtil
from src.common.tool import FileUtil
from src.common.tool import TimeUtils
from src.common.result_type import ObdiagResult


class GatherOsInfoHandler(BaseHandler):
    def _init(self, gather_pack_dir='./', is_scene=False, **kwargs):
        """Subclass initialization"""
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        self.is_scene = is_scene
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

        for node in self.nodes:
            if node.get("ssh_type") == "docker":
                self._log_warn("the ssh_type is docker not support sysstat")
                raise ValueError("the ssh_type is docker not support sysstat")

        # Initialize options
        store_dir_option = self._get_option('store_dir')
        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self._log_warn(f'args --store_dir [{os.path.abspath(store_dir_option)}] incorrect: No such directory, Now create it')
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_path = os.path.abspath(store_dir_option)

        self.scope_option = self._get_option('scope')

    def handle(self) -> ObdiagResult:
        """Main handle logic"""
        self._validate_initialized()

        try:
            if self.is_scene:
                pack_dir_this_command = self.local_stored_path
            else:
                pack_dir_this_command = os.path.join(self.local_stored_path, f"obdiag_gather_{TimeUtils.timestamp_to_filename_time(self.gather_timestamp)}")
            self._log_verbose(f"Use {pack_dir_this_command} as pack dir.")
            gather_tuples = []

            def handle_from_node(node):
                st = time.time()
                resp = self.__handle_from_node(node, pack_dir_this_command)
                file_size = ""
                if len(resp["error"]) == 0:
                    file_size = os.path.getsize(resp["gather_pack_path"])
                gather_tuples.append((node.get("ip"), False, resp["error"], file_size, int(time.time() - st), resp["gather_pack_path"]))

            exec_tag = False
            for node in self.nodes:
                if node.get("ssh_type") == "docker" or node.get("ssh_type") == "kubernetes":
                    self._log_warn(f"Skip gather from node {node.get('ip')} because it is a docker or kubernetes node")
                    continue
                handle_from_node(node)
                exec_tag = True

            if not exec_tag:
                self._log_verbose("No node to gather from, skip")
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

            summary_tuples = self.__get_overall_summary(gather_tuples)
            self._log_info(summary_tuples)
            # Persist the summary results to a file
            FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

        except Exception as e:
            return self._handle_error(e)

    def __handle_from_node(self, node, local_stored_path):
        resp = {"skip": False, "error": "", "gather_pack_path": ""}
        remote_ip = node.get("ip")
        remote_user = node.get("ssh_username")
        self._log_verbose(f"Sending Collect Shell Command to node {remote_ip} ...")
        DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        remote_dir_name = "sysstat_{0}_{1}".format(remote_ip.replace(":", "_"), now_time)
        remote_dir_full_path = "/tmp/{0}".format(remote_dir_name)
        ssh_failed = False
        ssh_client = None
        try:
            ssh_client = SshClient(self.context, node)
        except Exception as e:
            self.stdio.exception(f"ssh {remote_user}@{remote_ip}: failed, Please check the node conf.")
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the node conf."
        if not ssh_failed:
            mkdir(ssh_client, remote_dir_full_path, self.stdio)

            self.__gather_dmesg_boot_info(ssh_client, remote_dir_full_path)
            self.__gather_dmesg_current_info(ssh_client, remote_dir_full_path)
            if self.__tsar_exit(ssh_client):
                self.__gather_cpu_info(ssh_client, remote_dir_full_path)
                self.__gather_mem_info(ssh_client, remote_dir_full_path)
                self.__gather_swap_info(ssh_client, remote_dir_full_path)
                self.__gather_io_info(ssh_client, remote_dir_full_path)
                self.__gather_traffic_info(ssh_client, remote_dir_full_path)
                self.__gather_tcp_udp_info(ssh_client, remote_dir_full_path)
            tar_cmd = f"cd /tmp && tar -czf {remote_dir_name}.tar.gz {remote_dir_name}/*"
            self._log_verbose(f"tar the pack by {tar_cmd}")
            tar_request = ssh_client.exec_cmd(f"cd /tmp && tar -czf {remote_dir_name}.tar.gz {remote_dir_name}/*")
            self._log_verbose(f"tar_request: {tar_request}")
            remote_file_full_path = f"/tmp/{remote_dir_name}.tar.gz"
            file_size = get_file_size(ssh_client, remote_file_full_path, self.stdio)
            if int(file_size) < self._file_size_limit:
                local_file_path = f"{local_stored_path}/{os.path.basename(remote_file_full_path)}"
                self._log_verbose(f"local file path {local_file_path}...")
                download_file(ssh_client, remote_file_full_path, local_file_path, self.stdio)
                resp["error"] = ""
                ssh_client.exec_cmd(f"rm -rf {remote_file_full_path}")
                self._log_verbose(f"download success. On node {ssh_client.get_ip()} delete file: {remote_file_full_path}")
            else:
                resp["error"] = "File too large"
            resp["gather_pack_path"] = f"{local_stored_path}/{os.path.basename(remote_file_full_path)}"
        return resp

    def __gather_dmesg_current_info(self, ssh_client, gather_path):
        try:
            dmesg_cmd = f"dmesg --ctime > {gather_path}/dmesg.human.current"
            self._log_verbose(f"gather dmesg current info, run cmd = [{dmesg_cmd}]")
            ssh_client.exec_cmd(dmesg_cmd)
        except Exception as e:
            self._log_error(f"Failed to gather dmesg current info on server {ssh_client.get_name()}: {e}")

    def __gather_dmesg_boot_info(self, ssh_client, dir_path):
        try:
            file_exit_cmd = "ls -l /var/log/dmesg 2>/dev/null"
            file_exit = ssh_client.exec_cmd(file_exit_cmd)
            if file_exit:
                dmesg_cmd = f'cp --force /var/log/dmesg {dir_path}/dmesg.boot'
                self._log_verbose(f"gather dmesg boot info on server {ssh_client.get_name()}, run cmd = [{dmesg_cmd}]")
                ssh_client.exec_cmd(dmesg_cmd)
            else:
                self._log_warn(f"the file /var/log/dmesg on server {ssh_client.get_name()} not found")
        except Exception as e:
            self._log_error(f"Failed to gather the /var/log/dmesg on server {ssh_client.get_name()}: {e}")

    def __tsar_exit(self, ssh_client):
        try:
            cmd = "tar --help"
            exit = ssh_client.exec_cmd(cmd)
            if exit:
                return True
        except Exception as e:
            self._log_warn("tsar not found: {0}".format(e))

    def __gather_cpu_info(self, ssh_client, gather_path):
        try:
            tsar_cmd = f"tsar --cpu -i 1 > {gather_path}/one_day_cpu_data.txt"
            self._log_verbose(f"gather cpu info on server {ssh_client.get_name()}, run cmd = [{tsar_cmd}]")
            ssh_client.exec_cmd(tsar_cmd)
        except Exception as e:
            self._log_error(f"Failed to gather cpu info use tsar on server {ssh_client.get_name()}: {e}")

    def __gather_mem_info(self, ssh_client, gather_path):
        try:
            tsar_cmd = f"tsar --mem -i 1 > {gather_path}/one_day_mem_data.txt"
            self._log_verbose(f"gather memory info on server {ssh_client.get_name()}, run cmd = [{tsar_cmd}]")
            ssh_client.exec_cmd(tsar_cmd)
        except Exception as e:
            self._log_error(f"Failed to gather memory info use tsar on server {ssh_client.get_name()}: {e}")

    def __gather_swap_info(self, ssh_client, gather_path):
        try:
            tsar_cmd = f"tsar  --swap --load > {gather_path}/tsar_swap_data.txt"
            self._log_verbose(f"gather swap info on server {ssh_client.get_name()}, run cmd = [{tsar_cmd}]")
            ssh_client.exec_cmd(tsar_cmd)
        except Exception as e:
            self._log_error(f"Failed to gather swap info use tsar on server {ssh_client.get_name()}: {e}")

    def __gather_io_info(self, ssh_client, gather_path):
        try:
            tsar_cmd = f"tsar --io > {gather_path}/tsar_io_data.txt"
            self._log_verbose(f"gather io info on server {ssh_client.get_name()}, run cmd = [{tsar_cmd}]")
            ssh_client.exec_cmd(tsar_cmd)
        except Exception as e:
            self._log_error(f"Failed to gather io info use tsar on server {ssh_client.get_name()}: {e}")

    def __gather_traffic_info(self, ssh_client, gather_path):
        try:
            tsar_cmd = f"tsar  --traffic > {gather_path}/tsar_traffic_data.txt"
            self._log_verbose(f"gather traffic info on server {ssh_client.get_name()}, run cmd = [{tsar_cmd}]")
            ssh_client.exec_cmd(tsar_cmd)
        except Exception as e:
            self._log_error(f"Failed to gather traffic info use tsar on server {ssh_client.get_name()}: {e}")

    def __gather_tcp_udp_info(self, ssh_client, gather_path):
        try:
            tsar_cmd = f"tsar  --tcp --udp -d 1 > {gather_path}/tsar_tcp_udp_data.txt"
            self._log_verbose(f"gather tcp and udp info on server {ssh_client.get_name()}, run cmd = [{tsar_cmd}]")
            ssh_client.exec_cmd(tsar_cmd)
        except Exception as e:
            self._log_error(f"Failed to gather tcp and udp info use tsar on server {ssh_client.get_name()}: {e}")

    def __get_overall_summary(self, node_summary_tuple):
        """
        Generate overall summary from gather tuples using BaseHandler template method.
        :param node_summary_tuple: List of tuples (node, is_err, error_msg, file_size, consume_time, pack_path)
        :return: Formatted summary table string
        """
        summary_tab = []
        field_names = ["Node", "Status", "Size", "Time", "PackPath"]
        for tup in node_summary_tuple:
            node = tup[0]
            is_err = tup[1]
            file_size = tup[3]
            consume_time = tup[4]
            pack_path = tup[5]
            try:
                format_file_size = FileUtil.size_format(num=file_size, output_str=True)
            except Exception as e:
                self._log_verbose("Failed to format file size {0}: {1}".format(file_size, e))
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        # Use BaseHandler template method
        return self._generate_summary_table(field_names, summary_tab, "Gather Sysstat Summary")
