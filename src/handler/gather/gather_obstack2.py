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
@time: 2020/7/19
@file: gather_obstack2.py
@desc:
"""
import os
import sys
import time
import datetime

# Removed tabulate import - now using BaseHandler._generate_summary_table

from src.common.base_handler import BaseHandler
from src.common.command import download_file, is_empty_dir, is_support_arch, get_observer_version, get_observer_pid, mkdir, get_file_size, delete_file_force, is_empty_file, upload_file
from src.common.constant import const
from src.common.ssh_client.ssh import SshClient
from src.common.tool import TimeUtils
from src.common.tool import Util
from src.common.tool import DirectoryUtil
from src.common.tool import FileUtil
from src.common.tool import StringUtils
from src.common.result_type import ObdiagResult


class GatherObstack2Handler(BaseHandler):
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

        # Use ConfigAccessor if available (file_number_limit is from BaseHandler @property)
        if self.config:
            self.file_size_limit = self.config.gather_file_size_limit
            self.config_path = self.config.basic_config_path
        else:
            # Fallback to direct config access
            if self.context.inner_config is None:
                self.file_size_limit = 2 * 1024 * 1024 * 1024
            else:
                basic_config = self.context.inner_config['obdiag']['basic']
                self.file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
                self.config_path = basic_config['config_path']

        # Initialize options
        store_dir_option = self._get_option('store_dir')
        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self._log_warn(f'args --store_dir [{os.path.abspath(store_dir_option)}] incorrect: No such directory, Now create it')
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_path = os.path.abspath(store_dir_option)

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
                resp = self.__handle_from_node(pack_dir_this_command, node)
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
            last_info = f"For result details, please run cmd \033[32m' cat {os.path.join(pack_dir_this_command, 'result_summary.txt')} '\033[0m\n"
            self._log_info(last_info)
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

        except Exception as e:
            return self._handle_error(e)

    def __handle_from_node(self, local_stored_path, node):
        resp = {"skip": False, "error": "", "gather_pack_path": ""}
        remote_ip = node.get("ip")
        remote_user = node.get("ssh_username")
        self._log_verbose(f"Sending Collect Shell Command to node {remote_ip} ...")
        DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        remote_dir_name = f"obstack2_{remote_ip.replace(':', '_')}_{now_time}"
        remote_dir_full_path = f"/tmp/{remote_dir_name}"
        ssh_failed = False
        ssh_client = None
        try:
            ssh_client = SshClient(self.context, node)
        except Exception as e:
            self.stdio.exception(f"ssh {remote_user}@{remote_ip}: failed, Please check the node conf.")
            resp["skip"] = True
            resp["error"] = "Please check the node conf."
            raise Exception("Please check the node conf.")

        if not is_support_arch(ssh_client):
            resp["error"] = f"remote server {ssh_client.get_name()} arch not support gather obstack"
            return resp
        mkdir(ssh_client, remote_dir_full_path)
        # install and chmod obstack2
        ob_version = get_observer_version(self.context)
        if not StringUtils.compare_versions_greater(ob_version, const.MIN_OB_VERSION_SUPPORT_GATHER_OBSTACK):
            self._log_verbose(f"This version {ob_version} does not support gather obstack . The minimum supported version is {const.MIN_OB_VERSION_SUPPORT_GATHER_OBSTACK}")
            resp["error"] = f"{ob_version} not support gather obstack"
            resp["gather_pack_path"] = f"{local_stored_path}"
            return resp
        is_need_install_obstack = self.__is_obstack_exists(ssh_client)
        if is_need_install_obstack:
            self._log_verbose(f"There is no obstack2 on the host {remote_ip}. It needs to be installed. Please wait a moment ...")
            if getattr(sys, 'frozen', False):
                absPath = os.path.dirname(sys.executable)
            else:
                absPath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
            # check node is x86_64 or aarch64
            node_arch = ssh_client.exec_cmd("arch").strip()
            if node_arch == "aarch64":
                obstack2_local_stored_full_path = os.path.join(absPath, const.OBSTACK2_LOCAL_STORED_PATH_AARCH64)
            elif node_arch == "x86_64":
                obstack2_local_stored_full_path = os.path.join(absPath, const.OBSTACK2_LOCAL_STORED_PATH_X86_64)
            else:
                self._log_warn(f"node:{ssh_client.get_name()} arch is {node_arch} not support gather obstack. obdiag will try use obstack_x86 to gather info")
                obstack2_local_stored_full_path = os.path.join(absPath, const.OBSTACK2_LOCAL_STORED_PATH_X86_64)
            # issue #133 . Check libtinfo.so.5 is existing
            self._log_verbose(f"Check node:{ssh_client.get_name()} libtinfo.so.5 is existing or not")
            libtinfo_info = ssh_client.exec_cmd("ldconfig -p | grep libtinfo.so.5")
            if not libtinfo_info:
                self._log_warn(f"node:{ssh_client.get_name()} libtinfo.so.5 not found, obstack need it")
                resp["error"] = f"node:{ssh_client.get_name()} libtinfo.so.5 not found, obstack need it"
                return resp
            upload_file(ssh_client, obstack2_local_stored_full_path, const.OBSTACK2_DEFAULT_INSTALL_PATH, self.context.stdio)
            self._log_verbose("Installation of obstack2 is completed and gather begins ...")

        self.__chmod_obstack2(ssh_client)
        # get observer_pid
        observer_pid_list = get_observer_pid(ssh_client, node.get("home_path"), self.stdio)
        # gather obstack2 info
        for observer_pid in observer_pid_list:
            user = self.__get_observer_execute_user(ssh_client, observer_pid)
            self.__gather_obstack2_info(ssh_client, user, observer_pid, remote_dir_name, node)
            try:
                self.stdio.start_loading('gather obstack info')
                self.is_ready(ssh_client, observer_pid, remote_dir_name)
                self.stdio.stop_loading('gather obstack info sucess')
            except Exception as e:
                self.stdio.stop_loading('gather info failed')
                self._log_error(f"Gather obstack info on the host {remote_ip} observer pid {observer_pid}: {e}")
                delete_file_force(ssh_client, f"/tmp/{remote_dir_name}/observer_{observer_pid}_obstack.txt", self.stdio)
                pass
        if is_empty_dir(ssh_client, f"/tmp/{remote_dir_name}", self.stdio):
            resp["error"] = "gather failed, folder is empty"
            return resp
        tar_cmd = f"cd /tmp && tar -czf {remote_dir_name}.tar.gz {remote_dir_name}/*"
        tar_cmd_request = ssh_client.exec_cmd(tar_cmd)
        self._log_verbose(f"tar request is {tar_cmd_request}")
        remote_tar_file_path = f"{remote_dir_full_path}.tar.gz"
        file_size = get_file_size(ssh_client, remote_tar_file_path, self.stdio)
        remote_tar_full_path = os.path.join("/tmp", remote_tar_file_path)
        if int(file_size) < self.file_size_limit:
            local_file_path = f"{local_stored_path}/{remote_dir_name}.tar.gz"
            download_file(ssh_client, remote_tar_full_path, local_file_path, self.stdio)
            resp["error"] = ""
        else:
            resp["error"] = "File too large"
        delete_file_force(ssh_client, remote_tar_full_path, self.stdio)
        resp["gather_pack_path"] = f"{local_stored_path}/{remote_dir_name}.tar.gz"
        return resp

    @Util.retry(10, 5)
    def is_ready(self, ssh_client, pid, remote_dir_name):
        try:
            self._log_verbose(f"Check whether the directory /tmp/{remote_dir_name} or file /tmp/{remote_dir_name}/observer_{pid}_obstack.txt is empty")
            is_empty_dir_res = is_empty_dir(ssh_client, f"/tmp/{remote_dir_name}", self.stdio)
            is_empty_file_res = is_empty_file(ssh_client, f"/tmp/{remote_dir_name}/observer_{pid}_obstack.txt", self.stdio)
            if is_empty_dir_res or is_empty_file_res:
                self._log_warn(f"The server {ssh_client.get_name()} directory /tmp/{remote_dir_name} or file /tmp/{remote_dir_name}/observer_{pid}_obstack.txt is empty, waiting for the collection to complete")
                raise
        except Exception as e:
            raise e

    def __chmod_obstack2(self, ssh_client):
        cmd = f"chmod a+x {const.OBSTACK2_DEFAULT_INSTALL_PATH}"
        ssh_client.exec_cmd(cmd)

    def __is_obstack_exists(self, ssh_client):
        cmd = f"test -e {const.OBSTACK2_DEFAULT_INSTALL_PATH} && echo exists"
        stdout = ssh_client.exec_cmd(cmd)
        if stdout == 'exists':
            return False
        else:
            return True

    def __get_observer_execute_user(self, ssh_client, pid):
        cmd = f"ps -o ruser=userForLongName -e -o pid,ppid,c,stime,tty,time,cmd | grep observer | grep {pid} | awk '{{print $1}}'"
        stdout = ssh_client.exec_cmd(cmd)
        user = stdout.splitlines()[0]
        self._log_verbose(f"get observer execute user, run cmd = [{cmd}], result:{user} ")
        return user

    def __gather_obstack2_info(self, ssh_client, user, observer_pid, remote_gather_dir, node):
        cmd = f"{const.OBSTACK2_DEFAULT_INSTALL_PATH} {observer_pid} > /tmp/{remote_gather_dir}/observer_{observer_pid}_obstack.txt"
        if user == ssh_client.exec_cmd('whoami'):
            self._log_verbose(f"gather obstack info on server {ssh_client.get_name()}, run cmd = [{cmd}]")
            ssh_client.exec_cmd(cmd, timeout=10)
        else:
            chown_cmd = f"chown {user} /tmp/{remote_gather_dir}/"
            ssh_client.exec_cmd(chown_cmd)
            self._log_verbose(f"gather obstack info on server {ssh_client.get_name()}, run cmd = [su {user}, {cmd}]")
            ssh_client.ssh_invoke_shell_switch_user(user, cmd, 10)

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
            is_err = tup[2]
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
        return self._generate_summary_table(field_names, summary_tab, "Gather Ob stack Summary")
