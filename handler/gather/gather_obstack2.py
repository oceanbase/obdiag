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
@time: 2020/7/19
@file: gather_obstack2.py
@desc:
"""
import os
import sys
import time
import datetime

import tabulate

from common.command import download_file, is_empty_dir, is_support_arch, get_observer_version, get_observer_pid, mkdir, zip_dir, get_file_size, delete_file_force, is_empty_file, upload_file
from common.constant import const
from common.command import LocalClient, SshClient
from handler.base_shell_handler import BaseShellHandler
from common.tool import Util
from common.ssh import SshHelper
from common.tool import TimeUtils
from common.tool import Util
from common.tool import DirectoryUtil
from common.tool import FileUtil
from common.tool import NetUtils
from common.tool import StringUtils


class GatherObstack2Handler(BaseShellHandler):
    def __init__(self, context, gather_pack_dir='./', is_scene=False):
        super(GatherObstack2Handler, self).__init__()
        self.context = context
        self.stdio = context.stdio
        self.is_ssh = True
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        self.is_scene = is_scene
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
        store_dir_option = Util.get_option(options, 'store_dir')
        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('warn: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_path = os.path.abspath(store_dir_option)
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        if self.is_scene:
            pack_dir_this_command = self.local_stored_path
        else:
            pack_dir_this_command = os.path.join(self.local_stored_path, "gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
        self.stdio.verbose("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []

        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(pack_dir_this_command, node)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            gather_tuples.append((node.get("ip"), False, resp["error"], file_size, int(time.time() - st), resp["gather_pack_path"]))

        if self.is_ssh:
            for node in self.nodes:
                handle_from_node(node)
        else:
            local_ip = NetUtils.get_inner_ip()
            node = self.nodes[0]
            node["ip"] = local_ip
            for node in self.nodes:
                handle_from_node(node)

        summary_tuples = self.__get_overall_summary(gather_tuples)
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
        remote_dir_name = "obstack2_{0}_{1}".format(remote_ip, now_time)
        remote_dir_full_path = "/tmp/{0}".format(remote_dir_name)
        ssh_failed = False
        try:
            ssh_helper = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key, node, self.stdio)
        except Exception as e:
            self.stdio.exception("ssh {0}@{1}: failed, Please check the {2}".format(remote_user, remote_ip, self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)

        if not ssh_failed:
            if not is_support_arch(self.is_ssh, ssh_helper, self.stdio):
                resp["error"] = "remote server {0} arch not support gather obstack".format(ssh_helper.get_name())
                return resp
            mkdir(self.is_ssh, ssh_helper, remote_dir_full_path, self.stdio)

            # install and chmod obstack2
            ob_version = get_observer_version(self.is_ssh, ssh_helper, node.get("home_path"), self.stdio)
            if not StringUtils.compare_versions_greater(ob_version, const.MIN_OB_VERSION_SUPPORT_GATHER_OBSTACK):
                self.stdio.verbose("This version {0} does not support gather obstack . The minimum supported version is {1}".format(ob_version, const.MIN_OB_VERSION_SUPPORT_GATHER_OBSTACK))
                resp["error"] = "{0} not support gather obstack".format(ob_version)
                resp["gather_pack_path"] = "{0}".format(local_stored_path)
                return resp
            is_need_install_obstack = self.__is_obstack_exists(self.is_ssh, ssh_helper)
            if is_need_install_obstack:
                self.stdio.verbose("There is no obstack2 on the host {0}. It needs to be installed. " "Please wait a moment ...".format(remote_ip))
                if getattr(sys, 'frozen', False):
                    absPath = os.path.dirname(sys.executable)
                else:
                    absPath = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                obstack2_local_stored_full_path = os.path.join(absPath, const.OBSTACK2_LOCAL_STORED_PATH)
                upload_file(self.is_ssh, ssh_helper, obstack2_local_stored_full_path, const.OBSTACK2_DEFAULT_INSTALL_PATH, self.stdio)
                self.stdio.verbose("Installation of obstack2 is completed and gather begins ...")

            self.__chmod_obstack2(self.is_ssh, ssh_helper)
            # get observer_pid
            observer_pid_list = get_observer_pid(self.is_ssh, ssh_helper, node.get("home_path"), self.stdio)
            # gather obstack2 info
            for observer_pid in observer_pid_list:
                user = self.__get_observer_execute_user(ssh_helper, observer_pid)
                self.__gather_obstack2_info(self.is_ssh, ssh_helper, user, observer_pid, remote_dir_name, node)
                try:
                    self.stdio.start_loading('gather obstack info')
                    self.is_ready(ssh_helper, observer_pid, remote_dir_name)
                    self.stdio.stop_loading('gather obstack info sucess')
                except:
                    self.stdio.stop_loading('gather info failed')
                    self.stdio.error("Gather obstack info on the host {0} observer pid {1}".format(remote_ip, observer_pid))
                    delete_file_force(self.is_ssh, ssh_helper, "/tmp/{dir_name}/observer_{pid}_obstack.txt".format(dir_name=remote_dir_name, pid=observer_pid), self.stdio)
                    pass
            if is_empty_dir(self.is_ssh, ssh_helper, "/tmp/{0}".format(remote_dir_name), self.stdio):
                resp["error"] = "gather failed, folder is empty"
                return resp

            zip_dir(self.is_ssh, ssh_helper, "/tmp", remote_dir_name, self.stdio)
            remote_zip_file_path = "{0}.zip".format(remote_dir_full_path)

            file_size = get_file_size(self.is_ssh, ssh_helper, remote_zip_file_path, self.stdio)
            remote_file_full_path = "{0}.zip".format(remote_dir_full_path)
            if int(file_size) < self.file_size_limit:
                local_file_path = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
                download_file(self.is_ssh, ssh_helper, remote_file_full_path, local_file_path, self.stdio)
                resp["error"] = ""
            else:
                resp["error"] = "File too large"
            delete_file_force(self.is_ssh, ssh_helper, remote_file_full_path, self.stdio)
            ssh_helper.ssh_close()
            resp["gather_pack_path"] = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
        return resp

    @Util.retry(5, 2)
    def is_ready(self, ssh_helper, pid, remote_dir_name):
        try:
            self.stdio.verbose("Check whether the directory /tmp/{dir_name} or " "file /tmp/{dir_name}/observer_{pid}_obstack.txt is empty".format(dir_name=remote_dir_name, pid=pid))
            is_empty_dir_res = is_empty_dir(self.is_ssh, ssh_helper, "/tmp/{0}".format(remote_dir_name), self.stdio)
            is_empty_file_res = is_empty_file(self.is_ssh, ssh_helper, "/tmp/{dir_name}/observer_{pid}_obstack.txt".format(dir_name=remote_dir_name, pid=pid), self.stdio)
            if is_empty_dir_res or is_empty_file_res:
                self.stdio.verbose(
                    "The server {host_ip} directory /tmp/{dir_name} or file /tmp/{dir_name}/observer_{pid}_obstack.txt"
                    " is empty, waiting for the collection to complete".format(host_ip=ssh_helper.get_name() if self.is_ssh else NetUtils.get_inner_ip(self.stdio), dir_name=remote_dir_name, pid=pid)
                )
                raise
        except Exception as e:
            raise e

    def __chmod_obstack2(self, is_ssh, ssh_helper):
        cmd = "chmod a+x {file}".format(file=const.OBSTACK2_DEFAULT_INSTALL_PATH)
        SshClient(self.stdio).run(ssh_helper, cmd) if is_ssh else LocalClient(self.stdio).run(cmd)

    def __is_obstack_exists(self, is_ssh, ssh_helper):
        cmd = "test -e {file} && echo exists".format(file=const.OBSTACK2_DEFAULT_INSTALL_PATH)
        stdout = SshClient(self.stdio).run(ssh_helper, cmd) if is_ssh else LocalClient(self.stdio).run(cmd)
        if stdout == 'exists':
            return False
        else:
            return True

    def __get_observer_execute_user(self, ssh_helper, pid):
        cmd = "ps -o ruser=userForLongName -e -o pid,ppid,c,stime,tty,time,cmd | grep observer | grep {0} | awk {1}".format(pid, "'{print $1}'")
        stdout = SshClient(self.stdio).run(ssh_helper, cmd) if self.is_ssh else LocalClient(self.stdio).run(cmd)
        user = stdout.splitlines()[0]
        self.stdio.verbose("get observer execute user, run cmd = [{0}], result:{1} ".format(cmd, user))
        return user

    def __gather_obstack2_info(self, is_ssh, ssh_helper, user, observer_pid, remote_gather_dir, node):
        cmd = "{obstack} {pid} > /tmp/{gather_dir}/observer_{pid}_obstack.txt".format(obstack=const.OBSTACK2_DEFAULT_INSTALL_PATH, pid=observer_pid, gather_dir=remote_gather_dir)
        if is_ssh:
            if user == ssh_helper.username:
                self.stdio.verbose("gather obstack info on server {0}, run cmd = [{1}]".format(ssh_helper.get_name(), cmd))
                SshClient(self.stdio).run_ignore_err(ssh_helper, cmd)
            else:
                ssh_helper_new = SshHelper(ssh_helper.host_ip, ssh_helper.username, ssh_helper.password, ssh_helper.ssh_port, ssh_helper.key_file, node)
                chown_cmd = "chown {user} /tmp/{gather_dir}/".format(user=user, gather_dir=remote_gather_dir)
                SshClient(self.stdio).run(ssh_helper_new, chown_cmd)
                self.stdio.verbose("gather obstack info on server {0}, run cmd = [su {1}, {2}]".format(ssh_helper.get_name(), user, cmd))
                ssh_helper_new.ssh_invoke_shell_switch_user(user, cmd, 10)
        else:
            LocalClient(self.stdio).run(cmd)

    @staticmethod
    def __get_overall_summary(node_summary_tuple):
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
            except:
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather Ob stack Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
