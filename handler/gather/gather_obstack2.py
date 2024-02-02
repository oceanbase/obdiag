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
import re
import sys
import threading
import time
import datetime

import tabulate
import uuid

from common.command import download_file, is_empty_dir, is_support_arch, get_observer_version, get_observer_pid, mkdir, zip_dir, get_file_size, delete_file_force, is_empty_file, upload_file
from common.logger import logger
from common.obdiag_exception import OBDIAGInvalidArgs
from common.constant import const
from common.command import LocalClient, SshClient
from handler.base_shell_handler import BaseShellHandler
from utils.version_utils import compare_versions_greater
from utils.retry_utils import retry
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file, parse_size
from utils.shell_utils import SshHelper
from utils.time_utils import timestamp_to_filename_time
from utils.utils import get_localhost_inner_ip, display_trace


class GatherObstack2Handler(BaseShellHandler):
    def __init__(self, nodes, gather_pack_dir, gather_timestamp=None, common_config=None, is_scene=False):
        super(GatherObstack2Handler, self).__init__(nodes)
        self.is_ssh = True
        self.gather_timestamp = gather_timestamp
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        self.is_scene = is_scene
        self.config_path = const.DEFAULT_CONFIG_PATH
        if common_config is None:
            self.file_size_limit = 2 * 1024 * 1024 * 1024
        else:
            self.file_size_limit = int(parse_size(common_config["file_size_limit"]))

    def handle(self, args):
        if not self.__check_valid_args(args):
            return
        if self.is_scene:
            pack_dir_this_command = self.local_stored_path
        else:
            pack_dir_this_command = os.path.join(self.local_stored_path, "gather_pack_{0}".format(timestamp_to_filename_time(self.gather_timestamp)))
        logger.info("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []
        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(args, pack_dir_this_command, node)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            gather_tuples.append((node.get("ip"), False, resp["error"],
                                  file_size,
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

        summary_tuples = self.__get_overall_summary(gather_tuples)
        print(summary_tuples)
        display_trace(uuid.uuid3(uuid.NAMESPACE_DNS, str(os.getpid())))
        # Persist the summary results to a file
        write_result_append_to_file(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)
        last_info = "For result details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(pack_dir_this_command, "result_summary.txt"))
        print(last_info)

    def __handle_from_node(self, args, local_stored_path, node):
        resp = {
            "skip": False,
            "error": "",
            "gather_pack_path": ""
        }
        remote_ip = node.get("ip") if self.is_ssh else get_localhost_inner_ip()
        remote_user = node.get("user")
        remote_password = node.get("password")
        remote_port = node.get("port")
        remote_private_key = node.get("private_key")
        remote_home_path = node.get("home_path")
        logger.info(
            "Sending Collect Shell Command to node {0} ...".format(remote_ip))
        mkdir_if_not_exist(local_stored_path)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        remote_dir_name = "obstack2_{0}_{1}".format(remote_ip, now_time)
        remote_dir_full_path = "/tmp/{0}".format(remote_dir_name)
        ssh_failed = False
        try:
            ssh_helper = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key,node)
        except Exception as e:
            logger.error("ssh {0}@{1}: failed, Please check the {2}".format(
                remote_user, 
                remote_ip, 
                self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)

        if not ssh_failed:
            if not is_support_arch(self.is_ssh, ssh_helper):
                resp["error"] = "remote server {0} arch not support gather obstack".format(ssh_helper.get_name())
                return resp
            mkdir(self.is_ssh, ssh_helper, remote_dir_full_path)

            # install and chmod obstack2
            ob_version = get_observer_version(self.is_ssh, ssh_helper, node.get("home_path"))
            if not compare_versions_greater(ob_version, const.MIN_OB_VERSION_SUPPORT_GATHER_OBSTACK):
                logger.info("This version {0} does not support gather obstack . The minimum supported version is {1}".
                            format(ob_version, const.MIN_OB_VERSION_SUPPORT_GATHER_OBSTACK))
                resp["error"] = "{0} not support gather obstack".format(ob_version)
                resp["gather_pack_path"] = "{0}".format(local_stored_path)
                return resp
            is_need_install_obstack = self.__is_obstack_exists(self.is_ssh, ssh_helper)
            if is_need_install_obstack:
                logger.info("There is no obstack2 on the host {0}. It needs to be installed. "
                            "Please wait a moment ...".format(remote_ip))
                if getattr(sys, 'frozen', False):
                    absPath = os.path.dirname(sys.executable)
                else:
                    absPath = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
                obstack2_local_stored_full_path = os.path.join(absPath, const.OBSTACK2_LOCAL_STORED_PATH)
                upload_file(self.is_ssh, ssh_helper, obstack2_local_stored_full_path, const.OBSTACK2_DEFAULT_INSTALL_PATH)
                logger.info("Installation of obstack2 is completed and gather begins ...")
                    
            self.__chmod_obstack2(self.is_ssh, ssh_helper)
            # get observer_pid
            observer_pid_list = get_observer_pid(self.is_ssh, ssh_helper, node.get("home_path"))
            # gather obstack2 info
            for observer_pid in observer_pid_list:
                user = self.__get_observer_execute_user(ssh_helper, observer_pid)
                self.__gather_obstack2_info(self.is_ssh, ssh_helper, user, observer_pid, remote_dir_name,node)
                try:
                    self.is_ready(ssh_helper, observer_pid, remote_dir_name)
                except:
                    logger.error("Gather obstack info on the host {0} observer pid {1}".format(remote_ip, observer_pid))
                    delete_file_force(self.is_ssh,  ssh_helper, "/tmp/{dir_name}/observer_{pid}_obstack.txt"
                                                .format(dir_name=remote_dir_name, pid=observer_pid))
                    pass
            if is_empty_dir(self.is_ssh, ssh_helper, "/tmp/{0}".format(remote_dir_name)):
                resp["error"] = "gather failed, folder is empty"
                return resp
            
            zip_dir(self.is_ssh, ssh_helper, "/tmp", remote_dir_name)
            remote_zip_file_path = "{0}.zip".format(remote_dir_full_path)

            file_size = get_file_size(self.is_ssh, ssh_helper, remote_zip_file_path)
            remote_file_full_path = "{0}.zip".format(remote_dir_full_path)
            if int(file_size) < self.file_size_limit:
                local_file_path = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
                download_file(self.is_ssh, ssh_helper, remote_file_full_path, local_file_path)
                resp["error"] = ""
            else:
                resp["error"] = "File too large"
            delete_file_force(self.is_ssh,  ssh_helper, remote_file_full_path)
            ssh_helper.ssh_close()
            resp["gather_pack_path"] = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
        return resp

    @retry(5, 2)
    def is_ready(self, ssh_helper, pid, remote_dir_name):
        try:
            logger.info("Check whether the directory /tmp/{dir_name} or "
                        "file /tmp/{dir_name}/observer_{pid}_obstack.txt is empty"
                        .format(dir_name=remote_dir_name, pid=pid))
            is_empty_dir_res = is_empty_dir(self.is_ssh, ssh_helper, "/tmp/{0}".format(remote_dir_name))
            is_empty_file_res = is_empty_file(self.is_ssh, ssh_helper, "/tmp/{dir_name}/observer_{pid}_obstack.txt"
                                                     .format(dir_name=remote_dir_name, pid=pid))
            if is_empty_dir_res or is_empty_file_res:
                logger.info(
                    "The server {host_ip} directory /tmp/{dir_name} or file /tmp/{dir_name}/observer_{pid}_obstack.txt"
                    " is empty, waiting for the collection to complete"
                        .format(host_ip=ssh_helper.get_name() if self.is_ssh else get_localhost_inner_ip(), dir_name=remote_dir_name, pid=pid))
                raise
        except Exception as e:
            raise e

    @staticmethod
    def __chmod_obstack2(is_ssh, ssh_helper):
        cmd = "chmod a+x {file}".format(file=const.OBSTACK2_DEFAULT_INSTALL_PATH)
        SshClient().run(ssh_helper, cmd) if is_ssh else LocalClient().run(cmd)

    @staticmethod
    def __is_obstack_exists(is_ssh, ssh_helper):
        cmd = "test -e {file} && echo exists".format(file=const.OBSTACK2_DEFAULT_INSTALL_PATH)
        stdout =  SshClient().run(ssh_helper, cmd) if is_ssh else LocalClient().run(cmd)
        if stdout == 'exists':
            return False
        else:
            return True

    def __get_observer_execute_user(self, ssh_helper, pid):
        cmd = "ps -o ruser=userForLongName -e -o pid,ppid,c,stime,tty,time,cmd | grep observer | grep {0} | awk {1}".format(pid, "'{print $1}'")
        stdout =  SshClient().run(ssh_helper, cmd) if self.is_ssh else LocalClient().run(cmd)
        user = stdout.splitlines()[0]
        logger.info("get observer execute user, run cmd = [{0}], result:{1} ".format(cmd, user))
        return user

    @staticmethod
    def __gather_obstack2_info(is_ssh, ssh_helper, user, observer_pid, remote_gather_dir,node):
        cmd = "{obstack} {pid} > /tmp/{gather_dir}/observer_{pid}_obstack.txt".format(
            obstack=const.OBSTACK2_DEFAULT_INSTALL_PATH,
            pid=observer_pid,
            gather_dir=remote_gather_dir)
        if is_ssh:
            if user == ssh_helper.username:
                logger.debug("gather obstack info on server {0}, run cmd = [{1}]".format(ssh_helper.get_name(), cmd))
                SshClient().run_ignore_err(ssh_helper, cmd)
            else:
                ssh_helper_new = SshHelper(ssh_helper.host_ip, ssh_helper.username, ssh_helper.password, ssh_helper.ssh_port, ssh_helper.key_file,node)
                chown_cmd = "chown {user} /tmp/{gather_dir}/".format(user=user,gather_dir=remote_gather_dir)
                SshClient().run(ssh_helper_new, chown_cmd)
                logger.info("gather obstack info on server {0}, run cmd = [su {1}, {2}]".format(ssh_helper.get_name(), user, cmd))
                ssh_helper_new.ssh_invoke_shell_switch_user(user, cmd, 10)
        else:
            LocalClient().run(cmd)

    def __check_valid_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        # 1: store_dir must exist, else create directory.
        if getattr(args, "store_dir") is not None:
            if not os.path.exists(os.path.abspath(getattr(args, "store_dir"))):
                logger.warn("Error: args --store_dir [{0}] incorrect: No such directory, Now create it".format(os.path.abspath(getattr(args, "store_dir"))))
                os.makedirs(os.path.abspath(getattr(args, "store_dir")))
            self.local_stored_path = os.path.abspath(getattr(args, "store_dir"))
        return True

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
                format_file_size = size_format(file_size, output_str=True)
            except:
                format_file_size = size_format(0, output_str=True)
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed",
                                format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather Ob stack Summary:\n" + \
               tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
