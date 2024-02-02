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
@time: 2020/7/14
@file: gather_sysstat.py
@desc:
"""
import os
import time
import datetime

import tabulate
import uuid

from common.logger import logger
from common.constant import const
from common.command import LocalClient, SshClient
from common.command import get_file_size, download_file, mkdir, zip_dir
from handler.base_shell_handler import BaseShellHandler
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file, parse_size
from utils.shell_utils import SshHelper
from utils.time_utils import timestamp_to_filename_time
from utils.utils import get_localhost_inner_ip, display_trace


class GatherOsInfoHandler(BaseShellHandler):
    def __init__(self, nodes, gather_pack_dir, gather_timestamp=None, common_config=None, is_scene=False):
        super(GatherOsInfoHandler, self).__init__(nodes)
        for node in nodes:
            if node.get("ssh_type") == "docker":
                logger.error("the ssh_type is docker not support sysstat")
                raise Exception("the ssh_type is docker not support sysstat")
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
        # check args first
        if not self.__check_valid_args(args):
            return

        if self.is_scene:
            pack_dir_this_command = self.local_stored_path
        else:
            pack_dir_this_command = os.path.join(self.local_stored_path,"gather_pack_{0}".format(timestamp_to_filename_time(self.gather_timestamp)))
        logger.info("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []

        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(args, node, pack_dir_this_command)
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

    def __handle_from_node(self, args, node, local_stored_path):
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
        remote_dir_name = "sysstat_{0}_{1}".format(remote_ip, now_time)
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
            mkdir(self.is_ssh, ssh_helper, remote_dir_full_path)

            self.__gather_dmesg_boot_info(ssh_helper, remote_dir_full_path)
            self.__gather_dmesg_current_info(ssh_helper, remote_dir_full_path)
            self.__gather_cpu_info(ssh_helper, remote_dir_full_path)
            self.__gather_mem_info(ssh_helper, remote_dir_full_path)
            zip_dir(self.is_ssh, ssh_helper, "/tmp", remote_dir_name)
            remote_file_full_path = "{0}.zip".format(remote_dir_full_path)
            file_size = get_file_size(self.is_ssh, ssh_helper, remote_file_full_path)
            if int(file_size) < self.file_size_limit:
                local_file_path = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
                logger.info(
                    "local file path {0}...".format(local_file_path))
                download_file(self.is_ssh, ssh_helper, remote_file_full_path, local_file_path)
                resp["error"] = ""
            else:
                resp["error"] = "File too large"
            # delete_file_force(self.is_ssh, ssh_helper, remote_file_full_path)
            ssh_helper.ssh_close()
            resp["gather_pack_path"] = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
        return resp

    def __gather_dmesg_current_info(self, ssh_helper, gather_path):
        try:
            dmesg_cmd = "dmesg --ctime > {gather_path}/dmesg.human.current".format(
                gather_path=gather_path)
            logger.info("gather dmesg current info, run cmd = [{0}]".format(dmesg_cmd))
            SshClient().run(ssh_helper, dmesg_cmd) if self.is_ssh else LocalClient().run(dmesg_cmd)
        except:
            logger.error("Failed to gather dmesg current info on server {0}".format(ssh_helper.get_name()))

    def __gather_dmesg_boot_info(self, ssh_helper, dir_path):
        try:
            file_exit_cmd = "ls -l {file_path} 2>/dev/null".format(file_path="/var/log/dmesg")
            file_exit = SshClient().run(ssh_helper, file_exit_cmd) if self.is_ssh else LocalClient().run(file_exit_cmd)
            if file_exit:
                dmesg_cmd = 'cp --force /var/log/dmesg {dir_path}/dmesg.boot'.format(dir_path=dir_path)
                logger.info("gather dmesg boot info on server {0}, run cmd = [{1}]".format(ssh_helper.get_name(), dmesg_cmd))
                SshClient().run(ssh_helper, dmesg_cmd) if self.is_ssh else LocalClient().run(dmesg_cmd)
            else:
                logger.warn("the file /var/log/dmesg on server {0} not found ".format(ssh_helper.get_name()))
        except:
            logger.error("Failed to gather the /var/log/dmesg on server {0}".format(ssh_helper.get_name()))

    def __gather_cpu_info(self, ssh_helper, gather_path):
        try:
            tsar_cmd = "tsar --cpu -i 1 > {gather_path}/one_day_cpu_data.txt".format(
                gather_path=gather_path)
            logger.info("gather cpu info on server {0}, run cmd = [{1}]".format(ssh_helper.get_name(), tsar_cmd))
            SshClient().run(ssh_helper, tsar_cmd) if self.is_ssh else LocalClient().run(tsar_cmd)
        except:
            logger.error("Failed to gather cpu info use tsar on server {0}".format(ssh_helper.get_name()))

    def __gather_mem_info(self, ssh_helper, gather_path):
        try:
            tsar_cmd = "tsar --mem -i 1 > {gather_path}/one_day_mem_data.txt".format(
                gather_path=gather_path)
            logger.info("gather memory info on server {0}, run cmd = [{1}]".format(ssh_helper.get_name(), tsar_cmd))
            SshClient().run(ssh_helper, tsar_cmd) if self.is_ssh else LocalClient().run(tsar_cmd)
        except:
            logger.error("Failed to gather memory info use tsar on server {0}".format(ssh_helper.get_name()))


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
            is_err = tup[1]
            file_size = tup[3]
            consume_time = tup[4]
            pack_path = tup[5]
            try:
                format_file_size = size_format(file_size, output_str=True)
            except:
                format_file_size = size_format(0, output_str=True)
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed",
                                format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather Sysstat Summary:\n" + \
               tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
