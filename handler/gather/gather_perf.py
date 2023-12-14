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
@time: 2023/01/12
@file: gather_perf.py
@desc:
"""
import os
import threading
import time
import datetime

import tabulate
import uuid

from common.command import get_observer_pid, mkdir, zip_dir, get_file_size, download_file, delete_file_force
from common.logger import logger
from common.command import LocalClient, SshClient
from common.obdiag_exception import OBDIAGInvalidArgs
from common.constant import const
from handler.base_shell_handler import BaseShellHandler
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file, parse_size
from utils.shell_utils import SshHelper
from utils.time_utils import timestamp_to_filename_time
from utils.utils import get_localhost_inner_ip, display_trace


class GatherPerfHandler(BaseShellHandler):
    def __init__(self, nodes, gather_pack_dir, gather_timestamp, common_config):
        super(GatherPerfHandler, self).__init__(nodes)
        self.is_ssh = True
        self.gather_timestamp = gather_timestamp
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        self.ob_install_dir = None
        self.scope = "all"
        self.config_path = const.DEFAULT_CONFIG_PATH
        if common_config is None:
            self.file_size_limit = 2 * 1024 * 1024
        else:
            self.file_size_limit = int(parse_size(common_config["file_size_limit"]))

    def handle(self, args):
        if not self.__check_valid_args(args):
            return
        if args.store_dir is not None:
            self.local_stored_path = os.path.abspath(args.store_dir)
        pack_dir_this_command = os.path.join(self.local_stored_path,"gather_pack_{0}".format(timestamp_to_filename_time(self.gather_timestamp)))
        logger.info("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []

        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(node, pack_dir_this_command)
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

    def __handle_from_node(self, node, local_stored_path):
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
        remote_dir_name = "perf_{0}_{1}".format(node.get("ip"), now_time)
        remote_dir_full_path = "/tmp/{0}".format(remote_dir_name)
        ssh_failed = False
        try:
            ssh_helper = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port,
                                   remote_private_key, node)
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

            pid_observer_list = get_observer_pid(self.is_ssh, ssh_helper, node.get("home_path"))
            if len(pid_observer_list) == 0:
                resp["error"] = "can't find observer"
                return resp
            for pid_observer in pid_observer_list:
                if self.scope == "sample":
                    self.__gather_perf_sample(ssh_helper, remote_dir_full_path, pid_observer)
                elif self.scope == "flame":
                    self.__gather_perf_flame(ssh_helper, remote_dir_full_path, pid_observer)
                elif self.scope == "pstack":
                    self.__gather_pstack(ssh_helper, remote_dir_full_path, pid_observer)
                else:
                    self.__gather_perf_sample(ssh_helper, remote_dir_full_path, pid_observer)
                    self.__gather_perf_flame(ssh_helper, remote_dir_full_path, pid_observer)
                    self.__gather_pstack(ssh_helper, remote_dir_full_path, pid_observer)

            zip_dir(self.is_ssh, ssh_helper, "/tmp", remote_dir_name)
            remote_file_full_path = "{0}.zip".format(remote_dir_full_path)
            file_size = get_file_size(self.is_ssh, ssh_helper, remote_file_full_path)
            if int(file_size) < self.file_size_limit:
                local_file_path = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
                download_file(self.is_ssh,ssh_helper, remote_file_full_path, local_file_path)
                resp["error"] = ""
            else:
                resp["error"] = "File too large"
            delete_file_force(self.is_ssh, ssh_helper, remote_file_full_path)
            ssh_helper.ssh_close()
            resp["gather_pack_path"] = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
        return resp

    def __gather_perf_sample(self, ssh_helper, gather_path, pid_observer):
        try:
            cmd = "cd {gather_path} && perf record -o sample.data -e cycles -c 100000000 -p {pid} -g -- sleep 20".format(
            gather_path=gather_path, pid=pid_observer)
            logger.info("gather perf sample, run cmd = [{0}]".format(cmd))
            SshClient().run_ignore_err(ssh_helper, cmd) if self.is_ssh else LocalClient().run(cmd)

            generate_data = "cd {gather_path} && perf script -i sample.data -F ip,sym -f > sample.viz".format(
            gather_path=gather_path)
            logger.info("generate perf sample data, run cmd = [{0}]".format(generate_data))
            SshClient().run_ignore_err(ssh_helper, generate_data) if self.is_ssh else LocalClient().run(generate_data)
        except:
            logger.error("generate perf sample data on server [{0}] failed".format(ssh_helper.get_name()))

    def __gather_perf_flame(self, ssh_helper, gather_path, pid_observer):
        try:
            perf_cmd = "cd {gather_path} && perf record -o flame.data -F 99 -p {pid} -g -- sleep 20".format(
            gather_path=gather_path, pid=pid_observer)
            logger.info("gather perf, run cmd = [{0}]".format(perf_cmd))
            SshClient().run_ignore_err(ssh_helper, perf_cmd) if self.is_ssh else LocalClient().run(perf_cmd)

            generate_data = "cd {gather_path} && perf script -i flame.data > flame.viz".format(
            gather_path=gather_path)
            logger.info("generate perf data, run cmd = [{0}]".format(generate_data))
            SshClient().run_ignore_err(ssh_helper, generate_data) if self.is_ssh else LocalClient().run(generate_data)
        except:
            logger.error("generate perf data on server [{0}] failed".format(ssh_helper.get_name()))

    def __gather_pstack(self, ssh_helper, gather_path, pid_observer):
        try:
            pstack_cmd = "cd {gather_path} && pstack {pid} > pstack.viz".format(
            gather_path=gather_path, pid=pid_observer)
            logger.info("gather pstack, run cmd = [{0}]".format(pstack_cmd))
            SshClient().run(ssh_helper, pstack_cmd) if self.is_ssh else LocalClient().run(pstack_cmd)
        except:
            logger.error("gather pstack on server failed [{0}]".format(ssh_helper.get_name()))

    def __check_valid_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        # 1: store_dir must exist, else return "No such file or directory".
        if getattr(args, "store_dir") is not None:
            if not os.path.exists(os.path.abspath(getattr(args, "store_dir"))):
                logger.error("Error: args --store_dir [{0}] incorrect: No such directory."
                             .format(os.path.abspath(getattr(args, "store_dir"))))
                return False
        if getattr(args, "scope") is not None:
            self.scope = getattr(args, "scope")[0]
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
        return "\nGather Perf Summary:\n" + \
               tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
