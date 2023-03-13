#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2020/7/14
@file: gather_sysstat_handler
@desc:
"""
import os
import threading
import time
import datetime

import tabulate

from common.logger import logger
from common.obdiag_exception import OBDIAGInvalidArgs
from common.constant import const
from handler.base_shell_handler import BaseShellHandler
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file, parse_size
from utils.shell_utils import SshHelper
from utils.time_utils import timestamp_to_filename_time


class GatherOsInfoHandler(BaseShellHandler):
    def __init__(self, nodes, gather_pack_dir, gather_timestamp, common_config):
        super(GatherOsInfoHandler, self).__init__(nodes)
        self.gather_timestamp = gather_timestamp
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        if common_config is None:
            self.file_size_limit = 2 * 1024 * 1024
        else:
            self.file_size_limit = int(parse_size(common_config["file_size_limit"]))

    def handle(self, args):
        # check args first
        if not self.__check_valid_args(args):
            raise OBDIAGInvalidArgs("Invalid args, args={0}".format(args))

        # if user indicates the store_dir, use it, otherwise use the dir in the config(default)
        if args.store_dir is not None:
            self.local_stored_path = os.path.abspath(args.store_dir)

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
                                  int(time.time() - st),
                                  resp["gather_pack_path"]))

        node_threads = [threading.Thread(None, handle_from_node, args=(
            node["ip"],
            node["user"],
            node["password"],
            node["port"],
            node["private_key"]))
                        for node in self.nodes]
        list(map(lambda x: x.start(), node_threads))
        list(map(lambda x: x.join(timeout=const.GATHER_THREAD_TIMEOUT), node_threads))

        summary_tuples = self.__get_overall_summary(gather_tuples)
        print(summary_tuples)
        # Persist the summary results to a file
        write_result_append_to_file(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)

    def __handle_from_node(self, args, ip, user, password, port, private_key, local_stored_path):
        resp = {
            "skip": False,
            "error": "",
            "gather_pack_path": ""
        }
        remote_ip = ip
        remote_user = user
        remote_password = password
        remote_port = port
        remote_private_key = private_key
        logger.info(
            "Sending Collect Shell Command to node {0} ...".format(remote_ip))
        mkdir_if_not_exist(local_stored_path)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        remote_dir_name = "sysstat_{0}_{1}".format(ip, now_time)
        remote_dir_full_path = "/tmp/{0}".format(remote_dir_name)
        ssh_helper = SshHelper(remote_ip, remote_user, remote_password, remote_port, remote_private_key)
        ssh_helper.ssh_mkdir_if_not_exist(remote_dir_full_path)

        self.__gather_dmesg_boot_info(ssh_helper, remote_dir_full_path)
        self.__gather_dmesg_current_info(ssh_helper, remote_dir_full_path)
        self.__gather_cpu_info(ssh_helper, remote_dir_full_path)
        self.__gather_mem_info(ssh_helper, remote_dir_full_path)

        ssh_helper.zip_rm_dir("/tmp", remote_dir_name)
        remote_file_full_path = "{0}.zip".format(remote_dir_full_path)
        file_size = ssh_helper.get_file_size(remote_file_full_path)
        if int(file_size) < self.file_size_limit:
            local_file_path = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
            ssh_helper.download(remote_file_full_path, local_file_path)
            resp["error"] = ""
        else:
            resp["error"] = "File too large"
        ssh_helper.delete_file_force(remote_file_full_path)
        ssh_helper.ssh_close()
        resp["gather_pack_path"] = "{0}/{1}.zip".format(local_stored_path, remote_dir_name)
        return resp

    def __gather_dmesg_current_info(self, ssh_helper, gather_path):
        try:
            dmesg_cmd = "dmesg --ctime > {gather_path}/dmesg.human.current".format(
                gather_path=gather_path)
            logger.info("gather dmesg current info, run cmd = [{0}]".format(dmesg_cmd))
            ssh_helper.ssh_exec_cmd(dmesg_cmd)
        except:
            logger.error("Failed to gather dmesg current info on server {0}".format(ssh_helper.host_ip))

    def __gather_dmesg_boot_info(self, ssh_helper, dir_path):
        try:
            dmesg_cmd = 'cp --force /var/log/dmesg {dir_path}/dmesg.boot'.format(dir_path=dir_path)
            logger.info("gather dmesg boot info on server {0}, run cmd = [{1}]".format(ssh_helper.host_ip, dmesg_cmd))
            ssh_helper.ssh_exec_cmd(dmesg_cmd)
        except:
            logger.error("Failed to gather the /var/log/dmesg on server {0}".format(ssh_helper.host_ip))

    @staticmethod
    def __gather_cpu_info(ssh_helper, gather_path):
        try:
            tsar_cmd = "tsar --cpu -i 1 > {gather_path}/one_day_cpu_data.txt".format(
                gather_path=gather_path)
            logger.info("gather cpu info on server {0}, run cmd = [{1}]".format(ssh_helper.host_ip, tsar_cmd))
            ssh_helper.ssh_exec_cmd(tsar_cmd)
        except:
            logger.error("Failed to gather cpu info use tsar on server {0}".format(ssh_helper.host_ip))

    @staticmethod
    def __gather_mem_info(ssh_helper, gather_path):
        try:
            tsar_cmd = "tsar --mem -i 1 > {gather_path}/one_day_mem_data.txt".format(
                gather_path=gather_path)
            logger.info("gather memory info on server {0}, run cmd = [{1}]".format(ssh_helper.host_ip, tsar_cmd))
            ssh_helper.ssh_exec_cmd(tsar_cmd)
        except:
            logger.error("Failed to gather memory info use tsar on server {0}".format(ssh_helper.host_ip))

    @staticmethod
    def __check_valid_args(args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        # 1: store_dir must exist, else return "No such file or directory".
        if args.store_dir is not None and not os.path.exists(os.path.abspath(args.store_dir)):
            logger.error("Error: Set store dir {0} failed: No such directory.".format(os.path.abspath(args.store_dir)))
            return False
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
