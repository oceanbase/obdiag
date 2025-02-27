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
import time
import datetime

import tabulate

from src.common.command import get_observer_pid, mkdir, zip_dir, get_file_size, download_file, delete_file_force, is_empty_dir, is_empty_file
from src.common.command import SshClient
from src.common.constant import const
from src.handler.base_shell_handler import BaseShellHandler
from src.common.tool import Util
from src.common.tool import DirectoryUtil
from src.common.tool import FileUtil
from src.common.tool import NetUtils
from src.common.tool import TimeUtils
from src.common.result_type import ObdiagResult


class GatherPerfHandler(BaseShellHandler):
    def __init__(self, context, gather_pack_dir='./', is_scene=False):
        super(GatherPerfHandler, self).__init__()
        self.context = context
        self.stdio = context.stdio
        self.is_ssh = True
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        self.ob_install_dir = None
        self.is_scene = is_scene
        self.scope = "all"
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
        count_option = Util.get_option(options, 'count')
        if self.context.get_variable("gather_perf_sample_count", None):
            count_option = self.context.get_variable("gather_perf_sample_count")
        if isinstance(count_option, str):
            try:
                count_option = int(count_option)
            except ValueError:
                try:
                    count_option = float(count_option)
                except ValueError:
                    count_option = None
        if isinstance(count_option, (int, float)) and count_option >= 1000000:
            count_option = count_option
        else:
            count_option = 100000000
        self.count_option = count_option
        store_dir_option = Util.get_option(options, 'store_dir')
        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_path = os.path.abspath(store_dir_option)
        self.scope_option = Util.get_option(options, 'scope')
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init option failed")
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init config failed")
        if self.is_scene:
            pack_dir_this_command = self.local_stored_path
        else:
            pack_dir_this_command = os.path.join(self.local_stored_path, "obdiag_gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
        self.stdio.verbose("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []

        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(node, pack_dir_this_command)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            gather_tuples.append((node.get("ip"), False, resp["error"], file_size, int(time.time() - st), resp["gather_pack_path"]))

        exec_tag = False
        if self.is_ssh:
            for node in self.nodes:
                if node.get("ssh_type") == "docker" or node.get("ssh_type") == "kubernetes":
                    self.stdio.warn("Skip gather from node {0} because it is a docker or kubernetes node".format(node.get("ip")))
                    continue
                handle_from_node(node)
                exec_tag = True
        else:
            local_ip = NetUtils.get_inner_ip(self.stdio)
            node = self.nodes[0]
            node["ip"] = local_ip
            for node in self.nodes:
                handle_from_node(node)
        if not exec_tag:
            self.stdio.verbose("No node to gather from, skip")
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

        summary_tuples = self.__get_overall_summary(gather_tuples)
        self.stdio.print(summary_tuples)
        # Persist the summary results to a file
        FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)
        last_info = "For result details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(pack_dir_this_command, "result_summary.txt"))
        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

    def __handle_from_node(self, node, local_stored_path):
        resp = {"skip": False, "error": "", "gather_pack_path": ""}
        remote_ip = node.get("ip") if self.is_ssh else NetUtils.get_inner_ip(self.stdio)
        remote_user = node.get("ssh_username")
        self.stdio.verbose("Sending Collect Shell Command to node {0} ...".format(remote_ip))
        DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        remote_dir_name = "perf_{0}_{1}".format(node.get("ip").replace(":", "_"), now_time)
        remote_dir_full_path = "/tmp/{0}".format(remote_dir_name)
        ssh_failed = False
        ssh_client = None
        try:
            ssh_client = SshClient(self.context, node)
        except Exception as e:
            self.stdio.exception("ssh {0}@{1}: failed, Please check the node conf.".format(remote_user, remote_ip))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the node conf."
            return resp
        if not ssh_failed:
            mkdir(ssh_client, remote_dir_full_path, self.stdio)
            pid_observer_list = get_observer_pid(ssh_client, node.get("home_path"), self.stdio)
            if len(pid_observer_list) == 0:
                resp["error"] = "can't find observer"
                return resp
            for pid_observer in pid_observer_list:
                if self.__perf_checker(ssh_client):
                    if self.scope == "sample":
                        self.__gather_perf_sample(ssh_client, remote_dir_full_path, pid_observer)
                    elif self.scope == "flame":
                        self.__gather_perf_flame(ssh_client, remote_dir_full_path, pid_observer)
                    else:
                        self.__gather_perf_sample(ssh_client, remote_dir_full_path, pid_observer)
                        self.__gather_perf_flame(ssh_client, remote_dir_full_path, pid_observer)
                self.__gather_top(ssh_client, remote_dir_full_path, pid_observer)

            tar_cmd = "cd /tmp && tar -czf {0}.tar.gz {0}/*".format(remote_dir_name)
            tar_cmd_request = ssh_client.exec_cmd(tar_cmd)
            self.stdio.verbose("tar request is {0}".format(tar_cmd_request))
            remote_tar_file_path = "{0}.tar.gz".format(remote_dir_full_path)
            file_size = get_file_size(ssh_client, remote_tar_file_path, self.stdio)
            remote_tar_full_path = os.path.join("/tmp", remote_tar_file_path)
            if int(file_size) < self.file_size_limit:
                local_file_path = "{0}/{1}.tar.gz".format(local_stored_path, remote_dir_name)
                download_file(ssh_client, remote_tar_full_path, local_file_path, self.stdio)
                resp["error"] = ""
            else:
                resp["error"] = "File too large"
            delete_file_force(ssh_client, remote_tar_full_path, self.stdio)
            resp["gather_pack_path"] = "{0}/{1}.tar.gz".format(local_stored_path, remote_dir_name)
        return resp

    def __gather_perf_sample(self, ssh_client, gather_path, pid_observer):
        try:
            self.stdio.start_loading('gather perf sample')
            cmd = "cd {gather_path} && perf record -o sample.data -e cycles -c {count_option} -p {pid} -g -- sleep 20".format(gather_path=gather_path, count_option = self.count_option, pid=pid_observer)
            self.stdio.verbose("gather perf sample, run cmd = [{0}]".format(cmd))
            ssh_client.exec_cmd(cmd)
            generate_data = "cd {gather_path} && perf script -i sample.data -F ip,sym -f > sample.viz".format(gather_path=gather_path)
            self.stdio.verbose("generate perf sample data, run cmd = [{0}]".format(generate_data))
            ssh_client.exec_cmd(generate_data)
            self.is_ready(ssh_client, os.path.join(gather_path, 'sample.viz'))
            self.stdio.stop_loading('gather perf sample')
        except:
            self.stdio.error("generate perf sample data on server [{0}] failed".format(ssh_client.get_name()))

    def __perf_checker(self, ssh_client):
        cmd = "command -v perf"
        result = ssh_client.exec_cmd(cmd)

        if result:
            self.stdio.verbose("perf is installed at [{0}] on server [{1}]".format(result, ssh_client.get_name()))
            return True
        else:
            self.stdio.error("perf is not installed on server [{0}]. gather perf information will be skipped. Please install perf manually. ".format(ssh_client.get_name()))
            return False

    def __gather_perf_flame(self, ssh_client, gather_path, pid_observer):
        try:
            self.stdio.start_loading('gather perf flame')
            perf_cmd = "cd {gather_path} && perf record -o flame.data -F 99 -p {pid} -g -- sleep 20".format(gather_path=gather_path, pid=pid_observer)
            self.stdio.verbose("gather perf, run cmd = [{0}]".format(perf_cmd))
            ssh_client.exec_cmd(perf_cmd)

            generate_data = "cd {gather_path} && perf script -i flame.data > flame.viz".format(gather_path=gather_path)
            self.stdio.verbose("generate perf data, run cmd = [{0}]".format(generate_data))
            ssh_client.exec_cmd(generate_data)
            self.is_ready(ssh_client, os.path.join(gather_path, 'flame.viz'))
            self.stdio.stop_loading('gather perf flame')
        except:
            self.stdio.error("generate perf data on server [{0}] failed".format(ssh_client.get_name()))

    def __gather_top(self, ssh_client, gather_path, pid_observer):
        try:
            cmd = "cd {gather_path} && top -Hp {pid} -b -n 1 > top.txt".format(gather_path=gather_path, pid=pid_observer)
            self.stdio.verbose("gather top, run cmd = [{0}]".format(cmd))
            ssh_client.exec_cmd(cmd)
        except:
            self.stdio.error("gather top on server failed [{0}]".format(ssh_client.get_name()))

    @Util.retry(3, 5)
    def is_ready(self, ssh_client, remote_path):
        try:
            self.stdio.verbose("check whether the file {remote_path} is empty".format(remote_path=remote_path))
            is_empty_file_res = is_empty_file(ssh_client, remote_path, self.stdio)
            if is_empty_file_res:
                self.stdio.warn("The server {host_ip} directory {remote_path} is empty, waiting for the collection to complete".format(host_ip=ssh_client.get_name(), remote_path=remote_path))
                raise
        except Exception as e:
            raise e

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
                format_file_size = FileUtil.size_format(num=file_size, output_str=True)
            except:
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather Perf Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
