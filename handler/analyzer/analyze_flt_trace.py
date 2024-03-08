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
@time: 2023/11/21
@file: analyze_flt_trace.py
@desc:
"""
import json
import os
import sys
import threading
import uuid
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback

from common.logger import logger
from common.constant import const
from common.command import LocalClient, SshClient, delete_file
from handler.analyzer.log_parser.tree import Tree
from utils.file_utils import mkdir_if_not_exist, find_all_file
from common.command import download_file, mkdir
from utils.shell_utils import SshHelper
from utils.time_utils import str_2_timestamp
from utils.time_utils import timestamp_to_filename_time
from utils.time_utils import get_current_us_timestamp
from utils.utils import get_localhost_inner_ip, display_trace


class AnalyzeFltTraceHandler(object):
    def __init__(self, nodes, gather_pack_dir):
        self.directly_analyze_files = False
        self.analyze_files_list = []
        self.is_ssh = True
        self.gather_ob_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.gather_pack_dir = gather_pack_dir
        self.flt_trace_id = ''
        self.nodes = nodes
        self.workers = const.FLT_TRACE_WORKER
        self.max_recursion = const.FLT_TRACE_TREE_MAX_RECURSION
        self.config_path = const.DEFAULT_CONFIG_PATH
        self.top = const.FLT_TRACE_TREE_TOP_LEAF
        self.output = const.FLT_TRACE_OUTPUT
        self.gather_timestamp = get_current_us_timestamp()

    def handle(self, args):
        if not self.__check_valid_and_parse_args(args):
            return
        local_store_parent_dir = os.path.join(self.gather_pack_dir, "analyze_flt_result_{0}".format(timestamp_to_filename_time(self.gather_timestamp)))
        logger.info("Use {0} as pack dir.".format(local_store_parent_dir))
        analyze_tuples = []
        node_files = []
        old_files = []

        def handle_from_node(node):
            resp, node_file_s = self.__handle_from_node(node, old_files, local_store_parent_dir)
            old_files.extend(node_file_s)
            for node_file in node_file_s:
                node_files.append([node, node_file])
                analyze_tuples.append((node.get("ip"), False, resp["error"], node_file))

        if self.is_ssh:
            for node in self.nodes:
                handle_from_node(node)
        else:
            local_ip = '127.0.0.1'
            node = self.nodes[0]
            node["ip"] = local_ip
            handle_from_node(node)

        tree = Tree()
        with ProcessPoolExecutor(max(min(self.workers, len(node_files)), 1)) as executor:
            future_to_url = [executor.submit(self.parse_file, file) for file in node_files]
            for future in as_completed(future_to_url):
                data = future.result()
                tree.build(data)
        # output tree
        self.__output(local_store_parent_dir, tree, self.output)
        display_trace(uuid.uuid3(uuid.NAMESPACE_DNS, str(os.getpid())))
        return analyze_tuples

    def __handle_from_node(self, node, old_files, local_store_parent_dir):
        resp = {
            "skip": False,
            "error": ""
        }
        remote_ip = node.get("ip") if self.is_ssh else '127.0.0.1'
        remote_user = node.get("user")
        remote_password = node.get("password")
        remote_port = node.get("port")
        remote_private_key = node.get("private_key")
        node_files = []
        logger.info("Sending Collect Shell Command to node {0} ...".format(remote_ip))
        mkdir_if_not_exist(local_store_parent_dir)
        if "ssh_type" in node and node["ssh_type"] == "docker":
            local_store_dir = "{0}/docker_{1}".format(local_store_parent_dir, node["container_name"])
        else:
            local_store_dir = "{0}/{1}".format(local_store_parent_dir, remote_ip)
        mkdir_if_not_exist(local_store_dir)
        ssh_failed = False
        try:
            ssh = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key, node)
        except Exception as e:
            ssh = None
            logger.error("ssh {0}@{1}: failed, Please check the {2}".format(
                remote_user,
                remote_ip,
                self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)
        if not ssh_failed:
            gather_dir_name = "trace_merged_cache"
            gather_dir_full_path = "{0}/{1}".format("/tmp", gather_dir_name)
            mkdir(self.is_ssh, ssh, gather_dir_full_path)
            if self.is_ssh:
                self.__get_online_log_file(ssh, node, gather_dir_full_path, local_store_dir)
            else:
                self.__get_offline_log_file(ssh, gather_dir_full_path, local_store_dir)
            delete_file(self.is_ssh, ssh, os.path.join(gather_dir_full_path, str(node.get("host_type")) + '-' + str(self.flt_trace_id)))
            ssh.ssh_close()
            for file in find_all_file(local_store_dir):
                if self.flt_trace_id in file and (file not in old_files):
                    node_files.append(file)
        return resp, node_files

    def __get_online_log_file(self, ssh_helper, node, gather_path, local_store_dir):
        """
        :param ssh_helper, log_name, gather_path
        :return:
        """
        home_path = node.get("home_path")
        log_path = os.path.join(home_path, "log")
        local_store_path = "{0}/{1}".format(local_store_dir, str(node.get("host_type")) + '-' + str(self.flt_trace_id))
        def check_filename(filename):
            if os.path.exists(filename):
                # 文件已存在，尝试添加后缀
                for nu in range(1, 1000):
                    new_filename = filename + f".{nu}"
                    if not os.path.exists(new_filename):
                        # 找到可用的文件名
                        return new_filename
                # 没有可用的后缀
                return None
            else:
                # 文件不存在
                return filename

        local_store_path = check_filename(local_store_path)
        grep_cmd = "grep '{grep_args}' {log_dir}/*trace.log* > {gather_path}/{log_name} ".format(
            grep_args=self.flt_trace_id,
            gather_path=gather_path,
            log_name=self.flt_trace_id,
            log_dir=log_path)
        logger.debug("grep files, run cmd = [{0}]".format(grep_cmd))
        SshClient().run(ssh_helper, grep_cmd)
        log_full_path = "{gather_path}/{log_name}".format(
            log_name=self.flt_trace_id,
            gather_path=gather_path
        )
        download_file(True, ssh_helper, log_full_path, local_store_path)

    def __get_offline_log_file(self, ssh_helper, log_full_path, local_store_dir):
        """
        :param ssh_helper, log_name
        :return:
        """
        local_store_path = os.path.join(local_store_dir, self.flt_trace_id)
        log_name_list = self.__get_log_name_list_offline()
        if self.flt_trace_id is not None and (len(log_name_list) > 0):
            grep_cmd = "grep -e '{grep_args}' {log_file} > {local_store_path} ".format(
                grep_args=self.flt_trace_id,
                log_file=' '.join(log_name_list),
                local_store_path=local_store_path)
            LocalClient().run(grep_cmd)
            download_file(False, ssh_helper, log_full_path, local_store_path)

    def __get_log_name_list_offline(self):
        """
        :param:
        :return: log_name_list
        """
        log_name_list = []
        if self.analyze_files_list and len(self.analyze_files_list) > 0:
            for path in self.analyze_files_list:
                if os.path.exists(path):
                    if os.path.isfile(path):
                        log_name_list.append(path)
                    else:
                        log_names = find_all_file(path)
                        if len(log_names) > 0:
                            log_name_list.extend(log_names)
        logger.info("get log list {}".format(log_name_list))
        return log_name_list

    def __parse_log_file(self, node, file, trace):
        counter = 0
        li = []
        with open(file, 'r', encoding='utf-8') as f:
            while True:
                line = f.readline()
                if line:
                    parsed = self.parse_line(node, line, trace)
                    if parsed:
                        counter += 1
                        li.append(parsed)
                else:
                    logger.info('file:{} trace:{} total:{}'.format(file, trace, counter))
                    break
        return li

    def __parse_json_file(self, node, file, trace):
        key_mapping = {
            'span_id': 'id',
            'parent': 'parent_id',
            'span_name': 'name',
        }
        time_keys = ['start_ts', 'end_ts']

        def remap_key(di):
            for key, new_key in key_mapping.items():
                if key in di:
                    temp = di[key]
                    di[new_key] = temp
                    di.pop(key)
            return di
        li = []
        with open(file, 'r', encoding='utf-8') as f:
            content = f.read()
            try:
                data = json.loads(content)
            except Exception:
                logger.info(traceback.format_exc())
                sys.exit()
            if not isinstance(data, list):
                raise ValueError('json file is not a list')
            for item in data:
                if trace == item['trace_id']:
                    li.append(remap_key(item))
                for key in time_keys:
                    item[key] = str_2_timestamp(item[key])
        return li

    def parse_line(self, node, line, trace):
        traced_prefix = '"trace_id"'
        traced = '{' + ('"trace_id":"%s"' % trace if trace else '')
        data_start = '{"host_ip": "' + node.get("ip") + '", "host_type":  "' + node.get("host_type") + ' ", "trace_data":'
        data_end = '}'
        idx = line.find(traced)
        if idx == -1:
            return
        else:
            try:
                countStr = line.count(traced_prefix)
                if countStr == 1:
                    return json.loads(data_start + line[idx:-1] + data_end)
                else:
                    line_last = line[idx + 1:len(line) + 1]
                    idx_last = line_last.find(traced_prefix)
                    new_line = line[idx:(idx + idx_last + 1)]
                    if len(new_line) > 10:
                        return json.loads(data_start + new_line + data_end)
                    else:
                        pass
            except:
                if line.endswith(']}\n'):
                    new_line_data = line[idx:-3] + "...\"}]}"
                else:
                    new_line_data = line[idx:-1] + '}'
                try:
                    return json.loads(data_start + new_line_data + data_end)
                except:
                    new_line_data = line.replace('\t', '\\t')[idx:-5] + '..."}]}'
                    return json.loads(data_start + new_line_data + data_end)

    def __scan_trace_file(self, path='.'):
        keyword = 'trace.log'
        for entry in os.listdir(path):
            if keyword in entry:
                yield os.path.join(path, entry)

    def __file_mapping(self, args):
        trace_id = args.trace_id
        results = []
        for log_dir in args.log_dirs:
            if not os.path.isdir(log_dir):
                logger.info('Dir not exist: {}'.format(log_dir))
                continue
            for file in self.__scan_trace_file(log_dir):
                results.append((file, trace_id))
        for file in args.log_files:
            if not os.path.isfile(file):
                logger.info('File not exist: {}'.format(file))
                continue
            if (file, trace_id) not in results:
                results.append((file, trace_id))
        return results

    def __output(self, result_dir, tree, output_terminal=60):
        if not tree.nodes:
            logger.warning("The analysis result is empty")
            return
        filename = os.path.join(result_dir, '{}.txt'.format(self.flt_trace_id))
        line_counter = 0
        with open(filename, 'w', encoding='utf-8') as f:
            for line in tree.traverse(self.max_recursion, self.top):
                f.write(line)
                f.write('\n')
                line_counter += 1
                if line_counter < output_terminal:
                    if len(line) > 100:
                        print(line[:97], '...')
                    else:
                        print(line)
                elif line_counter == output_terminal:
                    print('Result too large, wait a moment ...\n')
        logger.debug('Result saved: {}'.format(os.path.abspath(filename)))
        last_info = "For more details, please run cmd \033[32m' cat {0} '\033[0m\n".format(filename)
        print(last_info)

    def parse_file(self, file):
        logger.info('parse file: {}'.format(file[1]))
        if file[1].endswith('.json'):
            return self.__parse_json_file(file[0], file[1], self.flt_trace_id)
        else:
            return self.__parse_log_file(file[0], file[1], self.flt_trace_id)

    def __check_valid_and_parse_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        if getattr(args, "files") is not None:
            self.directly_analyze_files = True
            self.analyze_files_list = getattr(args, "files")
            self.is_ssh = False
        # 2: store_dir must exist, else create directory.
        if getattr(args, "store_dir") is not None:
            if not os.path.exists(os.path.abspath(getattr(args, "store_dir"))):
                logger.warn("Error: args --store_dir [{0}] incorrect: No such directory, Now create it".format(os.path.abspath(getattr(args, "store_dir"))))
                os.makedirs(os.path.abspath(getattr(args, "store_dir")))
            self.gather_pack_dir = os.path.abspath(getattr(args, "store_dir"))
        if getattr(args, "flt_trace_id") is not None:
            self.flt_trace_id = getattr(args, "flt_trace_id")[0]
        if getattr(args, "top") is not None:
            self.top = int(getattr(args, "top")[0])
        if getattr(args, "recursion") is not None:
            self.max_recursion = int(getattr(args, "recursion")[0])
        if getattr(args, "output") is not None:
            self.output = int(getattr(args, "output")[0])
        return True
