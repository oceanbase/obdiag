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
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
from common.constant import const
from common.command import LocalClient, SshClient, delete_file
from handler.analyzer.log_parser.tree import Tree
from common.command import download_file, mkdir
from common.ssh import SshHelper
from common.tool import TimeUtils
from common.tool import Util
from common.tool import DirectoryUtil
from common.tool import FileUtil


class AnalyzeFltTraceHandler(object):
    def __init__(self, context, gather_pack_dir=None):
        self.context = context
        self.stdio = context.stdio
        self.directly_analyze_files = False
        self.analyze_files_list = []
        self.is_ssh = True
        self.gather_ob_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.gather_pack_dir = gather_pack_dir
        self.flt_trace_id = ''
        self.nodes = []
        self.workers = const.FLT_TRACE_WORKER
        self.max_recursion = const.FLT_TRACE_TREE_MAX_RECURSION
        self.config_path = const.DEFAULT_CONFIG_PATH
        self.top = const.FLT_TRACE_TREE_TOP_LEAF
        self.output = const.FLT_TRACE_OUTPUT
        self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        self.nodes = self.context.cluster_config['servers']
        self.inner_config = self.context.inner_config
        return True

    def init_option(self):
        options = self.context.options
        files_option = Util.get_option(options, 'files')
        store_dir_option = Util.get_option(options, 'store_dir')
        flt_trace_id_option = Util.get_option(options, 'flt_trace_id')
        top_option = Util.get_option(options, 'top')
        recursion_option = Util.get_option(options, 'recursion')
        output_option = Util.get_option(options, 'output')
        if store_dir_option is not None:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('Warning: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        if files_option:
            self.directly_analyze_files = True
            self.analyze_files_list = files_option
            self.is_ssh = False
        if flt_trace_id_option:
            self.flt_trace_id = flt_trace_id_option
        else:
            self.stdio.error("option --flt_trace_id not found, please provide")
            return False
        if top_option:
            self.top = int(top_option)
        if recursion_option:
            self.max_recursion = int(recursion_option)
        if output_option:
            self.output = int(output_option)
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        local_store_parent_dir = os.path.join(self.gather_pack_dir, "analyze_flt_result_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
        self.stdio.verbose("Use {0} as pack dir.".format(local_store_parent_dir))
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
        return analyze_tuples

    def __handle_from_node(self, node, old_files, local_store_parent_dir):
        resp = {"skip": False, "error": ""}
        remote_ip = node.get("ip") if self.is_ssh else '127.0.0.1'
        remote_user = node.get("ssh_username")
        remote_password = node.get("ssh_password")
        remote_port = node.get("ssh_port")
        remote_private_key = node.get("ssh_key_file")
        node_files = []
        self.stdio.verbose("Sending Collect Shell Command to node {0} ...".format(remote_ip))
        DirectoryUtil.mkdir(path=local_store_parent_dir, stdio=self.stdio)
        if "ssh_type" in node and node["ssh_type"] == "docker":
            local_store_dir = "{0}/docker_{1}".format(local_store_parent_dir, node["container_name"])
        else:
            local_store_dir = "{0}/{1}".format(local_store_parent_dir, remote_ip)
        DirectoryUtil.mkdir(path=local_store_dir, stdio=self.stdio)
        ssh_failed = False
        try:
            ssh = SshHelper(self.is_ssh, remote_ip, remote_user, remote_password, remote_port, remote_private_key, node)
        except Exception as e:
            ssh = None
            self.stdio.exception("ssh {0}@{1}: failed, Please check the {2}".format(remote_user, remote_ip, self.config_path))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the {0}".format(self.config_path)
        if not ssh_failed:
            gather_dir_name = "trace_merged_cache"
            gather_dir_full_path = "{0}/{1}".format("/tmp", gather_dir_name)
            mkdir(self.is_ssh, ssh, gather_dir_full_path, self.stdio)
            if self.is_ssh:
                self.__get_online_log_file(ssh, node, gather_dir_full_path, local_store_dir)
            else:
                self.__get_offline_log_file(ssh, gather_dir_full_path, local_store_dir)
            delete_file(self.is_ssh, ssh, os.path.join(gather_dir_full_path, str(node.get("host_type")) + '-' + str(self.flt_trace_id)), self.stdio)
            ssh.ssh_close()
            for file in FileUtil.find_all_file(local_store_dir):
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
        grep_cmd = "grep '{grep_args}' {log_dir}/*trace.log* > {gather_path}/{log_name} ".format(grep_args=self.flt_trace_id, gather_path=gather_path, log_name=self.flt_trace_id, log_dir=log_path)
        self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
        SshClient(self.stdio).run(ssh_helper, grep_cmd)
        log_full_path = "{gather_path}/{log_name}".format(log_name=self.flt_trace_id, gather_path=gather_path)
        download_file(True, ssh_helper, log_full_path, local_store_path, self.stdio)

    def __get_offline_log_file(self, ssh_helper, log_full_path, local_store_dir):
        """
        :param ssh_helper, log_name
        :return:
        """
        local_store_path = os.path.join(local_store_dir, self.flt_trace_id)
        log_name_list = self.__get_log_name_list_offline()
        if self.flt_trace_id is not None and (len(log_name_list) > 0):
            grep_cmd = "grep -e '{grep_args}' {log_file} > {local_store_path} ".format(grep_args=self.flt_trace_id, log_file=' '.join(log_name_list), local_store_path=local_store_path)
            LocalClient(self.stdio).run(grep_cmd)
            download_file(False, ssh_helper, log_full_path, local_store_path, self.stdio)

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
                        log_names = FileUtil.find_all_file(path)
                        if len(log_names) > 0:
                            log_name_list.extend(log_names)
        self.stdio.verbose("get log list {}".format(log_name_list))
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
                    self.stdio.verbose('file:{} trace:{} total:{}'.format(file, trace, counter))
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
                self.stdio.verbose(traceback.format_exc())
                sys.exit()
            if not isinstance(data, list):
                raise ValueError('json file is not a list')
            for item in data:
                if trace == item['trace_id']:
                    li.append(remap_key(item))
                for key in time_keys:
                    item[key] = TimeUtils.str_2_timestamp(item[key])
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
                    line_last = line[idx + 1 : len(line) + 1]
                    idx_last = line_last.find(traced_prefix)
                    new_line = line[idx : (idx + idx_last + 1)]
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
                self.stdio.verbose('Dir not exist: {}'.format(log_dir))
                continue
            for file in self.__scan_trace_file(log_dir):
                results.append((file, trace_id))
        for file in args.log_files:
            if not os.path.isfile(file):
                self.stdio.verbose('File not exist: {}'.format(file))
                continue
            if (file, trace_id) not in results:
                results.append((file, trace_id))
        return results

    def __output(self, result_dir, tree, output_terminal=60):
        if not tree.nodes:
            self.stdio.warn("The analysis result is empty")
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
                        self.stdio.print("{0} {1}".format(line[:97], '...'))
                    else:
                        self.stdio.print(line)
                elif line_counter == output_terminal:
                    self.stdio.print('Result too large, wait a moment ...\n')
        self.stdio.verbose('Result saved: {}'.format(os.path.abspath(filename)))
        last_info = "For more details, please run cmd \033[32m' cat {0} '\033[0m\n".format(filename)
        self.stdio.print(last_info)

    def parse_file(self, file):
        self.stdio.verbose('parse file: {}'.format(file[1]))
        if file[1].endswith('.json'):
            return self.__parse_json_file(file[0], file[1], self.flt_trace_id)
        else:
            return self.__parse_log_file(file[0], file[1], self.flt_trace_id)
