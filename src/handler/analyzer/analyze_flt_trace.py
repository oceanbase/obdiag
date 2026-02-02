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
@time: 2023/11/21
@file: analyze_flt_trace.py
@desc:
"""
import json
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
import traceback
from src.common.constant import const
from src.common.command import LocalClient, SshClient, delete_file
from src.handler.analyzer.log_parser.tree import Tree
from src.common.command import download_file, mkdir
from src.common.tool import TimeUtils
from src.common.tool import Util
from src.common.tool import DirectoryUtil
from src.common.tool import FileUtil
from src.common.result_type import ObdiagResult
from src.common.ob_connector import OBConnector
from src.common.command import get_observer_version
from src.common.tool import StringUtils


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
        self.obproxy_nodes = []
        self.workers = const.FLT_TRACE_WORKER
        self.max_recursion = const.FLT_TRACE_TREE_MAX_RECURSION
        self.config_path = const.DEFAULT_CONFIG_PATH
        self.top = const.FLT_TRACE_TREE_TOP_LEAF
        self.output = const.FLT_TRACE_OUTPUT
        self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        self.nodes = self.context.cluster_config.get('servers', [])
        self.obproxy_nodes = self.context.obproxy_config.get('servers', [])
        # Note: Filtering will be done after collecting FLT trace logs
        # because we need to extract SQL trace_id or time range from the logs first
        if len(self.obproxy_nodes) > 0:
            self.nodes.extend(self.obproxy_nodes)
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
        temp_dir_option = Util.get_option(options, 'temp_dir')
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
        if temp_dir_option:
            self.gather_ob_log_temporary_dir = temp_dir_option
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data='init option failed')
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data='init config failed')
        local_store_parent_dir = os.path.join(self.gather_pack_dir, "obdiag_analyze_flt_result_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
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

        # First, collect logs from observer nodes to extract SQL trace_ids or time range
        observer_nodes = [n for n in self.nodes if n.get("host_type") != "OBPROXY"]
        obproxy_nodes_list = [n for n in self.nodes if n.get("host_type") == "OBPROXY"]

        # Collect observer logs first
        observer_files = []
        if self.is_ssh:
            for node in observer_nodes:
                resp, node_file_s = self.__handle_from_node(node, old_files, local_store_parent_dir)
                old_files.extend(node_file_s)
                for node_file in node_file_s:
                    observer_files.append([node, node_file])
                    node_files.append([node, node_file])
                    analyze_tuples.append((node.get("ip"), False, resp["error"], node_file))
        else:
            local_ip = '127.0.0.1'
            if observer_nodes:
                node = observer_nodes[0]
                node["ip"] = local_ip
                resp, node_file_s = self.__handle_from_node(node, old_files, local_store_parent_dir)
                old_files.extend(node_file_s)
                for node_file in node_file_s:
                    observer_files.append([node, node_file])
                    node_files.append([node, node_file])
                    analyze_tuples.append((node.get("ip"), False, resp["error"], node_file))

        # Filter obproxy nodes based on client_ip from gv$ob_sql_audit after collecting observer logs
        # Extract SQL trace_ids or time range from collected FLT trace logs
        # If no matching obproxy nodes are found, fallback to old strategy (scan all obproxy nodes)
        filtered_obproxy_nodes = None
        if self.flt_trace_id and len(obproxy_nodes_list) > 0 and len(observer_files) > 0:
            filtered_obproxy_nodes = self.__filter_obproxy_nodes_by_client_ip_from_logs(observer_files)
            if filtered_obproxy_nodes is not None and len(filtered_obproxy_nodes) > 0:
                original_count = len(obproxy_nodes_list)
                filtered_count = len(filtered_obproxy_nodes)
                self.stdio.print("Successfully filtered obproxy nodes: {0} -> {1} nodes (reduced by {2} nodes)".format(original_count, filtered_count, original_count - filtered_count))
                obproxy_nodes_list = filtered_obproxy_nodes
            else:
                # No matching obproxy nodes found, fallback to old strategy: use all obproxy nodes
                self.stdio.print("No matching obproxy nodes found by client_ip, fallback to old strategy: collect logs from all {0} obproxy node(s)".format(len(obproxy_nodes_list)))

        # Collect logs from filtered obproxy nodes (or all obproxy nodes when fallback)
        if self.is_ssh:
            for node in obproxy_nodes_list:
                handle_from_node(node)
        elif obproxy_nodes_list:
            local_ip = '127.0.0.1'
            node = obproxy_nodes_list[0]
            node["ip"] = local_ip
            handle_from_node(node)

        tree = Tree()
        with ProcessPoolExecutor(max(min(self.workers, len(node_files)), 1)) as executor:
            future_to_url = [executor.submit(self.parse_file, file) for file in node_files]
            for future in as_completed(future_to_url):
                data = future.result()
                tree.build(data)
        # output tree
        result = self.__output(local_store_parent_dir, tree, self.output)
        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": local_store_parent_dir, "result": result})

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
        ssh_client = None
        try:
            ssh_client = SshClient(self.context, node)
        except Exception as e:
            ssh = None
            self.stdio.exception("ssh {0}@{1}: failed, Please check the conf.".format(remote_user, remote_ip))
            ssh_failed = True
            resp["skip"] = True
            resp["error"] = "Please check the conf."
            return resp, node_files
        if not ssh_failed:
            gather_dir_name = "trace_merged_cache"
            gather_dir_full_path = "{0}/{1}".format(self.gather_ob_log_temporary_dir, gather_dir_name)
            mkdir_info = mkdir(ssh_client, gather_dir_full_path, self.stdio)
            if mkdir_info:
                resp["skip"] = True
                resp["error"] = mkdir_info
                return resp, node_files
            if self.is_ssh:
                self.__get_online_log_file(ssh_client, node, gather_dir_full_path, local_store_dir)
            else:
                self.__get_offline_log_file(ssh_client, gather_dir_full_path, local_store_dir)
            delete_file(ssh_client, os.path.join(gather_dir_full_path, str(node.get("host_type")) + '-' + str(self.flt_trace_id)), self.stdio)
            for file in FileUtil.find_all_file(local_store_dir):
                if self.flt_trace_id in file and (file not in old_files):
                    node_files.append(file)
        return resp, node_files

    def __get_online_log_file(self, ssh_client, node, gather_path, local_store_dir):
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
        ssh_client.exec_cmd(grep_cmd)
        log_full_path = "{gather_path}/{log_name}".format(log_name=self.flt_trace_id, gather_path=gather_path)
        download_file(ssh_client, log_full_path, local_store_path, self.stdio)

    def __get_offline_log_file(self, ssh_client, log_path, local_store_dir):
        """
        :param ssh_client, log_name
        :return:
        """
        local_store_path = os.path.join(local_store_dir, self.flt_trace_id)
        log_name_list = self.__get_log_name_list_offline()
        if self.flt_trace_id is not None and (len(log_name_list) > 0):
            grep_cmd = "grep -e '{grep_args}' {log_file} > {local_store_path} ".format(grep_args=self.flt_trace_id, log_file=' '.join(log_name_list), local_store_path=local_store_path)
            LocalClient(self.stdio).run(grep_cmd)

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
        result_info = ""
        with open(filename, 'r', encoding='utf-8') as f:
            line_nu = 0
            for line in f:
                result_info += line
                line_nu += 1
                if line_nu > 60:
                    break
        return result_info

    def __get_sql_audit_view_name(self):
        """
        Get the correct SQL audit view name based on OceanBase version.
        Returns 'gv$ob_sql_audit' for OB 4.x, 'gv$sql_audit' for OB 3.x.
        """
        try:
            ob_version = get_observer_version(self.context)
            is_ob4 = StringUtils.compare_versions_greater(ob_version, "4.0.0.0") or ob_version.startswith("4.")
            if is_ob4:
                return "oceanbase.gv$ob_sql_audit"
            else:
                return "oceanbase.gv$sql_audit"
        except Exception as e:
            self.stdio.warn("Failed to get OceanBase version, defaulting to 3.x view name (gv$sql_audit): {0}".format(str(e)))
            return "oceanbase.gv$sql_audit"

    def __filter_obproxy_nodes_by_client_ip_from_logs(self, node_files):
        """
        Filter obproxy nodes based on client_ip extracted from FLT trace logs.
        First try to extract SQL trace_ids from FLT trace logs, then query SQL audit view.
        If SQL trace_ids are not found, try to extract time range and query by time range.
        Returns filtered obproxy nodes list, or None if filtering failed.
        """
        try:
            ob_cluster = self.context.cluster_config
            if not ob_cluster or not ob_cluster.get("db_host"):
                self.stdio.warn("Database connection not available, skip obproxy node filtering")
                return None

            # Get all obproxy nodes from config
            all_obproxy_nodes = self.context.obproxy_config.get('servers', [])
            if not all_obproxy_nodes:
                self.stdio.verbose("No obproxy nodes configured, skip filtering")
                return None

            # Get correct SQL audit view name based on OB version
            sql_audit_view = self.__get_sql_audit_view_name()
            self.stdio.print("Starting to filter obproxy nodes based on client_ip from {0} for FLT trace_id: {1}".format(sql_audit_view, self.flt_trace_id))
            self.stdio.print("Original obproxy nodes count: {0}".format(len(all_obproxy_nodes)))

            # Extract SQL trace_ids from collected FLT trace logs
            sql_trace_ids = self.__extract_sql_trace_ids_from_logs(node_files)
            time_range = self.__extract_time_range_from_logs(node_files)

            # Create ob_connector to query SQL audit view
            self.stdio.verbose("Connecting to database {0}:{1} to query {2}".format(ob_cluster.get("db_host"), ob_cluster.get("db_port"), sql_audit_view))
            ob_connector = OBConnector(
                context=self.context,
                ip=ob_cluster.get("db_host"),
                port=ob_cluster.get("db_port"),
                username=ob_cluster.get("tenant_sys").get("user"),
                password=ob_cluster.get("tenant_sys").get("password"),
                timeout=10000,
            )

            client_ips = set()

            # First, try to query by SQL trace_ids extracted from logs
            if sql_trace_ids:
                self.stdio.verbose("Found {0} SQL trace_id(s) in FLT trace logs".format(len(sql_trace_ids)))
                client_ips.update(self.__query_client_ip_by_sql_trace_ids(ob_connector, sql_trace_ids, sql_audit_view))

            # If no client_ip found, try to query by time range
            if not client_ips and time_range:
                self.stdio.verbose("Querying {0} by time range: {1} to {2}".format(sql_audit_view, time_range[0], time_range[1]))
                client_ips.update(self.__query_client_ip_by_time_range(ob_connector, time_range, sql_audit_view))

            # Fallback: try direct FLT trace_id query (in case they match)
            if not client_ips:
                self.stdio.verbose("Trying direct FLT trace_id query as fallback")
                client_ips.update(self.__query_client_ip_by_flt_trace_id(ob_connector, sql_audit_view))

            if not client_ips:
                self.stdio.warn("No client_ip found in {0}, using all obproxy nodes".format(sql_audit_view))
                return None

            # Filter obproxy nodes by matching IP
            self.stdio.print("Found {0} distinct client_ip(s): {1}".format(len(client_ips), ", ".join(sorted(client_ips)) if client_ips else "none"))
            self.stdio.verbose("Matching obproxy nodes with client_ip(s)...")
            filtered_nodes = []
            for node in all_obproxy_nodes:
                node_ip = node.get("ip", "").strip()
                if node_ip in client_ips:
                    filtered_nodes.append(node)
                    self.stdio.verbose("Matched obproxy node: {0}".format(node_ip))

            if len(filtered_nodes) == 0:
                self.stdio.warn("No obproxy nodes matched the client_ip(s), using all obproxy nodes")
                return None

            return filtered_nodes

        except Exception as e:
            self.stdio.warn("Failed to filter obproxy nodes by client_ip: {0}, using all obproxy nodes".format(str(e)))
            self.stdio.verbose(traceback.format_exc())
            return None

    def __extract_sql_trace_ids_from_logs(self, node_files):
        """Extract SQL trace_ids from FLT trace log files."""
        sql_trace_ids = set()
        import re

        # SQL trace_id pattern: YB format like YB42060CEBF7-000648E247342CEF-0-0
        sql_trace_id_pattern = re.compile(r'YB[0-9A-F]+-[0-9A-F]+-[0-9]+-[0-9]+')

        for node_file_pair in node_files:
            file_path = node_file_pair[1]
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        matches = sql_trace_id_pattern.findall(line)
                        for match in matches:
                            sql_trace_ids.add(match)
            except Exception as e:
                self.stdio.verbose("Failed to read file {0}: {1}".format(file_path, str(e)))

        return list(sql_trace_ids)

    def __extract_time_range_from_logs(self, node_files):
        """Extract time range from FLT trace log files."""
        import re
        from datetime import datetime, timedelta

        timestamps = []
        # Try to extract timestamps from log lines (format may vary)
        timestamp_patterns = [
            re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}'),  # YYYY-MM-DD HH:MM:SS
            re.compile(r'"start_ts":\s*(\d+)'),  # JSON format start_ts
            re.compile(r'"end_ts":\s*(\d+)'),  # JSON format end_ts
        ]

        for node_file_pair in node_files:
            file_path = node_file_pair[1]
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        # Try pattern 1: datetime string
                        match = timestamp_patterns[0].search(line)
                        if match:
                            try:
                                dt = datetime.strptime(match.group(), '%Y-%m-%d %H:%M:%S')
                                timestamps.append(dt)
                            except:
                                pass
                        # Try pattern 2 & 3: timestamp in microseconds
                        for pattern in timestamp_patterns[1:]:
                            match = pattern.search(line)
                            if match:
                                try:
                                    ts_us = int(match.group(1))
                                    # Convert microseconds to datetime (assuming microseconds since epoch)
                                    dt = datetime.fromtimestamp(ts_us / 1000000)
                                    timestamps.append(dt)
                                except:
                                    pass
            except Exception as e:
                self.stdio.verbose("Failed to read file {0}: {1}".format(file_path, str(e)))

        if timestamps:
            min_time = min(timestamps)
            max_time = max(timestamps)
            # Add buffer: 5 minutes before and after
            time_from = (min_time - timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            time_to = (max_time + timedelta(minutes=5)).strftime('%Y-%m-%d %H:%M:%S')
            return (time_from, time_to)

        return None

    def __query_client_ip_by_sql_trace_ids(self, ob_connector, sql_trace_ids, sql_audit_view):
        """Query client_ip from SQL audit view by SQL trace_ids."""
        client_ips = set()
        if not sql_trace_ids:
            return client_ips

        # Build SQL with IN clause
        trace_ids_str = "', '".join(sql_trace_ids)
        sql = """
            SELECT DISTINCT client_ip 
            FROM {0}
            WHERE trace_id IN ('{1}')
            AND client_ip IS NOT NULL 
            AND length(client_ip) > 0
            LIMIT 1000
        """.format(
            sql_audit_view, trace_ids_str
        )

        try:
            self.stdio.verbose("Querying {0} by SQL trace_ids".format(sql_audit_view))
            result = ob_connector.execute_sql(sql)
            if result:
                for row in result:
                    if row and len(row) > 0 and row[0]:
                        client_ip = str(row[0]).strip()
                        if client_ip:
                            client_ips.add(client_ip)
                            self.stdio.verbose("Found client_ip: {0}".format(client_ip))
        except Exception as e:
            self.stdio.verbose("Failed to query {0} by SQL trace_ids: {1}".format(sql_audit_view, str(e)))

        return client_ips

    def __query_client_ip_by_time_range(self, ob_connector, time_range, sql_audit_view):
        """Query client_ip from SQL audit view by time range."""
        client_ips = set()
        if not time_range:
            return client_ips

        time_from, time_to = time_range
        # OB 4.x uses usec_to_time, OB 3.x uses from_unixtime
        if "gv$ob_sql_audit" in sql_audit_view:
            # OB 4.x
            sql = """
                SELECT DISTINCT client_ip 
                FROM {0}
                WHERE usec_to_time(request_time) >= '{1}'
                AND usec_to_time(request_time) <= '{2}'
                AND client_ip IS NOT NULL 
                AND length(client_ip) > 0
                LIMIT 1000
            """.format(
                sql_audit_view, time_from, time_to
            )
        else:
            # OB 3.x
            sql = """
                SELECT DISTINCT client_ip 
                FROM {0}
                WHERE from_unixtime(request_time/1000000) >= '{1}'
                AND from_unixtime(request_time/1000000) <= '{2}'
                AND client_ip IS NOT NULL 
                AND length(client_ip) > 0
                LIMIT 1000
            """.format(
                sql_audit_view, time_from, time_to
            )

        try:
            self.stdio.verbose("Querying {0} by time range".format(sql_audit_view))
            result = ob_connector.execute_sql(sql)
            if result:
                for row in result:
                    if row and len(row) > 0 and row[0]:
                        client_ip = str(row[0]).strip()
                        if client_ip:
                            client_ips.add(client_ip)
                            self.stdio.verbose("Found client_ip: {0}".format(client_ip))
        except Exception as e:
            self.stdio.verbose("Failed to query {0} by time range: {1}".format(sql_audit_view, str(e)))

        return client_ips

    def __query_client_ip_by_flt_trace_id(self, ob_connector, sql_audit_view):
        """Query client_ip from SQL audit view by FLT trace_id (fallback)."""
        client_ips = set()

        sql = """
            SELECT DISTINCT client_ip 
            FROM {0}
            WHERE trace_id = '{1}' 
            AND client_ip IS NOT NULL 
            AND length(client_ip) > 0
            LIMIT 1000
        """.format(
            sql_audit_view, self.flt_trace_id
        )

        try:
            self.stdio.verbose("Querying {0} by FLT trace_id (fallback)".format(sql_audit_view))
            result = ob_connector.execute_sql(sql)
            if result:
                for row in result:
                    if row and len(row) > 0 and row[0]:
                        client_ip = str(row[0]).strip()
                        if client_ip:
                            client_ips.add(client_ip)
                            self.stdio.verbose("Found client_ip: {0}".format(client_ip))
        except Exception as e:
            self.stdio.verbose("Failed to query {0} by FLT trace_id: {1}".format(sql_audit_view, str(e)))

        return client_ips

    def parse_file(self, file):
        self.stdio.verbose('parse file: {}'.format(file[1]))
        if file[1].endswith('.json'):
            return self.__parse_json_file(file[0], file[1], self.flt_trace_id)
        else:
            return self.__parse_log_file(file[0], file[1], self.flt_trace_id)
