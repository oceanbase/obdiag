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
@time: 2023/9/23
@file: analyze_log.py
@desc:
"""
import datetime
import os
import re

from tabulate import tabulate
from common.command import get_observer_version_by_sql
from handler.base_shell_handler import BaseShellHandler
from common.obdiag_exception import OBDIAGFormatException
from common.constant import const
from common.command import LocalClient, SshClient
from common.ob_log_level import OBLogLevel
from handler.meta.ob_error import OB_RET_DICT
from common.command import download_file, get_logfile_name_list, mkdir, delete_file
from common.tool import DirectoryUtil, TimeUtils, Util, StringUtils
from common.tool import Util
from common.tool import DirectoryUtil
from common.tool import FileUtil
from common.tool import TimeUtils
import common.ssh_client.local_client as ssh_client_local_client

from common.ob_connector import OBConnector
import re


class AnalyzeQueueHandler(BaseShellHandler):
    def __init__(self, context):
        super(AnalyzeQueueHandler, self).__init__()
        self.context = context
        self.stdio = context.stdio
        self.directly_analyze_files = False
        self.analyze_files_list = []
        self.is_ssh = True
        self.gather_timestamp = None
        self.gather_ob_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.gather_pack_dir = None
        self.ob_log_dir = None
        self.from_time_str = None
        self.to_time_str = None
        self.zip_encrypt = False
        self.log_level = OBLogLevel.WARN
        self.config_path = const.DEFAULT_CONFIG_PATH
        self.ob_cluster = self.context.cluster_config
        self.tenant = None
        self.queue = None
        self.tenant_id = None
        self.ip_list = None
        self.scope = None
        try:
            self.obconn = OBConnector(
                ip=self.ob_cluster.get("db_host"),
                port=self.ob_cluster.get("db_port"),
                username=self.ob_cluster.get("tenant_sys").get("user"),
                password=self.ob_cluster.get("tenant_sys").get("password"),
                stdio=self.stdio,
                timeout=10000,
                database="oceanbase",
            )
        except Exception as e:
            self.stdio.error("Failed to connect to database: {0}".format(e))
            raise OBDIAGFormatException("Failed to connect to database: {0}".format(e))

    def init_config(self):
        self.nodes = self.context.cluster_config['servers']
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
        from_option = Util.get_option(options, 'from')
        to_option = Util.get_option(options, 'to')
        since_option = Util.get_option(options, 'since')
        store_dir_option = Util.get_option(options, 'store_dir')
        files_option = Util.get_option(options, 'files')
        tenant_option = Util.get_option(options, 'tenant')
        queue_option = Util.get_option(options, 'queue')
        if tenant_option is None:
            self.stdio.exception('Error: tenant must input ')
            return False
        self.tenant = tenant_option
        observer_version = self.get_version()
        if StringUtils.compare_versions_greater(observer_version, "4.0.0.0"):
            sql = 'select tenant_id,GROUP_CONCAT(svr_ip ORDER BY svr_ip ) as ip_list from DBA_OB_UNITS where tenant_id=(select tenant_id from DBA_OB_TENANTS where tenant_name="{0}") group by tenant_id'.format(self.tenant)
        else:
            sql = 'select c.tenant_id,GROUP_CONCAT(DISTINCT b.svr_ip ORDER BY b.svr_ip) AS ip_list FROM __all_resource_pool a JOIN __all_unit b ON a.resource_pool_id = b.resource_pool_id JOIN __all_tenant c ON a.tenant_id = c.tenant_id WHERE c.tenant_name ="{0}"'.format(
                self.tenant
            )
        self.stdio.verbose("sql is {0}".format(sql))
        sql_result = self.obconn.execute_sql_return_cursor_dictionary(sql).fetchall()
        if len(sql_result) <= 0:
            self.stdio.exception('Error: tenant is {0} not  in this cluster '.format(tenant_option))
            return False
        self.stdio.verbose("sql_result is {0}".format(sql_result))
        for row in sql_result:
            self.tenant_id = row["tenant_id"]
            self.ip_list = row["ip_list"]
        self.stdio.verbose("tenant_id is {0}".format(self.tenant_id))
        self.stdio.verbose("ip_list is {0}".format(self.ip_list))
        self.queue = queue_option
        self.scope = "observer"
        # if files_option:
        #     self.is_ssh = False
        #     self.directly_analyze_files = True
        #     self.analyze_files_list = files_option
        if from_option is not None and to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(from_option)
                to_timestamp = TimeUtils.parse_time_str(to_option)
                self.from_time_str = from_option
                self.to_time_str = to_option
            except OBDIAGFormatException:
                self.stdio.exception('Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. from_datetime={0}, to_datetime={1}'.format(from_option, to_option))
                return False
            if to_timestamp <= from_timestamp:
                self.stdio.exception('Error: from datetime is larger than to datetime, please check.')
                return False
        elif (from_option is None or to_option is None) and since_option is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            if not self.directly_analyze_files:
                self.stdio.print('analyze log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.print('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option is not None:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            if not self.directly_analyze_files:
                self.stdio.print('analyze log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option is not None:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        return True

    def get_version(self):
        observer_version = ""
        try:
            observer_version = get_observer_version_by_sql(self.ob_cluster, self.stdio)
        except Exception as e:
            self.stdio.warn("AnalyzeHandler Failed to get observer version:{0}".format(e))
        self.stdio.verbose("AnalyzeHandler.init get observer version: {0}".format(observer_version))
        return observer_version

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        local_store_parent_dir = os.path.join(self.gather_pack_dir, "obdiag_analyze_pack_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp())))
        self.stdio.verbose("Use {0} as pack dir.".format(local_store_parent_dir))
        analyze_tuples = []

        def handle_from_node(node):
            node_results = self.__handle_from_node(node, local_store_parent_dir)
            analyze_tuples.append((node.get("ip"), node_results))

        if self.is_ssh:
            nodes_new = []
            for node in self.nodes:
                if node["ip"] in self.ip_list:
                    nodes_new.append(node)
            self.nodes = nodes_new
            for node in self.nodes:
                handle_from_node(node)
        self.stdio.print(analyze_tuples)
        table_data = []
        headers = ['IP', 'Tenant Name', 'From_TimeStamp', 'To_TimeStamp', 'Is Queue', 'Queue Limit', 'Over Queue Limit Count', 'Max Queue']
        for ip, info in analyze_tuples:
            row = [ip, info['tenant_name'], info['from_datetime_timestamp'], info['to_datetime_timestamp'], info['is_queue'], info['queue_limit'], info['over_queue_limit'], info['max_queue']]
            table_data.append(row)
        queue_result = tabulate(table_data, headers=headers, tablefmt="pretty")
        self.stdio.print(queue_result)
        FileUtil.write_append(os.path.join(local_store_parent_dir, "result_details.txt"), str(queue_result))
        return queue_result

    def __handle_from_node(self, node, local_store_parent_dir):
        ssh_client = SshClient(self.context, node)
        try:
            node_results = []
            queue_limit = self.queue
            result_dict = {}
            remote_ip = node.get("ip") if self.is_ssh else '127.0.0.1'
            remote_user = node.get("ssh_username")
            remote_password = node.get("ssh_password")
            remote_port = node.get("ssh_port")
            remote_private_key = node.get("ssh_key_file")
            remote_home_path = node.get("home_path")
            self.stdio.verbose("Sending Collect Shell Command to node {0} ...".format(remote_ip))
            DirectoryUtil.mkdir(path=local_store_parent_dir, stdio=self.stdio)
            local_store_dir = "{0}/{1}".format(local_store_parent_dir, ssh_client.get_name())
            DirectoryUtil.mkdir(path=local_store_dir, stdio=self.stdio)
        except Exception as e:
            ssh_failed = True
            raise Exception("Please check the {0}".format(self.config_path))

        from_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.from_time_str))
        to_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.to_time_str))
        gather_dir_name = "ob_log_{0}_{1}_{2}".format(ssh_client.get_name(), from_datetime_timestamp, to_datetime_timestamp)
        gather_dir_full_path = "{0}/{1}".format("/tmp", gather_dir_name)
        mkdir(ssh_client, gather_dir_full_path)

        log_list = self.__handle_log_list(ssh_client, node)
        self.stdio.print(FileUtil.show_file_list_tabulate(remote_ip, log_list, self.stdio))
        for log_name in log_list:
            if self.directly_analyze_files:
                self.__pharse_offline_log_file(ssh_client, log_name=log_name, local_store_dir=local_store_dir)
                analyze_log_full_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
            else:
                self.__pharse_log_file(ssh_client, node=node, log_name=log_name, gather_path=gather_dir_full_path, local_store_dir=local_store_dir)
                analyze_log_full_path = "{0}/{1}".format(local_store_dir, log_name)
            self.stdio.start_loading('analyze log start')
            file_result = self.__parse_log_lines(analyze_log_full_path)
            self.stdio.stop_loading('analyze log sucess')
            node_results.append(file_result)
        delete_file(ssh_client, gather_dir_full_path, self.stdio)
        ssh_client.ssh_close()
        self.stdio.print(node_results)
        count, max_queue_value = self.count_and_find_max_queues(node_results, queue_limit)
        self.stdio.print(count)
        self.stdio.print(max_queue_value)
        result_dict['tenant_name'] = self.tenant
        if max_queue_value > queue_limit:
            result_dict['is_queue'] = 'yes'
        else:
            result_dict['is_queue'] = 'no'
        result_dict['queue_limit'] = queue_limit
        result_dict['over_queue_limit'] = count
        result_dict['max_queue'] = max_queue_value
        result_dict['from_datetime_timestamp'] = from_datetime_timestamp
        result_dict['to_datetime_timestamp'] = to_datetime_timestamp
        self.stdio.print(result_dict)
        return result_dict

    def count_and_find_max_queues(self, data, queue_limit):
        count = 0
        max_queue_value = 0
        for sublist in data:
            for item in sublist:
                for key, value in item.items():
                    if 'queue' in key:
                        value = int(value)
                        if value > queue_limit:
                            count += 1
                            if value > max_queue_value:
                                max_queue_value = value

        return count, max_queue_value

    def __handle_log_list(self, ssh_client, node):
        if self.directly_analyze_files:
            log_list = self.__get_log_name_list_offline()
        else:
            log_list = self.__get_log_name_list(ssh_client, node)
        if len(log_list) > self.file_number_limit:
            self.stdio.warn("{0} The number of log files is {1}, out of range (0,{2}]".format(node.get("ip"), len(log_list), self.file_number_limit))
            return log_list
        elif len(log_list) == 0:
            self.stdio.warn("{0} The number of log files is {1}, No files found, " "Please adjust the query limit".format(node.get("ip"), len(log_list)))
            # resp["skip"] = (True,)
            # resp["error"] = "No files found"
            return log_list
        return log_list

    def __get_log_name_list(self, ssh_client, node):
        """
        :param ssh_client:
        :return: log_name_list
        """
        home_path = node.get("home_path")
        log_path = os.path.join(home_path, "log")
        get_oblog = "ls -1 -F %s/*%s.log* | grep -E 'observer.log(\.[0-9]+){0,1}$' | grep -v 'wf'|awk -F '/' '{print $NF}'" % (log_path, self.scope)
        # get_oblog = "ls -1 -F %s/*%s.log* | awk -F '/' '{print $NF}'" % (log_path, self.scope)
        log_name_list = []
        log_files = ssh_client.exec_cmd(get_oblog)
        if log_files:
            log_name_list = get_logfile_name_list(ssh_client, self.from_time_str, self.to_time_str, log_path, log_files, self.stdio)
        else:
            self.stdio.error("Unable to find the log file. Please provide the correct --ob_install_dir, the default is [/home/admin/oceanbase]")
        return log_name_list

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

    def __pharse_log_file(self, ssh_client, node, log_name, gather_path, local_store_dir):
        home_path = node.get("home_path")
        log_path = os.path.join(home_path, "log")
        local_store_path = "{0}/{1}".format(local_store_dir, log_name)
        obs_log_path = "{0}/{1}".format(log_path, log_name)
        gather_log_path = "{0}/{1}".format(gather_path, log_name)
        self.stdio.verbose("obs_log_path {0}".format(obs_log_path))
        self.stdio.verbose("gather_log_path {0}".format(gather_log_path))
        self.stdio.verbose("local_store_path {0}".format(local_store_path))
        self.stdio.verbose("log_name {0}".format(log_name))
        pattern = "dump tenant info(tenant={id:tenant_id,"
        search_pattern = pattern.replace("{id:tenant_id,", f"{{id:{self.tenant_id},")
        search_pattern = '"' + search_pattern + '"'
        self.stdio.verbose("search_pattern = [{0}]".format(search_pattern))
        command = ['grep', search_pattern, obs_log_path]
        grep_cmd = ' '.join(command) + f' >> {gather_log_path}'
        self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
        ssh_client.exec_cmd(grep_cmd)
        log_full_path = "{gather_path}/{log_name}".format(log_name=log_name, gather_path=gather_path)
        download_file(ssh_client, log_full_path, local_store_path, self.stdio)

    def __pharse_offline_log_file(self, ssh_client, log_name, local_store_dir):
        """
        :param ssh_helper, log_name
        :return:
        """

        ssh_client = ssh_client_local_client.LocalClient(context=self.context, node={"ssh_type": "local"})
        local_store_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
        grep_cmd = "grep -e 'dump tenant info(tenant={id:{tenant_id},' {log_name} >> {local_store_path} ".format(tenant_id=self.tenant_id, log_name=log_name, local_store_path=local_store_path)
        self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
        ssh_client.exec_cmd(grep_cmd)

    def __parse_log_lines(self, file_full_path):
        """
        Process the observer's log line by line
        """
        log_lines = []
        with open(file_full_path, 'r', encoding='utf8', errors='ignore') as file:
            for line in file:
                log_lines.append(line.strip())
        pattern_timestamp = r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]'
        pattern_req_queue = r'req_queue:total_size=(\d+)'
        pattern_multi_level_queue = r'multi_level_queue:total_size=(\d+)'
        pattern_group_id = r'group_id = (\d+),queue_size = (\d+)'

        # get group_id
        all_group_ids = set()
        for log in log_lines:
            matches = re.findall(pattern_group_id, log)
            for match in matches:
                all_group_ids.add(int(match[0]))

        results = []
        group_id_columns = {f'group_id_{gid}_queue_size': 'NA' for gid in all_group_ids}

        for log in log_lines:
            timestamp = re.search(pattern_timestamp, log).group(1)
            req_queue_size = re.search(pattern_req_queue, log).group(1) if re.search(pattern_req_queue, log) else 'NA'
            multi_level_queue_size = re.search(pattern_multi_level_queue, log).group(1) if re.search(pattern_multi_level_queue, log) else 'NA'

            group_info = {}
            matches = re.findall(pattern_group_id, log)
            for match in matches:
                group_id, queue_size = match
                group_info[f'group_id_{group_id}_queue_size'] = queue_size

            result = {
                'timestamp': timestamp,
                'req_queue_total_size': req_queue_size,
                'multi_level_queue_total_size': multi_level_queue_size,
                **group_info,
                **{k: 'NA' for k in group_id_columns if k not in group_info},
            }

            results.append(result)
        return results

    def __get_time_from_ob_log_line(self, log_line):
        """
        Get the time from the observer's log line
        :param log_line
        :return: time_str
        """
        time_str = ""
        if len(log_line) >= 28:
            time_str = log_line[1 : log_line.find(']')]
        return time_str

    def __get_trace_id(self, log_line):
        """
        Get the trace_id from the observer's log line
        :param log_line
        :return: trace_id
        """
        pattern = re.compile(r'\[Y(.*?)\]')
        find = pattern.search(log_line)
        if find and find.group(1):
            return find.group(1).strip('[').strip(']')

    def __get_log_level(self, log_line):
        """
        Get the log level from the observer's log line
        :param log_line
        :return: log level
        """
        level_lits = ["DEBUG ", "TRACE ", "INFO ", "WDIAG ", "WARN ", "EDIAG ", "ERROR ", "FATAL "]
        length = len(log_line)
        if length > 38:
            length = 38
        for level in level_lits:
            idx = log_line[:length].find(level)
            if idx != -1:
                return OBLogLevel().get_log_level(level.rstrip())
        return 0

    @staticmethod
    def __get_overall_summary(node_summary_tuples, is_files=False):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple
        :return: a string indicating the overall summary
        """
        field_names = ["Node", "Status", "FileName", "ErrorCode", "Message", "Count"]
        t = []
        t_details = []
        field_names_details = field_names
        field_names_details.extend(["Cause", "Solution", "First Found Time", "Last Found Time", "Trace_IDS"])
        for tup in node_summary_tuples:
            is_empty = True
            node = tup[0]
            is_err = tup[2]
            node_results = tup[3]
            if is_err:
                is_empty = False
                t.append([node, "Error:" + tup[2] if is_err else "Completed", None, None, None, None])
                t_details.append([node, "Error:" + tup[2] if is_err else "Completed", None, None, None, None, None, None, None, None, None])
            for log_result in node_results:
                for ret_key, ret_value in log_result.items():
                    if ret_key is not None:
                        error_code_info = OB_RET_DICT.get(ret_key, "")
                        if len(error_code_info) > 3:
                            is_empty = False
                            t.append([node, "Error:" + tup[2] if is_err else "Completed", ret_value["file_name"], ret_key, error_code_info[1], ret_value["count"]])
                            t_details.append(
                                [
                                    node,
                                    "Error:" + tup[2] if is_err else "Completed",
                                    ret_value["file_name"],
                                    ret_key,
                                    error_code_info[1],
                                    ret_value["count"],
                                    error_code_info[2],
                                    error_code_info[3],
                                    ret_value["first_found_time"],
                                    ret_value["last_found_time"],
                                    str(ret_value["trace_id_list"]),
                                ]
                            )
            if is_empty:
                t.append([node, "\033[32mPASS\033[0m", None, None, None, None])
                t_details.append([node, "\033[32mPASS\033[0m", None, None, None, None, None, None, None, None, None])
        title = "\nAnalyze OceanBase Offline Log Summary:\n" if is_files else "\nAnalyze OceanBase Online Log Summary:\n"
        t.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=False)
        t_details.sort(key=lambda x: (x[0], x[1], x[2], x[3]), reverse=False)
        return title, field_names, t, t_details
