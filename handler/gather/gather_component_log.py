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
import datetime
import os
import threading
import uuid

from prettytable import PrettyTable
from common.command import get_file_start_time, get_file_size, is_empty_dir
from common.constant import const
from common.ssh_client.ssh import SshClient
from common.tool import FileUtil, TimeUtils, Util
from handler.base_shell_handler import BaseShellHandler
from handler.gather.plugins.redact import Redact
from result_type import ObdiagResult


class GatherComponentLogHandler(BaseShellHandler):
    # log_scope_list
    log_scope_list = {"observer": ["observer", "rootservice", "election"], "obproxy": ["obproxy", "obproxy_digest", "obproxy_stat", "obproxy_slow", "obproxy_limit"], "oms": ["connector", "error"]}

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.oms_module_id = None
        self.zip_encrypt = None
        self.redact_dir = None
        self.gather_log_conf_dict = None
        self.thread_nums = None
        self.oms_log_path = None
        self.is_scene = None
        self.inner_config = None
        self.stdio = None
        self.context = None
        self.target = None
        self.from_option = None
        self.to_option = None
        self.since_option = None
        self.scope = None
        self.grep = None
        self.encrypt = None
        self.store_dir = None
        self.temp_dir = None
        self.redact = None
        self.nodes = None

    def init(self, context, *args, **kwargs):
        try:
            self.context = context
            self.stdio = self.context.stdio
            self.inner_config = self.context.inner_config
            self.target = kwargs.get('target', None)
            self.from_option = kwargs.get('from', None)
            self.to_option = kwargs.get('to', None)
            self.since_option = kwargs.get('since', None)
            self.scope = kwargs.get('scope', None)
            self.grep = kwargs.get('grep', None)
            self.encrypt = kwargs.get('encrypt', None)
            self.store_dir = kwargs.get('store_dir', None)
            self.temp_dir = kwargs.get('temp_dir', None)
            self.redact = kwargs.get('redact', None)
            self.nodes = kwargs.get('nodes', None)
            self.is_scene = kwargs.get('is_scene', False)
            self.oms_log_path = kwargs.get('oms_log_path', None)
            self.thread_nums = kwargs.get('thread_nums', 3)
            self.zip_encrypt = kwargs.get('zip_encrypt', False)
            self.oms_module_id = kwargs.get('oms_module_id', None)
            self._check_option()
            # build config dict for gather log on node
            self.gather_log_conf_dict = {"target": self.target, "tmp_dir": const.GATHER_LOG_TEMPORARY_DIR_DEFAULT, "zip_password": self.zip_password}
        except Exception as e:
            self.stdio.error("init GatherComponentLogHandler failed, error: {0}".format(str(e)))
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, "init GatherComponentLogHandler failed, error: {0}".format(str(e)))

    def _check_option(self):
        # target check
        if self.target is None or self.target == "":
            self.target = 'observer'
        else:
            self.target = self.target.lower().strip()
        if not isinstance(self.target, str):
            raise Exception("target option can only be string")
        self.target = self.target.lower().strip()
        if self.target != 'observer' and self.target != 'obproxy' and self.target != 'oms':
            raise Exception("target option can only be observer or obproxy or oms")

        # check store_dir
        if not os.path.exists(self.store_dir):
            raise Exception("store_dir: {0} is not exist".format(self.store_dir))
        if self.is_scene is False:
            target_dir = 'obdiag_gather_{0}'.format(self.target)
            self.store_dir = os.path.join(self.inner_config.get("store_dir"), target_dir)
            if not os.path.exists(self.store_dir):
                os.makedirs(self.store_dir)
        self.stdio.verbose("store_dir rebase: {0}".format(self.store_dir))

        # check nodes
        if self.nodes is None or len(self.nodes) == 0:
            # if self.nodes not input, use default nodes by self.target
            if self.target == 'observer':
                self.nodes = self.context.cluster_config.get("servers")
            elif self.target == 'obproxy':
                self.nodes = self.context.obproxy_config.get("servers")
            elif self.target == 'oms':
                self.nodes = self.context.oms_config.get("servers")
            else:
                raise Exception("can not get nodes by target: {0}".format(self.target))
        if len(self.nodes) == 0:
            raise Exception("can not get nodes by target: {0}, nodes's len is 0.".format(self.target))
        # build ssh_client for every node
        new_nodes = []
        for node in self.nodes:
            new_node = node
            ssh_client = SshClient(self.context, node)
            new_node["ssh_client"] = ssh_client
            new_nodes.append(new_node)
        self.nodes = new_nodes
        # check scope
        if self.scope is None or self.scope == "" or self.scope == "all":
            self.scope = "all"
            self.scope = self.log_scope_list[self.target]
        else:
            self.scope = self.scope.strip()
            if self.scope not in self.log_scope_list[self.target]:
                raise Exception("scope option can only be {0},the {1} just support {2}".format(self.scope, self.target, self.log_scope_list))
        # check encrypt
        if self.zip_encrypt:
            self.zip_password = Util.gen_password(16)
            self.stdio.verbose("zip_encrypt is True, zip_password is {0}".format(self.zip_password))
        # check redact
        if self.redact:
            if self.redact != "" and len(self.redact) != 0:
                if "," in self.redact and isinstance(self.redact, str):
                    self.redact = self.redact.split(",")
                else:
                    self.redact = [self.redact]

        # check inner_config
        if self.inner_config is None:
            self.file_number_limit = 20
            self.file_size_limit = 2 * 1024 * 1024 * 1024
        else:
            basic_config = self.inner_config['obdiag']['basic']
            self.file_number_limit = int(basic_config["file_number_limit"])
            self.file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
            self.config_path = basic_config['config_path']
        self.stdio.verbose("file_number_limit: {0}, file_size_limit: {1}, gather log config_path: {2}".format(self.file_number_limit, self.file_size_limit, self.config_path))

        # check thread_nums
        if self.thread_nums is None or not isinstance(self.thread_nums, int) or self.thread_nums <= 0:
            self.thread_nums = int(self.context.inner_config.get("obdiag", {}).get("gather", {}).get("thread_nums") or 3)
        self.stdio.verbose("thread_nums: {0}".format(self.thread_nums))

    def handler(self):
        try:
            # run on every node
            def run_on_node(context, conf_dict, node, pool_sema, gather_tuple):
                with pool_sema:
                    try:
                        task = GatherLogOnNode(context, node, conf_dict, pool_sema, gather_tuple)
                        task.handle()
                    except Exception as e:
                        self.stdio.exception(e)
                        self.stdio.error("gather log failed: {0}".format(str(e)))
                        return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="gather log failed: {0}".format(str(e)))

            self.stdio.start_loading("gather start")
            pool_sema = threading.BoundedSemaphore(value=self.thread_nums)
            node_threads = []
            gather_tuples = []
            for node in self.nodes:
                next_context = self.context
                next_context.stdio = self.stdio.sub_io()
                gather_tuple = {}
                node_thread = threading.Thread(
                    target=run_on_node,
                    args=(next_context, self.gather_log_conf_dict, node, pool_sema, gather_tuple),
                )
                node_thread.start()
                node_threads.append(node_threads)
                gather_tuples.append(gather_tuple)
            for node_thread in node_threads:
                node_thread.join()
            self.stdio.verbose("gather_tuples: {0}".format(gather_tuples))
            self.stdio.stop_loading("gather successes")
            # save result
            summary_tuples = self.__get_overall_summary(gather_tuples)
            self.stdio.print(summary_tuples)
            with open(os.path.join(self.store_dir, "result_details.txt"), 'a', encoding='utf-8') as fileobj:
                fileobj.write(summary_tuples.get_string())

            last_info = "For result details, please run cmd \033[32m' cat {0} '\033[0m\n".format(os.path.join(self.store_dir, "result_summary.txt"))
            self.stdio.print(last_info)
            try:
                if self.redact and len(self.redact) > 0:
                    self.stdio.start_loading("gather redact start")
                    self.stdio.verbose("redact_option is {0}".format(self.redact))
                    redact_dir = "{0}_redact".format(self.store_dir)
                    self.redact_dir = redact_dir
                    redact = Redact(self.context, self.store_dir, redact_dir, zip_password=self.zip_password)
                    redact.redact_files(self.redact)
                    self.stdio.print("redact success the log save on {0}".format(self.redact_dir))
                    self.stdio.stop_loading("gather redact successes")
                    return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": redact_dir, "redact_dir": self.redact_dir})
            except Exception as e:
                self.stdio.error("redact failed {0}".format(e))
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="redact failed {0}".format(e))
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.store_dir})
        except Exception as e:
            self.stdio.exception(e)
            self.stdio.error("gather log failed: {0}".format(str(e)))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="gather log failed: {0}".format(str(e)))

    def __get_overall_summary(self, node_summary_tuple):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple: (node, is_err, err_msg, size, consume_time, node_summary) for each node
        :return: a string indicating the overall summary
        """
        summary_tb = PrettyTable()
        summary_tb.title = "{0} Gather Ob Log Summary on {1}".format(self.target, datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        if self.zip_password:
            summary_tb.field_names = ["Node", "Status", "Size", "Password"]
        try:
            for tup in node_summary_tuple:
                summary_tb.add_row([tup["node"], tup["success"], tup["info"], tup["file_size"]])
        except Exception as e:
            self.stdio.error("gather log __get_overall_summary failed: {0}".format(str(e)))
        return summary_tb


# if target need add, you should check def about *_by_target
class GatherLogOnNode:
    def __init__(self, context, node, config, pool_sema, gather_tuple):
        self.ssh_client = node["ssh_client"]
        self.context = context
        self.stdio = context.stdio
        self.config = config
        self.node = node
        self.pool_sema = pool_sema
        self.gather_tuple = gather_tuple
        self.target = self.config.get("target")

        # mkdir tmp_dir
        self.tmp_dir = self.config.get("tmp_dir")
        self.tmp_dir = os.path.join(self.tmp_dir, "obdiag_gather_{0}".format(str(uuid.uuid4())))
        self.ssh_client.exec_cmd("mkdir -p {0}".format(self.tmp_dir))

        self.scope = self.config.get("scope")
        # todo log_path for oms
        if self.target == "oms":
            self.log_path = os.path.join(
                node.get("home_path"),
            )
        else:
            self.log_path = os.path.join(node.get("home_path"), "log")

        self.from_time_str = self.config.get("from_time")
        self.to_time_str = self.config.get("to_time")
        self.grep_option = self.config.get("grep_option")
        self.store_dir = self.config.get("store_dir")
        self.zip_password = self.config.get("zip_password")
        #
        self.file_number_limit = self.config.get("file_number_limit")
        self.file_size_limit = self.config.get("file_size_limit")
        self.gather_tuple = {
            "node": self.ssh_client.get_name(),
            "success": False,
            "info": "",
            "file_size": 0,
        }

    def handle(self):

        from_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.from_time_str))
        to_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.to_time_str))

        tmp_log_dir = os.path.join(self.tmp_dir, "ob_log_{0}_{1}_{2}_{3}".format(self.ssh_client.get_name(), from_datetime_timestamp, to_datetime_timestamp, uuid.uuid4()))
        # mkdir tmp_log_dir
        self.ssh_client.exec_cmd("mkdir -p {0}".format(tmp_log_dir))
        self.stdio.verbose("gather_log_on_node {0} tmp_log_dir: {1}".format(self.ssh_client.get_ip(), tmp_log_dir))
        try:
            # find logs
            logs_name = self.__find_logs_name()
            if logs_name is None or len(logs_name) == 0:
                self.stdio.error("gather_log_on_node {0} failed: no log found".format(self.ssh_client.get_ip()))
                self.gather_tuple["info"] = "no log found"
                return
            elif len(logs_name) > self.file_number_limit:
                self.stdio.error('{0} The number of log files is {1}, out of range (0,{2}], ' "Please adjust the query limit".format(self.ssh_client.get_name(), len(logs_name), self.file_number_limit))
                self.gather_tuple["info"] = "too many files {0} > {1}".format(len(logs_name), self.file_number_limit)
                return

            # gather log to remote tmp_dir ,if grep is exit, with grep
            self.__grep_log_to_tmp(logs_name, tmp_log_dir)

            # build tar file
            if is_empty_dir(self.ssh_client, tmp_log_dir, self.stdio):
                # if remote tmp_log_dir is empty, rm the dir and return
                self.ssh_client.exec_cmd("rm -rf {0}".format(tmp_log_dir))
                self.stdio.error("gather_log_on_node {0} failed: tmp_log_dir({1}) no log found".format(self.ssh_client.get_name(), tmp_log_dir))
                self.gather_tuple["info"] = "tmp_log_dir({0}) no log found".format(tmp_log_dir)
                return

            tar_file = os.path.join(self.tmp_dir, "{0}.tar.gz".format(tmp_log_dir))
            tar_cmd = "cd {0} && tar -czf {1}.tar.gz {1}/*".format(self.tmp_dir, tmp_log_dir)
            self.stdio.verbose("gather_log_on_node {0} tar_cmd: {1}".format(self.ssh_client.get_ip(), tar_cmd))
            self.ssh_client.exec_cmd(tar_cmd)

            # download log to local store_dir
            tar_file_size = int(get_file_size(self.ssh_client, tar_file))
            if tar_file_size > self.file_size_limit:
                self.stdio.error("gather_log_on_node {0} failed: File too large over gather.file_size_limit".format(self.ssh_client.get_ip()))
                self.gather_tuple["info"] = "File too large over gather.file_size_limit"
                return
            else:
                self.stdio.verbose("gather_log_on_node {0} download log to local store_dir: {1}".format(self.ssh_client.get_ip(), self.store_dir))
                self.ssh_client.download(tar_file, self.store_dir)

            # tar to zip
            tar_file_name = "{0}.tar.gz".format(tmp_log_dir)
            local_zip_store_path = os.path.join(self.store_dir, "{0}.zip".format(tmp_log_dir))
            FileUtil.tar_gz_to_zip(self.store_dir, tar_file_name, local_zip_store_path, self.zip_password, self.stdio)
            self.gather_tuple["file_size"] = tar_file_size
            self.gather_tuple["info"] = "file save in {0}".format(local_zip_store_path)
            self.gather_tuple["success"] = True
        except Exception as e:
            self.stdio.error("gather_log_on_node {0} failed: {1}".format(self.ssh_client.get_ip(), str(e)))
            self.gather_tuple["info"] = str(e)
        finally:
            self.ssh_client.exec_cmd("rm -rf {0}".format(tmp_log_dir))
            self.stdio.verbose("gather_log_on_node {0} finished".format(self.ssh_client.get_ip()))

    def __grep_log_to_tmp(self, logs_name, tmp_log_dir):
        grep_cmd = ""
        if self.grep_option:
            self.stdio.verbose("grep files, grep_option = [{0}]".format(self.grep_option))
            for grep_option in self.grep_option:
                if grep_cmd == "":
                    grep_cmd = "grep -e '{0}' ".format(grep_option)
                grep_cmd += "| grep -e '{0}'".format(grep_option)
        for log_name in logs_name:
            source_log_name = "{0}/{1}".format(self.log_path, log_name)
            target_log_name = "{0}/{1}".format(tmp_log_dir, log_name)
            self.stdio.verbose("grep files, source_log_name = [{0}], target_log_name = [{1}]".format(source_log_name, target_log_name))
            if log_name.endswith(".gz"):
                log_grep_cmd = "cp {0} {1}".format(source_log_name, target_log_name)
                self.stdio.verbose("grep files, run cmd = [{0}]".format(log_grep_cmd))
                self.ssh_client.exec_cmd(log_grep_cmd)
                continue
            log_grep_cmd = ""
            if grep_cmd == "":
                log_grep_cmd = "cp {0} {1}".format(source_log_name, target_log_name)
            else:
                log_grep_cmd = grep_cmd + " {0}".format(source_log_name)
                log_grep_cmd += " > {0} ".format(target_log_name)
            self.stdio.verbose("grep files, run cmd = [{0}]".format(log_grep_cmd))
            self.ssh_client.exec_cmd(log_grep_cmd)

    def __find_logs_name(self):
        try:
            logs_scope = ""
            for scope in self.scope:
                if logs_scope == "":
                    logs_scope = scope
                    continue
                logs_scope = logs_scope + "|" + scope
            self.stdio.verbose("gather_log_on_node {0} find logs scope: {1}".format(self.ssh_client.get_ip(), logs_scope))

            find_cmd = "ls -1 -F {0} |grep -E '{1}'| awk -F '/' ".format(self.log_path, logs_scope) + "'{print $NF}'"
            self.stdio.verbose("gather_log_on_node {0} find logs cmd: {1}".format(self.ssh_client.get_ip(), find_cmd))
            logs_name = self.ssh_client.exec_cmd(find_cmd)
            if logs_name is not None and len(logs_name) != 0:
                log_name_list = self.__get_logfile_name_list(self.from_time_str, self.to_time_str, self.log_path, logs_name)
            else:
                self.stdio.warn("gather_log_on_node {0} failed: no log found".format(self.ssh_client.get_ip()))
            return logs_name
        except Exception as e:
            raise Exception("gather_log_on_node {0} find logs failed: {1}".format(self.ssh_client.get_ip(), str(e)))

    def __get_logfile_name_list(self, from_time_str, to_time_str, log_dir, log_files):
        # TODO oms get all log file name list
        if self.target == "oms":
            return log_files
        self.stdio.verbose("get log file name list, from time {0}, to time {1}, log dir {2}, log files {3}".format(from_time_str, to_time_str, log_dir, log_files))
        log_name_list = []
        last_file_dict = {"prefix_file_name": "", "file_name": "", "file_end_time": ""}
        for file_name in log_files.split('\n'):
            if file_name == "":
                self.stdio.verbose("existing file name is empty")
                continue
            if not file_name.endswith("log") and not file_name.endswith("wf"):
                file_start_time_str = ""
                prefix_name = file_name[:-14] if len(file_name) > 24 else ""
                file_end_time_str = TimeUtils.filename_time_to_datetime(TimeUtils.extract_filename_time_from_log_name(file_name, self.stdio), self.stdio)
                if last_file_dict["prefix_file_name"] != "" and last_file_dict["prefix_file_name"] == prefix_name:
                    file_start_time_str = last_file_dict["file_end_time"]
                elif last_file_dict["prefix_file_name"] != "" and last_file_dict["prefix_file_name"] != prefix_name:
                    file_start_time_str = ""
                    file_end_time_str = ""
                elif last_file_dict["prefix_file_name"] == "":
                    file_start_time_str = get_file_start_time(self.ssh_client, file_name, log_dir, self.stdio)
                # When two time intervals overlap, need to add the file
                if (file_end_time_str != "") and (file_start_time_str != "") and (file_start_time_str <= to_time_str) and (file_end_time_str >= from_time_str):
                    log_name_list.append(file_name)
                last_file_dict = {"prefix_file_name": prefix_name, "file_name": file_name, "file_end_time": file_end_time_str}
            elif file_name.endswith("log") or file_name.endswith("wf"):
                # Get the first and last lines of text of the file. Here, use a command
                get_first_line_cmd = "head -n 1 {0}/{1} && tail -n 1 {0}/{1}".format(log_dir, file_name)
                first_and_last_line_text = self.ssh_client.exec_cmd(get_first_line_cmd)
                # Split the first and last lines of text
                first_and_last_line_text_list = str(first_and_last_line_text).splitlines()
                if len(first_and_last_line_text_list) >= 2:
                    first_line_text = first_and_last_line_text_list[0]
                    last_line_text = first_and_last_line_text_list[-1]
                    # Time to parse the first and last lines of text
                    file_start_time_str = TimeUtils.extract_time_from_log_file_text(first_line_text, self.stdio)
                    file_end_time = TimeUtils.extract_time_from_log_file_text(last_line_text, self.stdio)
                    self.stdio.verbose("The log file {0} starts at {1} ends at {2}".format(file_name, file_start_time_str, file_end_time))
                    self.stdio.verbose("to_time_str {0} from_time_str {1}".format(to_time_str, from_time_str))
                    if (file_start_time_str <= to_time_str) and (file_end_time >= from_time_str):
                        log_name_list.append(file_name)
        if len(log_name_list) > 0:
            self.stdio.verbose("Find the qualified log file {0} on Server [{1}], " "wait for the next step".format(log_name_list, self.ssh_client.get_ip()))
        else:
            self.stdio.warn("No found the qualified log file on Server [{0}]".format(self.ssh_client.get_name()))
        return log_name_list
