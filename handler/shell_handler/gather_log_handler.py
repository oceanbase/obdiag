#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/23
@file: gather_log_handler.py
@desc:
"""
import datetime
import os
import threading
import time

import tabulate

from handler.base_shell_handler import BaseShellHandler
from common.logger import logger
from common.odg_exception import ODGFormatException
from common.odg_exception import ODGInvalidArgs
from common.odg_exception import ODGSSHConnException
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file
from utils.shell_utils import SshHelper
from utils.password_util import gen_password
from utils.time_utils import parse_time_str
from utils.time_utils import extract_filename_time_from_log_name
from utils.time_utils import extract_time_from_log_file_text
from utils.time_utils import filename_time_to_datetime
from utils.time_utils import parse_time_length_to_sec
from utils.time_utils import timestamp_to_filename_time
from utils.time_utils import datetime_to_timestamp

OB_LOG_DIR_DEFAULT = "/home/admin/oceanbase/log"
GATHER_OB_LOG_TEMPORARY_DIR_DEFAULT = "/tmp"


class GatherLogHandler(BaseShellHandler):
    def __init__(self, nodes, gather_pack_dir, ob_log_dir, gather_timestamp):
        super(GatherLogHandler, self).__init__(nodes)
        self.gather_timestamp = gather_timestamp
        self.ob_log_dir = ob_log_dir
        self.gather_ob_log_temporary_dir = GATHER_OB_LOG_TEMPORARY_DIR_DEFAULT
        self.gather_pack_dir = gather_pack_dir
        self.from_time_str = None
        self.to_time_str = None
        self.grep_args = None
        self.scope = None
        self.zip_encrypt = False

    def handle(self, args):
        # check args first
        if not self.__check_valid_and_parse_args(args):
            raise ODGInvalidArgs("Invalid args, args={0}".format(args))
        # example of the format of pack dir for this command: {gather_pack_dir}/gather_pack_20190610123344
        pack_dir_this_command = os.path.join(self.gather_pack_dir,
                                             "gather_pack_{0}".format(timestamp_to_filename_time(
                                                 self.gather_timestamp)))
        logger.info("Use {0} as pack dir.".format(pack_dir_this_command))
        gather_tuples = []
        gather_pack_path_dict = {}

        def handle_from_node(ip, user, password, port, private_key):
            st = time.time()
            resp = self.__handle_from_node(args, ip, user, password, port, private_key, pack_dir_this_command)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            gather_tuples.append((ip, False, resp["error"],
                                  file_size,
                                  resp["zip_password"],
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
        list(map(lambda x: x.join(), node_threads))

        summary_tuples = self.__get_overall_summary(gather_tuples, self.zip_encrypt)
        print(summary_tuples)
        # Persist the summary results to a file
        write_result_append_to_file(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)

        # When using encryption mode, record the account and password information into the file
        return gather_tuples, gather_pack_path_dict

    def __handle_from_node(self, args, ip, user, password, port, private_key, pack_dir_this_command):
        resp = {
            "skip": False,
            "error": "",
            "zip_password": "",
            "gather_pack_path": ""
        }
        remote_ip = ip
        remote_user = user
        remote_password = password
        remote_port = port
        remote_private_key = private_key
        logger.info(
            "Sending Collect Shell Command to node {0} ...".format(remote_ip))
        mkdir_if_not_exist(pack_dir_this_command)
        try:
            ssh = SshHelper(remote_ip, remote_user, remote_password, remote_port, remote_private_key)
        except Exception as e:
            raise ODGSSHConnException("ssh {0}@{1}: failed, exception:{2} Please check the conf/node_config.json file"
                                      .format(remote_user, remote_ip, e))
        # transform timestamp(in us) to yyyymmddhhmmss (filename_time style)
        from_datetime_timestamp = timestamp_to_filename_time(datetime_to_timestamp(self.from_time_str))
        to_datetime_timestamp = timestamp_to_filename_time(datetime_to_timestamp(self.to_time_str))
        gather_dir_name = "ob_log_{0}_{1}_{2}".format(ssh.host_ip, from_datetime_timestamp, to_datetime_timestamp)
        gather_dir_full_path = "{0}/{1}".format("/tmp", gather_dir_name)
        self.__mkdir_if_not_exist(ssh, gather_dir_full_path)

        log_list, resp = self.__handle_log_list(ssh, ip, resp)
        if resp["skip"]:
            return resp
        for log_name in log_list:
            self.__pharse_log(ssh_helper=ssh, log_name=log_name, gather_path=gather_dir_full_path)
        self.__rm_empty_file(ssh, gather_dir_full_path)

        is_empty_dir = self.__is_empty_dir(ssh, gather_dir_full_path)
        if is_empty_dir:
            resp["error"] = "Empty file"
            resp["zip_password"] = ""
            self.__rm_log(ssh, gather_dir_full_path)
        else:
            self.__handle_zip_file(ip, ssh, resp, gather_dir_name, pack_dir_this_command)
        ssh.ssh_close()
        return resp

    def __handle_log_list(self, ssh, ip, resp):
        log_list = self.__get_log_name(ssh)
        if len(log_list) > 20:
            logger.warn(
                "{0} The number of log files is {1}, out of range (0,20], "
                "Please adjust the query limit".format(ip, len(log_list)))
            resp["skip"] = True,
            resp["error"] = "Too many files {0} > 20".format(len(log_list))
            return log_list, resp
        elif len(log_list) <= 0:
            logger.warn(
                "{0} The number of log files is {1}, out of range (0,20], "
                "Please adjust the query limit".format(ip, len(log_list)))
            resp["skip"] = True,
            resp["error"] = "No files found"
            return log_list, resp
        return log_list, resp

    def __get_log_name(self, ssh_helper):
        """
        通过传入的from to的时间来过滤一遍文件列表，提取出初步满足要求的文件列表
        :param ssh_helper:
        :return: list
        """
        if self.scope == "observer" or self.scope == "rootservice" or self.scope == "election":
            get_oblog = "ls -1 -F %s/*%s.log* | awk -F '/' '{print $NF}'" % (self.ob_log_dir, self.scope)
        else:
            get_oblog = "ls -1 -F %s/observer.log* %s/rootservice.log* %s/election.log* | awk -F '/' '{print $NF}'" % \
                        (self.ob_log_dir, self.ob_log_dir, self.ob_log_dir)
        log_files = ssh_helper.ssh_exec_cmd(get_oblog)
        log_name_list = []
        for file_name in log_files.split('\n'):
            if file_name == "":
                logger.warn("existing file name is empty")
                continue
            # 当日志文件的时间小于 self.from_time_str 的日志文件，不被加入到备选日志文件中
            # 当日志文件的时间大于 self.to_time_str 的日志文件，不被加入到备选日志文件中
            if not file_name.endswith("log") and not file_name.endswith("wf"):
                file_end_time_str = filename_time_to_datetime(extract_filename_time_from_log_name(file_name))
                if (file_end_time_str != '') and (
                        (file_end_time_str < self.from_time_str) or (file_end_time_str > self.to_time_str)):
                    pass
                else:
                    logger.info("Filter file %s on server %s", file_name, ssh_helper.host_ip)
                    get_first_line_cmd = "head -n 1 {0}/{1}".format(self.ob_log_dir, file_name)
                    first_line_text = ssh_helper.ssh_exec_cmd(get_first_line_cmd)
                    file_start_time_str = extract_time_from_log_file_text(first_line_text)
                    # 当第一行日志的时间大于 self.to_time_str 的日志文件，不被加入到备选日志文件中
                    if (file_end_time_str != '') and not (file_start_time_str > self.to_time_str):
                        log_name_list.append(file_name)
            elif file_name.endswith("log") or file_name.endswith("wf"):
                logger.info("Filter online file %s on server %s", file_name, ssh_helper.host_ip)
                # 获取文件的首末行文本，此处通过一条命令搞定
                get_first_line_cmd = "head -n 1 {0}/{1} && tail -n 1 {0}/{1}".format(self.ob_log_dir, file_name)
                first_and_last_line_text = ssh_helper.ssh_exec_cmd(get_first_line_cmd)

                # 拆分首末行文本
                first_and_last_line_text_list = str(first_and_last_line_text).splitlines()
                if len(first_and_last_line_text_list) >= 2:
                    first_line_text = first_and_last_line_text_list[0]
                    last_line_text = first_and_last_line_text_list[-1]

                    # 解析首末行文本的时间
                    file_start_time_str = extract_time_from_log_file_text(first_line_text)
                    file_end_time = extract_time_from_log_file_text(last_line_text)
                    if file_start_time_str >= self.from_time_str:
                        log_name_list.append(file_name)
                    elif (file_start_time_str < self.from_time_str) and (file_end_time > self.from_time_str):
                        log_name_list.append(file_name)
        if len(log_name_list):
            logger.info("Find the qualified log file {0} on Server [{1}], "
                        "wait for the next step".format(log_name_list, ssh_helper.host_ip))
        else:
            logger.warn("Failed to find the qualified log file on Server [{0}], "
                        "please check whether the input parameters are correct. ".format(ssh_helper.host_ip))
        return log_name_list

    def __pharse_log(self, ssh_helper, log_name, gather_path):
        """
        处理传入的日志文件，将满足条件的日志文件归集到一起
        :param ssh_helper, log_name, gather_path
        :return:
        """
        if self.grep_args is not None:
            grep_cmd = "grep {grep_args} {log_dir}/{log_name} >> {gather_path}/{log_name} ".format(
                grep_args=self.grep_args,
                gather_path=gather_path,
                log_name=log_name,
                log_dir=self.ob_log_dir)
            logger.info("Start grep files {0} on server {1}".format(log_name, ssh_helper.host_ip))
            logger.debug("grep files, run cmd = [{0}]".format(grep_cmd))
            ssh_helper.ssh_exec_cmd(grep_cmd)
        else:
            cp_cmd = "cp {log_dir}/{log_name} {gather_path}/{log_name} ".format(
                gather_path=gather_path,
                log_name=log_name,
                log_dir=self.ob_log_dir)
            logger.info("Start copy files {0} on server {1}".format(log_name, ssh_helper.host_ip))
            logger.debug("copy files, run cmd = [{0}]".format(cp_cmd))
            ssh_helper.ssh_exec_cmd(cp_cmd)

    def __tar_dir(self, ssh_helper, gather_path, gather_name):
        tar_cmd = "tar --remove-files -zcvf {gather_path}.tar.gz " \
                  "-C {log_dir} {gather_name}".format(gather_path=gather_path, log_dir=self.ob_log_dir,
                                                      gather_name=gather_name)
        logger.debug("compress dir, run cmd = [{0}]".format(tar_cmd))
        ssh_helper.ssh_exec_cmd(tar_cmd)

    def __zip_dir(self, ssh_helper, gather_path, gather_name):
        zip_cmd = "cd {gather_ob_log_stored} && zip {gather_name}.zip -rm {gather_name}".format(
            gather_ob_log_stored=self.gather_ob_log_temporary_dir,
            gather_path=gather_path,
            gather_name=gather_name)
        logger.info("compress dir, run cmd = [{0} on server {1}]".format(zip_cmd, ssh_helper.host_ip))
        logger.info("Please wait a moment ...")
        ssh_helper.ssh_exec_cmd(zip_cmd)

    def __handle_zip_file(self, ip, ssh, resp, gather_dir_name, pack_dir_this_command):
        zip_password = ""
        gather_dir_full_path = "{0}/{1}".format(self.gather_ob_log_temporary_dir, gather_dir_name)
        if self.zip_encrypt:
            zip_password = gen_password(16)
            self.__zip_encrypt_dir(ssh, zip_password, gather_dir_full_path, gather_dir_name)
        else:
            self.__zip_dir(ssh, gather_dir_full_path, gather_dir_name)
        gather_package_dir = "{0}.zip".format(gather_dir_full_path)

        gather_log_file_size = self.__get_zip_file_size(ssh, gather_package_dir)
        print(self.__show_zip_file_size(ip, gather_log_file_size))
        local_path = ""
        if int(gather_log_file_size) < 2 * 1024 * 1024 * 1024:
            local_path = self.__scp_log(ssh, gather_package_dir, pack_dir_this_command)
            resp["error"] = ""
            resp["zip_password"] = zip_password
        else:
            resp["error"] = "File too large"
            resp["zip_password"] = ""
        self.__rm_log(ssh, gather_package_dir)
        resp["gather_pack_path"] = local_path

        logger.debug(
            "Collect pack gathered from node {0}: stored in {1}".format(ip, gather_package_dir))
        return resp

    def __zip_encrypt_dir(self, ssh_helper, zip_password, gather_path, gather_dir_name):
        """
        Compress files by encryption
        :param args: ssh helper, password, gather path, gather dir name
        :return:
        """
        zip_cmd = "cd {gather_ob_log_stored} && zip --password {zip_password} {gather_name}.zip -rm {gather_name}".format(
            zip_password=zip_password,
            gather_ob_log_stored=self.gather_ob_log_temporary_dir,
            gather_path=gather_path,
            gather_name=gather_dir_name)
        logger.info("compress dir, run cmd = [{0}]".format(zip_cmd))
        ssh_helper.ssh_exec_cmd(zip_cmd)

    def __check_valid_and_parse_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        # 1: to timestamp must be larger than from timestamp, and be valid
        if getattr(args, "from") is not None and getattr(args, "to") is not None:
            try:
                from_timestamp = parse_time_str(getattr(args, "from"))
                to_timestamp = parse_time_str(getattr(args, "to"))
                self.from_time_str = getattr(args, "from")
                self.to_time_str = getattr(args, "to")
            except ODGFormatException:
                logger.error("Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. " \
                             "from_datetime={0}, to_datetime={1}".format(getattr(args, "from"), getattr(args, "to")))
                return False
            if to_timestamp <= from_timestamp:
                logger.error("Error: from datetime is larger than to datetime, please check.")
                return False
        elif (getattr(args, "from") is None or getattr(args, "to") is None) and args.since is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(
                seconds=parse_time_length_to_sec(args.since))).strftime('%Y-%m-%d %H:%M:%S')
        else:
            raise ODGInvalidArgs(
                "Invalid args, you need input since or from and to datetime, args={0}".format(args))
        # 2: store_dir must exist, else return "No such file or directory".
        if args.store_dir is not None and not os.path.exists(os.path.abspath(args.store_dir)):
            logger.error("Error: Set store dir {0} failed: No such directory.".format(os.path.abspath(args.store_dir)))
            return False
        if getattr(args, "grep") is not None:
            self.grep_args = getattr(args, "grep")[0]
        if getattr(args, "scope") is not None:
            self.scope = getattr(args, "scope")[0]
        # if user indicates the store_dir, use it, otherwise use the dir in the config(default)
        if args.store_dir is not None:
            self.gather_pack_dir = os.path.abspath(args.store_dir)
        if getattr(args, "encrypt")[0] == "true":
            self.zip_encrypt = True
        if getattr(args, "ob_log_dir") is not None:
            self.ob_log_dir = getattr(args, "ob_log_dir")
        else:
            self.ob_log_dir = OB_LOG_DIR_DEFAULT
        return True

    @staticmethod
    def __mkdir_if_not_exist(ssh_helper, dir_path):
        """
        Create a folder when it does not exist
        :param args: ssh helper, folder path
        :return:
        """
        mkdir_cmd = "mkdir -p {0}".format(dir_path)
        logger.debug("make dir if not exist,run cmd = [{0}]".format(mkdir_cmd))
        ssh_helper.ssh_exec_cmd(mkdir_cmd)

    @staticmethod
    def __scp_log(ssh_helper, gather_dir, pack_dir_this_command):
        """
        scp remote file log to local
        :param args: ssh helper, gather path, Directory where the command is executed
        :return: local path
        """
        logger.info("Start download dir[{0}],on server[{1}] to[{2}] on server {3}".format(gather_dir,
                                                                                          ssh_helper.host_ip,
                                                                                          pack_dir_this_command,
                                                                                          ssh_helper.host_ip))
        package_name = gather_dir[gather_dir.rfind('/'):]
        local_path = "{0}{1}".format(pack_dir_this_command, package_name)
        ssh_helper.download(gather_dir, local_path)
        logger.info("Please wait a moment ...")
        return local_path

    @staticmethod
    def __rm_log(ssh_helper, gather_dir):
        """
        delete file
        :param args: ssh helper, gather log full path
        :return:
        """
        rm_cmd = "rm -rf {0}".format(gather_dir)
        logger.info("To clear file[{0}] on server[{1}], use the command[{2}]".format(gather_dir, ssh_helper.host_ip,
                                                                                     rm_cmd))
        ssh_helper.ssh_exec_cmd(rm_cmd)

    @staticmethod
    def __rm_empty_file(ssh_helper, gather_path):
        """
        delete empty folder
        :param args: ssh helper, gather log full path
        :return:
        """
        rm_cmd = "find  {gather_path} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(gather_path=gather_path)
        logger.info("To clear empty files on server {0}".format(ssh_helper.host_ip))
        ssh_helper.ssh_exec_cmd(rm_cmd)

    @staticmethod
    def __is_empty_dir(ssh_helper, gather_path):
        """
        determine whether it is an empty folder
        :param args: ssh helper, gather log full path
        :return: true or false
        """
        cmd = "ls -A {gather_path}|wc -w".format(gather_path=gather_path)
        file_num = ssh_helper.ssh_exec_cmd(cmd)
        if int(file_num) == 0:
            return True
        else:
            return False

    @staticmethod
    def __get_zip_file_size(ssh_helper, gather_dir):
        """
        get the size of the compressed file
        :param args: ssh helper, gather log full path
        :return: file size
        """
        get_zip_file_size_cmd = "ls -nl %s | awk '{print $5}'" % gather_dir
        file_size = ssh_helper.ssh_exec_cmd(get_zip_file_size_cmd)
        return file_size

    @staticmethod
    def __show_zip_file_size(ip, file_size):
        """
        show the size of the compressed file
        :param args: remote host ip, file size
        :return: zip file info
        """
        format_file_size = size_format(int(file_size), output_str=True)
        summary_tab = []
        field_names = ["Node", "LogSize"]
        summary_tab.append((ip, format_file_size))
        return "\nZipFileInfo:\n" + \
               tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)

    @staticmethod
    def __get_overall_summary(node_summary_tuple, is_zip_encrypt):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple: (node, is_err, err_msg, size, consume_time, node_summary) for each node
        :return: a string indicating the overall summary
        """
        summary_tab = []
        field_names = ["Node", "Status", "Size"]
        if is_zip_encrypt:
            field_names.append("Password")
        field_names.append("Time")
        field_names.append("PackPath")
        for tup in node_summary_tuple:
            node = tup[0]
            is_err = tup[2]
            file_size = tup[3]
            consume_time = tup[5]
            pack_path = tup[6]
            try:
                format_file_size = size_format(file_size, output_str=True)
            except:
                format_file_size = size_format(0, output_str=True)
            if is_zip_encrypt:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed",
                                    format_file_size, tup[4], "{0} s".format(int(consume_time)), pack_path))
            else:
                summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed",
                                    format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather Ob Log Summary:\n" + \
               tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
