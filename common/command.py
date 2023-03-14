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
@time: 2023/02/08
@file: command.py
@desc:
"""
import re

from common.logger import logger
from utils.time_utils import extract_time_from_log_file_text, filename_time_to_datetime, \
    extract_filename_time_from_log_name


def get_file_size(ssh_helper, dir):
    """
    get the size of the file
    :return: file size
    """
    cmd = "ls -nl %s | awk '{print $5}'" % dir
    logger.info("run cmd = [{0} on server {1}]".format(cmd, ssh_helper.host_ip))
    file_size = ssh_helper.ssh_exec_cmd(cmd)
    return file_size


def scp_log(ssh_helper, dir, command):
    """
    scp remote file log to local
    :param args: ssh helper, file path, Directory where the command is executed
    :return: local path
    """
    package_name = dir[dir.rfind('/'):]
    local_path = "{0}{1}".format(command, package_name)
    logger.info("Please wait a moment, download file on server {0}".format(ssh_helper.host_ip))
    ssh_helper.download(dir, local_path)
    return local_path


def rm_rf_file(ssh_helper, dir):
    """
    delete file
    :param args: ssh helper, gather log full path
    :return:
    """
    rm_cmd = "rm -rf {0}".format(dir)
    logger.info("run cmd = [{0} on server {1}]".format(rm_cmd, ssh_helper.host_ip))
    ssh_helper.ssh_exec_cmd(rm_cmd)


def is_empty_dir(ssh_helper, dir):
    """
    determine whether it is an empty folder
    :param args: ssh helper, gather log full path
    :return: true or false
    """
    cmd = "ls -A {gather_path}|wc -w".format(gather_path=dir)
    logger.info("run cmd = [{0} on server {1}]".format(cmd, ssh_helper.host_ip))
    file_num = ssh_helper.ssh_exec_cmd(cmd)
    if int(file_num) == 0:
        return True
    else:
        return False


def get_file_start_time(ssh_helper, file_name, log_dir):
    get_first_line_cmd = "head -n 1 {0}/{1}".format(log_dir, file_name)
    logger.info("get file first time, run cmd = [{0} on server {1}]".format(get_first_line_cmd, ssh_helper.host_ip))
    first_line_text = ssh_helper.ssh_exec_cmd(get_first_line_cmd)
    return extract_time_from_log_file_text(first_line_text)


def get_logfile_name_list(ssh_helper, from_time_str, to_time_str, log_dir, log_files):
    log_name_list = []
    last_file_dict = {"prefix_file_name": "", "file_name": "", "file_end_time": ""}
    for file_name in log_files.split('\n'):
        if file_name == "":
            logger.warn("existing file name is empty")
            continue
        if not file_name.endswith("log") and not file_name.endswith("wf"):
            file_start_time_str = ""
            prefix_name = filename_remove_timetag(file_name)
            file_end_time_str = filename_time_to_datetime(extract_filename_time_from_log_name(file_name))
            if last_file_dict["prefix_file_name"] != "" and last_file_dict["prefix_file_name"] == prefix_name:
                file_start_time_str = last_file_dict["file_end_time"]
            elif last_file_dict["prefix_file_name"] != "" and last_file_dict["prefix_file_name"] != prefix_name:
                file_start_time_str = ""
                file_end_time_str = ""
            elif last_file_dict["prefix_file_name"] == "":
                file_start_time_str = get_file_start_time(ssh_helper, file_name, log_dir)
            # When two time intervals overlap, need to add the file
            if (file_end_time_str != "") and (file_start_time_str != "") and (file_start_time_str <= to_time_str) and (
                    file_end_time_str >= from_time_str):
                log_name_list.append(file_name)
            last_file_dict = {"prefix_file_name": prefix_name, "file_name": file_name,
                              "file_end_time": file_end_time_str}
        elif file_name.endswith("log") or file_name.endswith("wf"):
            logger.info("Filter online file %s on server %s", file_name, ssh_helper.host_ip)
            # Get the first and last lines of text of the file. Here, use a command
            get_first_line_cmd = "head -n 1 {0}/{1} && tail -n 1 {0}/{1}".format(log_dir, file_name)
            first_and_last_line_text = ssh_helper.ssh_exec_cmd(get_first_line_cmd)

            # Split the first and last lines of text
            first_and_last_line_text_list = str(first_and_last_line_text).splitlines()
            if len(first_and_last_line_text_list) >= 2:
                first_line_text = first_and_last_line_text_list[0]
                last_line_text = first_and_last_line_text_list[-1]

                # Time to parse the first and last lines of text
                file_start_time_str = extract_time_from_log_file_text(first_line_text)
                file_end_time = extract_time_from_log_file_text(last_line_text)
                if (file_start_time_str <= to_time_str) and (file_end_time >= from_time_str):
                    log_name_list.append(file_name)
    if len(log_name_list) > 0:
        logger.info("Find the qualified log file {0} on Server [{1}], "
                    "wait for the next step".format(log_name_list, ssh_helper.host_ip))
    else:
        logger.warn("Failed to find the qualified log file on Server [{0}], "
                    "please check whether the input parameters are correct. ".format(ssh_helper.host_ip))
    return log_name_list


def filename_remove_timetag(log_name):
    """ eg: observer.xxx.20221226231617 """
    if len(log_name) > 24:
        return log_name[:-14]
    return ""


def mkdir(ssh_helper, dir):
    """
    Create a folder when it does not exist
    :param args: ssh helper, folder path
    :return:
    """
    mkdir_cmd = "mkdir -p {0}".format(dir)
    logger.info("run cmd = [{0} on server {1}]".format(mkdir_cmd, ssh_helper.host_ip))
    ssh_helper.ssh_exec_cmd(mkdir_cmd)


def delete_empty_file(ssh_helper, dir):
    """
    delete empty folder
    :param args: ssh helper, gather log full path
    :return:
    """
    rm_cmd = "find  {dir} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(dir=dir)
    logger.info("run cmd = [{0} on server {1}]".format(rm_cmd, ssh_helper.host_ip))
    ssh_helper.ssh_exec_cmd(rm_cmd)


def zip_dir(ssh_helper, father_dir, zip_dir):
    zip_cmd = "cd {father_dir} && zip {zip_dir}.zip -rm {zip_dir}".format(
        father_dir=father_dir,
        zip_dir=zip_dir)
    logger.info("compress dir, run cmd = [{0} on server {1}]".format(zip_cmd, ssh_helper.host_ip))
    logger.info("Please wait a moment ...")
    ssh_helper.ssh_exec_cmd(zip_cmd)


def zip_encrypt_dir(ssh_helper, zip_password, father_dir, zip_dir):
    """
    Compress files by encryption
    :param args: ssh helper, password, raw_log_dir, gather dir name
    :return:
    """
    zip_cmd = "cd {father_dir} && zip --password {zip_password} {zip_dir}.zip -rm {zip_dir}".format(
        zip_password=zip_password,
        father_dir=father_dir,
        zip_dir=zip_dir)
    logger.info("run cmd = [{0} on server {1}]".format(zip_cmd, ssh_helper.host_ip))
    ssh_helper.ssh_exec_cmd(zip_cmd)


def is_support_arch(ssh_helper):
    support_arch_list = ["i386", "i486", "i586", "alpha", "x86_64"]
    cmd = "arch"
    logger.info("run cmd = [{0} on server {1}]".format(cmd, ssh_helper.host_ip))
    try:
        arch_info = ssh_helper.ssh_exec_cmd(cmd)
        logger.info("server {0} arch {1}".format(ssh_helper.host_ip, arch_info))
        if arch_info.replace("\n", "") in support_arch_list:
            return True
        else:
            return False
    except:
        logger.error("get server {0} arch info failed")
        return False


def get_observer_version(ob_install_dir, ssh_helper):
    ob_version = ""
    try:
        cmd = "{ob_install_dir}/bin/observer --version".format(ob_install_dir=ob_install_dir)
        ob_version_info = ssh_helper.ssh_exec_cmd_get_stderr(cmd)
        logger.info("get observer version, run cmd = [{0}], result:{1} ".format(cmd, ob_version_info))
        ob_version = re.findall(r'[(]OceanBase\s(.+?)[)]', ob_version_info)[0]
        return ob_version
    except:
        logger.error("get OceanBase version {0} info failed")
        return ob_version


def get_observer_pid(ssh_helper, ob_install_dir):
    try:
        cmd = "sudo cat {ob_install_dir}/run/observer.pid".format(ob_install_dir=ob_install_dir)
        pids = ssh_helper.ssh_exec_cmd(cmd)
        pid_list = pids.split()
        logger.info("get observer pid, run cmd = [{0}], result:{1} ".format(cmd, pid_list))
    except:
        logger.info("get observer pid failed")
        return []
    return pid_list
