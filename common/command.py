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
import os
import re
import subprocess
import sys

from paramiko import SSHException

from common.logger import logger
from utils.time_utils import extract_time_from_log_file_text, filename_time_to_datetime, \
    extract_filename_time_from_log_name


class LocalClient:
    def run(self, cmd):
        try:
            logger.info("[local host] run cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = out.communicate()
            if stderr:
                logger.error("run cmd = [{0}] on localhost, stderr=[{1}]".format(cmd, stderr))
            return stdout
        except:
            logger.error("run cmd = [{0}] on localhost".format(cmd))

    def run_get_stderr(self, cmd):
        try:
            logger.info("run cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = out.communicate()
            return stderr
        except:
            logger.error("run cmd = [{0}] on localhost".format(cmd))


class SshClient:
    def run(self, ssh_helper, cmd):
        try:
            logger.info("[remote host {0}] excute cmd = [{1}]".format(ssh_helper.host_ip, cmd))
            stdout = ssh_helper.ssh_exec_cmd(cmd)
            logger.debug(
                "[remote host {0}] excute cmd = [{1}] complete, stdout=[{2}]".format(ssh_helper.host_ip, cmd, stdout))
            return stdout
        except Exception as e:
            logger.error("[remote host {0}] excute cmd = [{1}] except: [{2}]".format(ssh_helper.host_ip, cmd, e))

    def run_get_stderr(self, ssh_helper, cmd):
        try:
            logger.info("[remote host {0}] run cmd = [{1}] start ...".format(ssh_helper.host_ip, cmd))
            std = ssh_helper.ssh_exec_cmd_get_stderr(cmd)
            return std
        except Exception as e:
            logger.error("[remote host {0}] run ssh cmd = [{1}] except: {2}".format(ssh_helper.host_ip, cmd, e))

    def run_ignore_err(self, ssh_helper, cmd):
        try:
            logger.info("[remote host {0}] run cmd = [{1}] start ...".format(ssh_helper.host_ip, cmd))
            std = ssh_helper.ssh_exec_cmd_ignore_err(cmd)
            return std
        except SSHException as e:
            logger.error("[remote host {0}] run ssh cmd = [{1}] except: {2}".format(ssh_helper.host_ip, cmd, e))

def get_file_size(is_ssh, ssh_helper, dir):
    """
    get the size of the file
    :param: is_ssh, ssh helper, dir
    :return: file size
    """
    cmd = "ls -nl %s | awk '{print $5}'" % dir
    if is_ssh:
        file_size = SshClient().run(ssh_helper, cmd)
    else:
        file_size = LocalClient().run(cmd)
    return file_size


def download_file(is_ssh, ssh_helper, remote_path, local_path):
    """
    download file
    :param args: is_ssh, ssh helper, file path
    :return: local path
    """
    logger.info(
        "Please wait a moment, download file [{0}] from server {1} to [{2}]".format(remote_path, ssh_helper.host_ip,
                                                                                    local_path))
    if is_ssh:
        ssh_helper.download(remote_path, local_path)
    else:
        cmd = "cp -r {0} {1}".format(remote_path, local_path)
        LocalClient().run(cmd)
    return local_path


def upload_file(is_ssh, ssh_helper, local_path, remote_path):
    """
    upload file
    :param args: is_ssh, ssh helper, local file path, remote file path
    :return: local path
    """
    logger.info("Please wait a moment, upload file to server {0}, local file path {1}, remote file path {2}".format(
        ssh_helper.host_ip, local_path, remote_path))
    if is_ssh:
        ssh_helper.upload(local_path, remote_path)
    else:
        cmd = "cp -r {0} {1}".format(local_path, remote_path)
        LocalClient().run(cmd)


def rm_rf_file(is_ssh, ssh_helper, dir):
    """
    delete file
    :param args: is_ssh, ssh helper, gather log full path
    :return:
    """
    cmd = "rm -rf {0}".format(dir)
    if is_ssh:
        SshClient().run(ssh_helper, cmd)
    else:
        LocalClient().run(cmd)


def is_empty_dir(is_ssh, ssh_helper, dir):
    """
    determine whether it is an empty folder
    :param args: is_ssh, ssh helper, gather log full path
    :return: true or false
    """
    cmd = "ls -A {gather_path}|wc -w".format(gather_path=dir)
    if is_ssh:
        file_num = SshClient().run(ssh_helper, cmd)
    else:
        file_num = LocalClient().run(cmd)
    if int(file_num) == 0:
        return True
    else:
        return False


def get_file_start_time(is_ssh, ssh_helper, file_name, dir):
    """
    get log file start time 
    :param args: is_ssh, ssh helper, gather log full path
    :return: true or false
    """
    cmd = "head -n 1 {0}/{1}".format(dir, file_name)
    if is_ssh:
        first_line_text = SshClient().run(ssh_helper, cmd)
    else:
        first_line_text = LocalClient().run(cmd)
    return extract_time_from_log_file_text(str(first_line_text))


def get_logfile_name_list(is_ssh, ssh_helper, from_time_str, to_time_str, log_dir, log_files):
    """
    get log name list 
    :param args: is_ssh, ssh helper, from time, to time, log dir, log file list
    :return: true or false
    """
    log_name_list = []
    last_file_dict = {"prefix_file_name": "", "file_name": "", "file_end_time": ""}
    for file_name in log_files.split('\n'):
        if file_name == "":
            logger.warn("existing file name is empty")
            continue
        if not file_name.endswith("log") and not file_name.endswith("wf"):
            file_start_time_str = ""
            prefix_name = file_name[:-14] if len(file_name) > 24 else ""
            file_end_time_str = filename_time_to_datetime(extract_filename_time_from_log_name(file_name))
            if last_file_dict["prefix_file_name"] != "" and last_file_dict["prefix_file_name"] == prefix_name:
                file_start_time_str = last_file_dict["file_end_time"]
            elif last_file_dict["prefix_file_name"] != "" and last_file_dict["prefix_file_name"] != prefix_name:
                file_start_time_str = ""
                file_end_time_str = ""
            elif last_file_dict["prefix_file_name"] == "":
                file_start_time_str = get_file_start_time(is_ssh, ssh_helper, file_name, log_dir)
            # When two time intervals overlap, need to add the file
            if (file_end_time_str != "") and (file_start_time_str != "") and (file_start_time_str <= to_time_str) and (
                    file_end_time_str >= from_time_str):
                log_name_list.append(file_name)
            last_file_dict = {"prefix_file_name": prefix_name, "file_name": file_name,
                              "file_end_time": file_end_time_str}
        elif file_name.endswith("log") or file_name.endswith("wf"):
            logger.info("Filter online file %s on server", file_name)
            # Get the first and last lines of text of the file. Here, use a command
            get_first_line_cmd = "head -n 1 {0}/{1} && tail -n 1 {0}/{1}".format(log_dir, file_name)
            if is_ssh:
                first_and_last_line_text = SshClient().run(ssh_helper, get_first_line_cmd)
            else:
                first_and_last_line_text = LocalClient().run(get_first_line_cmd)

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
                    "wait for the next step".format(log_name_list, "localhost" if is_ssh else ssh_helper.host_ip))
    else:
        logger.warn("Failed to find the qualified log file on Server [{0}], "
                    "please check whether the input parameters are correct. ".format(
            "localhost" if is_ssh else ssh_helper.host_ip))
    return log_name_list


def mkdir(is_ssh, ssh_helper, dir):
    """
    Create a folder when it does not exist
    :param args: is_ssh, ssh helper, folder path
    :return:
    """
    cmd = "mkdir -p {0}".format(dir)
    if is_ssh:
        SshClient().run(ssh_helper, cmd)
    else:
        LocalClient().run(cmd)


def delete_empty_file(is_ssh, ssh_helper, dir):
    """
    delete empty folder
    :param args: is_ssh, ssh helper, gather log full path
    :return:
    """
    cmd = "find  {dir} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(dir=dir)
    if is_ssh:
        SshClient().run(ssh_helper, cmd)
    else:
        LocalClient().run(cmd)


def zip_dir(is_ssh, ssh_helper, father_dir, zip_dir):
    """
    Compress files through zip
    :param args: is_ssh, ssh helper, father dir, zip dir
    :return:
    """
    cmd = "cd {father_dir} && zip {zip_dir}.zip -rm {zip_dir}".format(
        father_dir=father_dir,
        zip_dir=zip_dir)
    logger.info("Please wait a moment ...")
    if is_ssh:
        SshClient().run(ssh_helper, cmd)
    else:
        LocalClient().run(cmd)


def zip_encrypt_dir(is_ssh, ssh_helper, zip_password, father_dir, zip_dir):
    """
    Compress files by encryption
    :param args: is_ssh, ssh helper, password, raw_log_dir, gather dir name
    :return:
    """
    cmd = "cd {father_dir} && zip --password {zip_password} {zip_dir}.zip -rm {zip_dir}".format(
        zip_password=zip_password,
        father_dir=father_dir,
        zip_dir=zip_dir)
    logger.info("Please wait a moment ...")
    if is_ssh:
        SshClient().run(ssh_helper, cmd)
    else:
        LocalClient().run(cmd)


def is_support_arch(is_ssh, ssh_helper):
    """
    Determine if it is a supported operating system
    :param args: is_ssh, ssh helper
    :return:
    """
    support_arch_list = ["i386", "i486", "i586", "alpha", "x86_64"]
    cmd = "arch"
    try:
        if is_ssh:
            arch_info = SshClient().run(ssh_helper, cmd)
        else:
            arch_info = LocalClient().run(cmd)
        if arch_info.replace("\n", "") in support_arch_list:
            return True
        else:
            return False
    except:
        logger.error("get server arch info failed")
        return False


def get_observer_version(is_ssh, ssh_helper, ob_install_dir):
    """
    get observer version
    :param args: is_ssh, ssh helper, ob install dir
    :return:
    """
    ob_version = ""
    cmd = "{ob_install_dir}/bin/observer --version".format(ob_install_dir=ob_install_dir)
    if is_ssh:
        ob_version_info = SshClient().run_get_stderr(ssh_helper, cmd)
    else:
        ob_version_info = LocalClient().run(cmd)
    logger.info("get observer version, run cmd = [{0}] ".format(cmd))
    if ob_version_info is not None:
        ob_version = re.findall(r'[(]OceanBase.(.+?)[)]', ob_version_info)
        if len(ob_version) > 0:
            return ob_version[0]
        else:
            cmd = "export LD_LIBRARY_PATH={ob_install_dir}/lib && {ob_install_dir}/bin/observer --version".format(
                ob_install_dir=ob_install_dir)
            if is_ssh:
                ob_version_info = SshClient().run(ssh_helper, cmd)
            else:
                ob_version_info = LocalClient().run(cmd)
            logger.info("get observer version, run cmd = [{0}]".format(cmd))
            ob_version = re.findall(r'[(]OceanBase.CE\s(.+?)[)]', ob_version_info)[0]
            return ob_version


def get_observer_pid(is_ssh, ssh_helper, ob_install_dir):
    """
    get observer pid
    :param args: is_ssh, ssh helper, ob install dir
    :return:
    """
    try:
        cmd = "cat {ob_install_dir}/run/observer.pid".format(ob_install_dir=ob_install_dir)
        if is_ssh:
            pids = SshClient().run(ssh_helper, cmd)
        else:
            pids = LocalClient().run(cmd)
        pid_list = pids.split()
        logger.info("get observer pid, run cmd = [{0}], result:{1} ".format(cmd, pid_list))
    except:
        logger.info("get observer pid failed")
        return []
    return pid_list


def delete_file_force(is_ssh, ssh_helper, file_name):
    """
    delete file force
    :param args: is_ssh, ssh helper, file_name
    :return:
    """
    cmd = "rm -rf {0}".format(file_name)
    if is_ssh:
        SshClient().run(ssh_helper, cmd)
    else:
        LocalClient().run(cmd)


def delete_empty_file(is_ssh, ssh_helper, file_path):
    """
    delete empty file
    :param args: is_ssh, ssh helper, file_name
    :return:
    """
    cmd = "find  {file_path} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(file_path=file_path)
    if is_ssh:
        SshClient().run(ssh_helper, cmd)
    else:
        LocalClient().run(cmd)


def delete_file(is_ssh, ssh_helper, file_path):
    """
    delete file
    :param args: is_ssh, ssh helper, file_name
    :return:
    """
    cmd = "rm -rf {file_path}".format(file_path=file_path)
    if is_ssh:
        SshClient().run(ssh_helper, cmd)
    else:
        LocalClient().run(cmd)


def get_file_size(is_ssh, ssh_helper, file_path):
    """
    get file size
    :param args: is_ssh, ssh helper, file_path
    :return:
    """
    cmd = "ls -nl %s | awk '{print $5}'" % file_path
    if is_ssh:
        file_size = SshClient().run(ssh_helper, cmd)
    else:
        file_size = LocalClient().run(cmd)
    return file_size


def is_empty_dir(is_ssh, ssh_helper, dir_path):
    """
    is empty dir
    :param args: is_ssh, ssh helper, dir_path
    :return: True or False
    """
    cmd = "ls -A {dir_path}|wc -w".format(dir_path=dir_path)
    if is_ssh:
        file_num = SshClient().run(ssh_helper, cmd)
    else:
        file_num = LocalClient().run(cmd)
    if int(file_num) == 0:
        return True
    else:
        return False


def is_empty_file(is_ssh, ssh_helper, file_path):
    """
    is empty file
    :param args: is_ssh, ssh helper, file_path
    :return: True or False
    """
    file_size = get_file_size(is_ssh, ssh_helper, file_path)
    if int(file_size) == 0:
        return True
    else:
        return False


def get_obdiag_display(log_dir, trace_id):
    cmd = 'grep -h "\[{}\]" {}* | sed "s/\[{}\] //g" '.format(trace_id, log_dir, trace_id)
    stdout = LocalClient().run(cmd)
    print_stdout = str(stdout).replace('\\n', '\n').replace('\\t', '\t')
    if len(print_stdout)>0:
        print(print_stdout)
