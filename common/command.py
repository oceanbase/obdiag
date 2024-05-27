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
import subprocess

from paramiko import SSHException
from common.ob_connector import OBConnector
from common.tool import TimeUtils


class LocalClient(object):
    def __init__(self, stdio=None):
        self.stdio = stdio

    def run(self, cmd):
        try:
            self.stdio.verbose("[local host] run cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = out.communicate()
            if stderr:
                self.stdio.error("run cmd = [{0}] on localhost, stderr=[{1}]".format(cmd, stderr))
            return stdout
        except:
            self.stdio.error("run cmd = [{0}] on localhost".format(cmd))

    def run_get_stderr(self, cmd):
        try:
            self.stdio.verbose("run cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = out.communicate()
            return stderr
        except:
            self.stdio.error("run cmd = [{0}] on localhost".format(cmd))


class SshClient(object):
    def __init__(self, stdio=None):
        self.stdio = stdio

    def run(self, ssh_helper, cmd):
        try:
            self.stdio.verbose("[remote host {0}] excute cmd = [{1}]".format(ssh_helper.get_name(), cmd))
            stdout = ssh_helper.ssh_exec_cmd(cmd)
            self.stdio.verbose("[remote host {0}] excute cmd = [{1}] complete, stdout=[{2}]".format(ssh_helper.get_name(), cmd, stdout))
            return stdout
        except Exception as e:
            self.stdio.error("[remote host {0}] excute cmd = [{1}] except: [{2}]".format(ssh_helper.get_name(), cmd, e))

    def run_get_stderr(self, ssh_helper, cmd):
        try:
            self.stdio.verbose("[remote host {0}] run cmd = [{1}] start ...".format(ssh_helper.get_name(), cmd))
            std = ssh_helper.ssh_exec_cmd_get_stderr(cmd)
            return std
        except Exception as e:
            self.stdio.error("[remote host {0}] run ssh cmd = [{1}] except: {2}".format(ssh_helper.get_name(), cmd, e))

    def run_ignore_err(self, ssh_helper, cmd):
        try:
            self.stdio.verbose("[remote host {0}] run cmd = [{1}] start ...".format(ssh_helper.get_name(), cmd))
            std = ssh_helper.ssh_exec_cmd_ignore_err(cmd)
            return std
        except SSHException as e:
            self.stdio.error("[remote host {0}] run ssh cmd = [{1}] except: {2}".format(ssh_helper.get_name(), cmd, e))


def download_file(is_ssh, ssh_helper, remote_path, local_path, stdio=None):
    """
    download file
    :param args: is_ssh, ssh helper, file path
    :return: local path
    """
    try:
        if is_ssh:
            stdio.verbose("Please wait a moment, download file [{0}] from server {1} to [{2}]".format(remote_path, ssh_helper.get_name(), local_path))
            ssh_helper.download(remote_path, local_path)
        else:
            cmd = "cp -r {0} {1}".format(remote_path, local_path)
            LocalClient(stdio).run(cmd)
    except Exception as e:
        stdio.error("Download File Failed error: {0}".format(e))
    return local_path


def upload_file(is_ssh, ssh_helper, local_path, remote_path, stdio=None):
    """
    upload file
    :param args: is_ssh, ssh helper, local file path, remote file path
    :return: local path
    """
    stdio.verbose("Please wait a moment, upload file to server {0}, local file path {1}, remote file path {2}".format(ssh_helper.get_name(), local_path, remote_path))
    try:
        if is_ssh:
            ssh_helper.upload(local_path, remote_path)
        else:
            cmd = "cp -r {0} {1}".format(local_path, remote_path)
            LocalClient(stdio).run(cmd)
    except Exception as e:
        stdio.error("Upload File Failed error: {0}".format(e))


def rm_rf_file(is_ssh, ssh_helper, dir, stdio=None):
    """
    delete file
    :param args: is_ssh, ssh helper, gather log full path
    :return:
    """
    cmd = "rm -rf {0}".format(dir)
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def delete_file_in_folder(is_ssh, ssh_helper, file_path, stdio):
    """
    delete file
    :param args: is_ssh, ssh helper, file_name
    :return:
    """
    if (file_path is None) or (not 'gather_pack' in file_path):
        raise Exception("Please check file path, {0}".format(file_path))
    cmd = "rm -rf {file_path}/*".format(file_path=file_path)
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def is_empty_dir(is_ssh, ssh_helper, dir, stdio=None):
    """
    determine whether it is an empty folder
    :param args: is_ssh, ssh helper, gather log full path
    :return: true or false
    """
    cmd = "ls -A {gather_path}|wc -w".format(gather_path=dir)
    if is_ssh:
        file_num = SshClient(stdio).run(ssh_helper, cmd)
    else:
        file_num = LocalClient(stdio).run(cmd)
    if int(file_num) == 0:
        return True
    else:
        return False


def get_file_start_time(is_ssh, ssh_helper, file_name, dir, stdio=None):
    """
    get log file start time
    :param args: is_ssh, ssh helper, gather log full path
    :return: true or false
    """
    cmd = "head -n 1 {0}/{1}".format(dir, file_name)
    if is_ssh:
        first_line_text = SshClient(stdio).run(ssh_helper, cmd)
    else:
        first_line_text = LocalClient(stdio).run(cmd)
    return TimeUtils.extract_time_from_log_file_text(str(first_line_text))


def get_logfile_name_list(is_ssh, ssh_helper, from_time_str, to_time_str, log_dir, log_files, stdio=None):
    """
    get log name list
    :param args: is_ssh, ssh helper, from time, to time, log dir, log file list
    :return: true or false
    """
    stdio.verbose("get log file name list, from time {0}, to time {1}, log dir {2}, log files {3}".format(from_time_str, to_time_str, log_dir, log_files))
    log_name_list = []
    last_file_dict = {"prefix_file_name": "", "file_name": "", "file_end_time": ""}
    for file_name in log_files.split('\n'):
        if file_name == "":
            stdio.verbose("existing file name is empty")
            continue
        if not file_name.endswith("log") and not file_name.endswith("wf"):
            file_start_time_str = ""
            prefix_name = file_name[:-14] if len(file_name) > 24 else ""
            file_end_time_str = TimeUtils.filename_time_to_datetime(TimeUtils.extract_filename_time_from_log_name(file_name, stdio), stdio)
            if last_file_dict["prefix_file_name"] != "" and last_file_dict["prefix_file_name"] == prefix_name:
                file_start_time_str = last_file_dict["file_end_time"]
            elif last_file_dict["prefix_file_name"] != "" and last_file_dict["prefix_file_name"] != prefix_name:
                file_start_time_str = ""
                file_end_time_str = ""
            elif last_file_dict["prefix_file_name"] == "":
                file_start_time_str = get_file_start_time(is_ssh, ssh_helper, file_name, log_dir, stdio)
            # When two time intervals overlap, need to add the file
            if (file_end_time_str != "") and (file_start_time_str != "") and (file_start_time_str <= to_time_str) and (file_end_time_str >= from_time_str):
                log_name_list.append(file_name)
            last_file_dict = {"prefix_file_name": prefix_name, "file_name": file_name, "file_end_time": file_end_time_str}
        elif file_name.endswith("log") or file_name.endswith("wf"):
            # Get the first and last lines of text of the file. Here, use a command
            get_first_line_cmd = "head -n 1 {0}/{1} && tail -n 1 {0}/{1}".format(log_dir, file_name)
            if is_ssh:
                first_and_last_line_text = SshClient(stdio).run(ssh_helper, get_first_line_cmd)
            else:
                first_and_last_line_text = LocalClient(stdio).run(get_first_line_cmd)

            # Split the first and last lines of text
            first_and_last_line_text_list = str(first_and_last_line_text).splitlines()
            if len(first_and_last_line_text_list) >= 2:
                first_line_text = first_and_last_line_text_list[0]
                last_line_text = first_and_last_line_text_list[-1]

                # Time to parse the first and last lines of text
                file_start_time_str = TimeUtils.extract_time_from_log_file_text(first_line_text, stdio)
                file_end_time = TimeUtils.extract_time_from_log_file_text(last_line_text, stdio)
                stdio.verbose("The log file {0} starts at {1} ends at {2}".format(file_name, file_start_time_str, file_end_time))
                stdio.verbose("to_time_str {0} from_time_str {1}".format(to_time_str, from_time_str))
                if (file_start_time_str <= to_time_str) and (file_end_time >= from_time_str):
                    log_name_list.append(file_name)
    if len(log_name_list) > 0:
        stdio.verbose("Find the qualified log file {0} on Server [{1}], " "wait for the next step".format(log_name_list, "localhost" if not is_ssh else ssh_helper.get_name()))
    else:
        stdio.warn("No found the qualified log file on Server [{0}]".format("localhost" if not is_ssh else ssh_helper.get_name()))
    return log_name_list


def mkdir(is_ssh, ssh_helper, dir, stdio=None):
    """
    Create a folder when it does not exist
    :param args: is_ssh, ssh helper, folder path
    :return:
    """
    cmd = "mkdir -p {0}".format(dir)
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def delete_empty_file(is_ssh, ssh_helper, dir, stdio=None):
    """
    delete empty folder
    :param args: is_ssh, ssh helper, gather log full path
    :return:
    """
    cmd = "find  {dir} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(dir=dir)
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def zip_dir(is_ssh, ssh_helper, father_dir, zip_dir, stdio=None):
    """
    Compress files through zip
    :param args: is_ssh, ssh helper, father dir, zip dir
    :return:
    """
    cmd = "cd {father_dir} && zip {zip_dir}.zip -rm {zip_dir}".format(father_dir=father_dir, zip_dir=zip_dir)
    stdio.verbose("Please wait a moment ...")
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def zip_encrypt_dir(is_ssh, ssh_helper, zip_password, father_dir, zip_dir, stdio=None):
    """
    Compress files by encryption
    :param args: is_ssh, ssh helper, password, raw_log_dir, gather dir name
    :return:
    """
    cmd = "cd {father_dir} && zip --password {zip_password} {zip_dir}.zip -rm {zip_dir}".format(zip_password=zip_password, father_dir=father_dir, zip_dir=zip_dir)
    stdio.verbose("Please wait a moment ...")
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def is_support_arch(is_ssh, ssh_helper, stdio=None):
    """
    Determine if it is a supported operating system
    :param args: is_ssh, ssh helper
    :return:
    """
    support_arch_list = ["i386", "i486", "i586", "alpha", "x86_64"]
    cmd = "arch"
    try:
        if is_ssh:
            arch_info = SshClient(stdio).run(ssh_helper, cmd)
        else:
            arch_info = LocalClient(stdio).run(cmd)
        if arch_info.replace("\n", "") in support_arch_list:
            return True
        else:
            return False
    except:
        stdio.error("get server arch info failed")
        return False


def get_observer_version(is_ssh, ssh_helper, ob_install_dir, stdio):
    """
    get observer version
    :param args: is_ssh, ssh helper, ob install dir
    :return:
    """
    ob_version = ""
    cmd = "{ob_install_dir}/bin/observer --version".format(ob_install_dir=ob_install_dir)
    if is_ssh:
        ob_version_info = SshClient(stdio).run_get_stderr(ssh_helper, cmd)
    else:
        ob_version_info = LocalClient(stdio).run_get_stderr(cmd)
    stdio.verbose("get observer version, run cmd = [{0}] ".format(cmd))
    if ob_version_info is not None:
        ob_version = re.findall(r'[(]OceanBase.(.+?)[)]', ob_version_info)
        if len(ob_version) > 0:
            result = re.sub(r'[a-zA-Z]', '', ob_version[0])
            return result.strip()
        else:
            cmd = "export LD_LIBRARY_PATH={ob_install_dir}/lib && {ob_install_dir}/bin/observer --version".format(ob_install_dir=ob_install_dir)
            if is_ssh:
                ob_version_info = SshClient(stdio).run_get_stderr(ssh_helper, cmd)
            else:
                ob_version_info = LocalClient(stdio).run_get_stderr(cmd)
            stdio.verbose("get observer version with LD_LIBRARY_PATH,cmd:{0}".format(cmd))
            if "REVISION" not in ob_version_info:
                raise Exception("Please check conf about observer,{0}".format(ob_version_info))
            ob_version = re.findall(r'[(]OceanBase.*\s(.+?)[)]', ob_version_info)
            result = re.sub(r'[a-zA-Z]', '', ob_version[0])
            return result.strip()


def get_obproxy_version(is_ssh, ssh_helper, obproxy_install_dir, stdio):
    """
    get obproxy version
    :param args: is_ssh, ssh helper, ob install dir
    :return:
    """
    obproxy_version = ""
    cmd = "{obproxy_install_dir}/bin/obproxy --version".format(obproxy_install_dir=obproxy_install_dir)
    if is_ssh:
        obproxy_version_info = SshClient(stdio).run_get_stderr(ssh_helper, cmd)
    else:
        obproxy_version_info = LocalClient(stdio).run_get_stderr(cmd)
    stdio.verbose("get obproxy version, run cmd = [{0}] ".format(cmd))
    if obproxy_version_info is not None:
        ob_version = re.findall(r'[(]OceanBase.(.+? +?)[)]', obproxy_version_info)
        if len(ob_version) > 0:
            return ob_version[0]
        else:
            cmd = "export LD_LIBRARY_PATH={obproxy_install_dir}/lib && {obproxy_install_dir}/bin/obproxy --version".format(obproxy_install_dir=obproxy_install_dir)
            if is_ssh:
                obproxy_version_info = SshClient(stdio).run_get_stderr(ssh_helper, cmd)
            else:
                obproxy_version_info = LocalClient(stdio).run_get_stderr(cmd)
            stdio.verbose("get obproxy version with LD_LIBRARY_PATH,cmd:{0}, result:{1}".format(cmd, obproxy_version_info))
            if "REVISION" not in obproxy_version_info:
                raise Exception("Please check conf about proxy,{0}".format(obproxy_version_info))
            pattern = r"(\d+\.\d+\.\d+\.\d+)"
            match = re.search(pattern, obproxy_version_info)
            if match:
                obproxy_version_info = match.group(1)
                obproxy_version_info = obproxy_version_info.split()[0]
            else:
                pattern = r"(\d+\.\d+\.\d+)"
                match = re.search(pattern, obproxy_version_info)
                obproxy_version_info = match.group(1)
                obproxy_version_info = obproxy_version_info.split()[0]
            return obproxy_version_info


# Only applicable to the community version


def get_observer_version_by_sql(ob_cluster, stdio=None):
    stdio.verbose("start get_observer_version_by_sql . input: {0}".format(ob_cluster))
    try:
        ob_connector = OBConnector(ip=ob_cluster.get("db_host"), port=ob_cluster.get("db_port"), username=ob_cluster.get("tenant_sys").get("user"), password=ob_cluster.get("tenant_sys").get("password"), stdio=stdio, timeout=100)
        ob_version_info = ob_connector.execute_sql("select version();")
    except Exception as e:
        raise Exception("get_observer_version_by_sql Exception. Maybe cluster'info is error: " + e.__str__())

    ob_version = ob_version_info[0]
    stdio.verbose("get_observer_version_by_sql ob_version_info is {0}".format(ob_version))
    version = re.findall(r'OceanBase(_)?(.CE)?-v(.+)', ob_version[0])
    if len(version) > 0:
        return version[0][2]
    else:
        version = re.findall(r'(.+)', ob_version[0])
        return version[0]


def get_observer_pid(is_ssh, ssh_helper, ob_install_dir, stdio=None):
    """
    get observer pid
    :param args: is_ssh, ssh helper, ob install dir
    :return:
    """
    try:
        cmd = "cat {ob_install_dir}/run/observer.pid".format(ob_install_dir=ob_install_dir)
        if is_ssh:
            pids = SshClient(stdio).run(ssh_helper, cmd)
        else:
            pids = LocalClient(stdio).run(cmd)
        pid_list = pids.split()
        stdio.verbose("get observer pid, run cmd = [{0}], result:{1} ".format(cmd, pid_list))
    except:
        stdio.verbose("get observer pid failed")
        return []
    return pid_list


def delete_file_force(is_ssh, ssh_helper, file_name, stdio=None):
    """
    delete file force
    :param args: is_ssh, ssh helper, file_name
    :return:
    """
    cmd = "rm -rf {0}".format(file_name)
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def delete_empty_file(is_ssh, ssh_helper, file_path, stdio=None):
    """
    delete empty file
    :param args: is_ssh, ssh helper, file_name
    :return:
    """
    cmd = "find  {file_path} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(file_path=file_path)
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def delete_file(is_ssh, ssh_helper, file_path, stdio=None):
    """
    delete file
    :param args: is_ssh, ssh helper, file_name
    :return:
    """
    cmd = "rm -rf {file_path}".format(file_path=file_path)
    if is_ssh:
        SshClient(stdio).run(ssh_helper, cmd)
    else:
        LocalClient(stdio).run(cmd)


def get_file_size(is_ssh, ssh_helper, file_path, stdio=None):
    """
    get file size
    :param args: is_ssh, ssh helper, file_path
    :return:
    """
    cmd = "ls -nl %s | awk '{print $5}'" % file_path
    if is_ssh:
        file_size = SshClient(stdio).run(ssh_helper, cmd)
    else:
        file_size = LocalClient(stdio).run(cmd)
    return file_size


def is_empty_dir(is_ssh, ssh_helper, dir_path, stdio=None):
    """
    is empty dir
    :param args: is_ssh, ssh helper, dir_path
    :return: True or False
    """
    cmd = "ls -A {dir_path}|wc -w".format(dir_path=dir_path)
    if is_ssh:
        file_num = SshClient(stdio).run(ssh_helper, cmd)
    else:
        file_num = LocalClient(stdio).run(cmd)
    if int(file_num) == 0:
        return True
    else:
        return False


def is_empty_file(is_ssh, ssh_helper, file_path, stdio=None):
    """
    is empty file
    :param args: is_ssh, ssh helper, file_path
    :return: True or False
    """
    file_size = get_file_size(is_ssh, ssh_helper, file_path, stdio)
    if int(file_size) == 0:
        return True
    else:
        return False


def get_obdiag_display(log_dir, trace_id, stdio=None):
    cmd = 'grep -h "\[{}\]" {}* | sed "s/\[{}\] //g" '.format(trace_id, log_dir, trace_id)
    stdout = LocalClient(stdio).run(cmd)
    print_stdout = str(stdout).replace('\\n', '\n').replace('\\t', '\t')
    if len(print_stdout) > 0:
        print(print_stdout)


def uzip_dir_local(uzip_dir, stdio=None):
    """
    Uncompress files through zip
    :param args: father dir, zip dir
    :return:
    """
    cmd = f"cd {uzip_dir} && unzip *.zip && rm -rf *.zip"
    LocalClient(stdio).run(cmd)


def analyze_log_get_sqc_addr(uzip_dir, stdio):
    """
    analyze files
    :param args: father dir, uzip dir
    :return: ip_port
    """
    cmd = "cd {uzip_dir} && cd ob_log* && grep {key_words} * | grep -oP '{key_words}=\"\\K[^\"]+' | sort | uniq".format(uzip_dir=uzip_dir, key_words="px_obdiag_sqc_addr")
    stdout = LocalClient(stdio).run(cmd)
    sqc_addrs = stdout.decode().strip().split('\n')
    if len(sqc_addrs) > 0:
        if not re.match(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', sqc_addrs[0]):
            return None
        else:
            return sqc_addrs[0]
    else:
        return None


def find_home_path_by_port(is_ssh, ssh_helper, internal_port_str, stdio):
    cmd = "ps aux | grep observer | grep 'P {internal_port_str}' |  grep -oP '/[^\s]*/bin/observer' ".format(internal_port_str=internal_port_str)
    if is_ssh:
        stdout = SshClient(stdio).run(ssh_helper, cmd)
    else:
        stdout = LocalClient(stdio).run(cmd)

    str_list = stdout.strip().split('\n')
    home_path = ""
    for original_str in str_list:
        original_str = str(original_str)
        if original_str.endswith("/bin/observer") and not original_str.startswith('/[^\s]*'):
            home_path = original_str.rstrip("/bin/observer")
            break
    stdio.verbose("home_path:{0}".format(home_path))
    return home_path
