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
import traceback
from common.ob_connector import OBConnector
from common.ssh_client.ssh import SshClient
from common.tool import TimeUtils


class LocalClient(object):
    def __init__(self, stdio=None):
        self.stdio = stdio

    def run(self, cmd):
        try:
            self.stdio.verbose("[local host] run cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
            stdout, stderr = out.communicate()
            if stderr:
                self.stdio.error("run cmd = [{0}] on localhost, stderr=[{1}]".format(cmd, stderr))
            return stdout
        except:
            self.stdio.error("run cmd = [{0}] on localhost".format(cmd))

    def run_get_stderr(self, cmd):
        try:
            self.stdio.verbose("run cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
            stdout, stderr = out.communicate()
            return stderr
        except:
            self.stdio.error("run cmd = [{0}] on localhost".format(cmd))


def download_file(ssh_client, remote_path, local_path, stdio=None):
    """
    download file
    :return: local path
    """
    try:
        ssh_client.download(remote_path, local_path)
    except Exception as e:
        stdio.error("Download File Failed error: {0}".format(e))
        stdio.verbose(traceback.format_exc())
    return local_path


def upload_file(ssh_client, local_path, remote_path, stdio=None):
    """
    upload file
    :return: local path
    """
    stdio.verbose("Please wait a moment, upload file to server {0}, local file path {1}, remote file path {2}".format(ssh_client.get_name(), local_path, remote_path))
    try:
        ssh_client.upload(remote_path, local_path)
    except Exception as e:
        stdio.error("Upload File Failed error: {0}".format(e))


def rm_rf_file(ssh_client, dir, stdio=None):
    """
    delete file
    :return:
    """
    cmd = "rm -rf {0}".format(dir)
    ssh_client.exec_cmd(cmd)


def delete_file_in_folder(ssh_client, file_path, stdio=None):
    """
    delete file
    :return:
    """
    if (file_path is None) or (not 'gather_pack' in file_path):
        raise Exception("Please check file path, {0}".format(file_path))
    cmd = "rm -rf {file_path}/*".format(file_path=file_path)
    ssh_client.exec_cmd(cmd)


def is_empty_dir(ssh_client, dir, stdio=None):
    """
    determine whether it is an empty folder
    :return: true or false
    """
    cmd = "ls -A {gather_path}|wc -w".format(gather_path=dir)
    file_num = ssh_client.exec_cmd(cmd)
    if int(file_num) == 0:
        return True
    else:
        return False


def get_file_start_time(ssh_client, file_name, dir, stdio=None):
    """
    get log file start time
    :return: true or false
    """
    cmd = "head -n 1 {0}/{1}".format(dir, file_name)
    first_line_text = ssh_client.exec_cmd(cmd)
    return TimeUtils.extract_time_from_log_file_text(str(first_line_text))


def get_logfile_name_list(ssh_client, from_time_str, to_time_str, log_dir, log_files, stdio=None):
    """
    get log name list
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
                file_start_time_str = get_file_start_time(ssh_client, file_name, log_dir, stdio)
            # When two time intervals overlap, need to add the file
            if (file_end_time_str != "") and (file_start_time_str != "") and (file_start_time_str <= to_time_str) and (file_end_time_str >= from_time_str):
                log_name_list.append(file_name)
            last_file_dict = {"prefix_file_name": prefix_name, "file_name": file_name, "file_end_time": file_end_time_str}
        elif file_name.endswith("log") or file_name.endswith("wf"):
            # Get the first and last lines of text of the file. Here, use a command
            get_first_line_cmd = "head -n 1 {0}/{1} && tail -n 1 {0}/{1}".format(log_dir, file_name)
            first_and_last_line_text = ssh_client.exec_cmd(get_first_line_cmd)
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
        stdio.verbose("Find the qualified log file {0} on Server [{1}], " "wait for the next step".format(log_name_list, ssh_client.get_name()))
    else:
        stdio.warn("No found the qualified log file on Server [{0}]".format(ssh_client.get_name()))
    return log_name_list


def mkdir(ssh_client, dir, stdio=None):
    """
    Create a folder when it does not exist
    :return:
    """
    cmd = "mkdir -p {0}".format(dir)
    ssh_client.exec_cmd(cmd)


def delete_empty_file(ssh_client, dir):
    """
    delete empty folder
    :return:
    """
    cmd = "find  {dir} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(dir=dir)
    ssh_client.exec_cmd(cmd)


def zip_dir(ssh_client, father_dir, zip_dir, stdio=None):
    """
    Compress files through zip
    :return:
    """
    cmd = "cd {father_dir} && zip {zip_dir}.zip -rm {zip_dir}".format(father_dir=father_dir, zip_dir=zip_dir)
    ssh_client.exec_cmd(cmd)


def zip_encrypt_dir(ssh_client, zip_password, father_dir, zip_dir, stdio=None):
    """
    Compress files by encryption
    :return:
    """
    cmd = "cd {father_dir} && zip --password {zip_password} {zip_dir}.zip -rm {zip_dir}".format(zip_password=zip_password, father_dir=father_dir, zip_dir=zip_dir)
    ssh_client.exec_cmd(cmd)


def is_support_arch(ssh_client):
    """
    Determine if it is a supported operating system
    :return:
    """
    support_arch_list = ["i386", "i486", "i586", "alpha", "x86_64"]
    cmd = "arch"
    try:
        arch_info = ssh_client.exec_cmd(cmd)
        if arch_info.replace("\n", "") in support_arch_list:
            return True
        else:
            return False
    except:
        return False


def get_observer_version(context):
    """
    get observer version
    """
    stdio = context.stdio
    observer_version = ""
    try:
        stdio.verbose("get observer version, by sql")
        obcluster = context.cluster_config
        # by sql
        observer_version = get_observer_version_by_sql(obcluster, stdio)
    except Exception as e:
        try:
            stdio.verbose("get observer version, by sql fail. by ssh")
            nodes = context.cluster_config.get("servers")
            if len(nodes) > 0:
                sshclient = SshClient(context, nodes[0])
                ob_install_dir = nodes[0].get("home_path")
                observer_version = get_observer_version_by_ssh(sshclient, ob_install_dir, stdio)
        except Exception as e:
            raise Exception("get observer version fail.")
    if observer_version == "":
        raise Exception("get observer version fail.")
    return observer_version


def get_observer_version_by_ssh(ssh_client, ob_install_dir, stdio):
    """
    get observer version
    :return:
    """
    ob_version = ""
    cmd = "{ob_install_dir}/bin/observer --version".format(ob_install_dir=ob_install_dir)
    ob_version_info = ssh_client.exec_cmd(cmd)
    stdio.verbose("get observer version, run cmd = [{0}] ".format(cmd))
    if ob_version_info is not None:
        ob_version = re.findall(r'[(]OceanBase.(.+?)[)]', ob_version_info)
        if len(ob_version) > 0:
            result = re.sub(r'[a-zA-Z]', '', ob_version[0])
            return result.strip()
        else:
            cmd = "export LD_LIBRARY_PATH={ob_install_dir}/lib && {ob_install_dir}/bin/observer --version".format(ob_install_dir=ob_install_dir)
            ob_version_info = ssh_client.exec_cmd(cmd)
            stdio.verbose("get observer version with LD_LIBRARY_PATH,cmd:{0}".format(cmd))
            if "REVISION" not in ob_version_info:
                raise Exception("Please check conf about observer,{0}".format(ob_version_info))
            ob_version = re.findall(r'[(]OceanBase.*\s(.+?)[)]', ob_version_info)
            result = re.sub(r'[a-zA-Z]', '', ob_version[0])
            return result.strip()


def get_obproxy_version(context):
    """
    get obproxy version
    :return:
    """
    obproxy_version = ""
    stdio = context.stdio
    obproxy_nodes = context.obproxy_config.get("servers")
    if len(obproxy_nodes) < 1:
        raise Exception("obproxy_nodes is not exist. get_obproxy_version fail")
    obproxy_install_dir = obproxy_nodes[0]["home_path"]
    cmd = "{obproxy_install_dir}/bin/obproxy --version".format(obproxy_install_dir=obproxy_install_dir)
    ssh_client = SshClient(context, obproxy_nodes[0])
    obproxy_version_info = ssh_client.exec_cmd(cmd)
    stdio.verbose("get obproxy version, run cmd = [{0}] ".format(cmd))
    if obproxy_version_info is not None:
        pattern = r"(\d+\.\d+\.\d+\.\d+)"
        ob_version = re.findall(pattern, obproxy_version_info)
        if len(ob_version) > 0:
            return ob_version[0]
        else:
            cmd = "export LD_LIBRARY_PATH={obproxy_install_dir}/lib && {obproxy_install_dir}/bin/obproxy --version".format(obproxy_install_dir=obproxy_install_dir)
            obproxy_version_info = ssh_client.exec_cmd(cmd)
            stdio.verbose("get obproxy version with LD_LIBRARY_PATH,cmd:{0}, result:{1}".format(cmd, obproxy_version_info))
            if "REVISION" not in obproxy_version_info:
                raise Exception("Please check conf about proxy,{0}".format(obproxy_version_info))
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
    stdio.verbose("start get_observer_version_by_sql . input: {0}:{1}".format(ob_cluster.get("db_host"), ob_cluster.get("db_port")))
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


def get_observer_pid(ssh_client, ob_install_dir, stdio=None):
    """
    get observer pid
    :return:
    """
    try:
        cmd = "cat {ob_install_dir}/run/observer.pid".format(ob_install_dir=ob_install_dir)
        pids = ssh_client.exec_cmd(cmd)
        pid_list = pids.split()
        stdio.verbose("get observer pid, run cmd = [{0}], result:{1} ".format(cmd, pid_list))
    except:
        stdio.verbose("get observer pid failed")
        return []
    return pid_list


def delete_file_force(ssh_client, file_name, stdio=None):
    """
    delete file force
    :return:
    """
    cmd = "rm -rf {0}".format(file_name)
    ssh_client.exec_cmd(cmd)


def delete_empty_file(ssh_client, file_path, stdio=None):
    """
    delete empty file
    :return:
    """
    cmd = "find  {file_path} -name '*' -type f -size 0c | xargs -n 1 rm -f".format(file_path=file_path)
    ssh_client.exec_cmd(cmd)


def delete_file(ssh_client, file_path, stdio=None):
    """
    delete file
    :return:
    """
    cmd = "rm -rf {file_path}".format(file_path=file_path)
    ssh_client.exec_cmd(cmd)


def get_file_size(ssh_client, file_path, stdio=None):
    """
    get file size
    :return:
    """
    cmd = "ls -nl %s | awk '{print $5}'" % file_path
    return ssh_client.exec_cmd(cmd)


def is_empty_dir(ssh_client, dir_path, stdio=None):
    """
    is empty dir
    :return: True or False
    """
    cmd = "ls -A {dir_path}|wc -w".format(dir_path=dir_path)
    file_num = ssh_client.exec_cmd(cmd)
    if int(file_num) == 0:
        return True
    else:
        return False


def is_empty_file(ssh_client, file_path, stdio=None):
    """
    is empty file
    :return: True or False
    """
    file_size = get_file_size(ssh_client, file_path, stdio)
    if int(file_size) == 0:
        return True
    else:
        return False


# def get_obdiag_display(log_dir, trace_id, stdio=None):
#     cmd = 'grep -h "\[{}\]" {}* | sed "s/\[{}\] //g" '.format(trace_id, log_dir, trace_id)
#     stdout = LocalClient(stdio).run(cmd)
#     print_stdout = str(stdout).replace('\\n', '\n').replace('\\t', '\t')
#     if len(print_stdout) > 0:
#         print(print_stdout)


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


def find_home_path_by_port(ssh_client, internal_port_str, stdio):
    cmd = "ps aux | grep observer | grep 'P {internal_port_str}' |  grep -oP '/[^\s]*/bin/observer' ".format(internal_port_str=internal_port_str)
    stdout = ssh_client.exec_cmd(cmd)
    str_list = stdout.strip().split('\n')
    home_path = ""
    for original_str in str_list:
        original_str = str(original_str)
        if original_str.endswith("/bin/observer") and not original_str.startswith('/[^\s]*'):
            home_path = original_str.rstrip("/bin/observer")
            break
    stdio.verbose("home_path:{0}".format(home_path))
    return home_path
