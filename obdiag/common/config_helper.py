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
@time: 2022/6/25
@file: config_helper.py
@desc:
"""
import os
import pwinput
import time
from collections import OrderedDict
import yaml

from obdiag.common.command import get_observer_version
from obdiag.common.constant import const
from obdiag.common.ob_connector import OBConnector
from obdiag.common.ssh_client.base import SsherClient
from obdiag.common.tool import DirectoryUtil
from obdiag.common.tool import TimeUtils
from obdiag.common.tool import StringUtils
from obdiag.common.tool import YamlUtils
from obdiag.common.tool import Util


class ConfigHelper(object):
    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        options = self.context.options
        self.sys_tenant_user = Util.get_option(options, 'u')
        self.sys_tenant_password = Util.get_option(options, 'p')
        self.db_host = Util.get_option(options, 'h')
        self.db_port = Util.get_option(options, 'P')
        self.config_path = os.path.expanduser('~/.obdiag/config.yml')
        self.inner_config = self.context.inner_config
        self.ob_cluster = {
            "db_host": self.db_host,
            "db_port": self.db_port,
            "tenant_sys": {
                "password": self.sys_tenant_password,
                "user": self.sys_tenant_user,
            },
        }
        context.cluster_config = self.ob_cluster

    def get_cluster_name(self):
        ob_version = get_observer_version(self.context)
        obConnetcor = OBConnector(context=self.context, ip=self.db_host, port=self.db_port, username=self.sys_tenant_user, password=self.sys_tenant_password, timeout=100)
        if ob_version.startswith("3") or ob_version.startswith("2"):
            sql = "select cluster_name from oceanbase.v$ob_cluster"
            res = obConnetcor.execute_sql(sql)
            if len(res) == 0:
                self.stdio.error("Failed to get cluster name, please check whether the cluster config correct!!!")
            else:
                return res[0][0]
        else:
            return "obcluster"

    def get_host_info_list_by_cluster(self):
        ob_version = get_observer_version(self.context)
        obConnetcor = OBConnector(context=self.context, ip=self.db_host, port=self.db_port, username=self.sys_tenant_user, password=self.sys_tenant_password, timeout=100)
        sql = "select SVR_IP, SVR_PORT, ZONE, BUILD_VERSION from oceanbase.DBA_OB_SERVERS"
        if ob_version.startswith("3") or ob_version.startswith("2") or ob_version.startswith("1"):
            sql = "select SVR_IP, SVR_PORT, ZONE, BUILD_VERSION from oceanbase.__all_server"
        res = obConnetcor.execute_sql(sql)
        if len(res) == 0:
            raise Exception("Failed to get the node from cluster config, " "please check whether the cluster config correct!!!")
        host_info_list = []
        for row in res:
            host_info = OrderedDict()
            host_info["ip"] = row[0]
            self.stdio.verbose("get host info: %s", host_info)
            host_info_list.append(host_info)
        return host_info_list

    def build_configuration_by_file(self, file_path=""):
        if file_path == "":
            raise Exception("Please input the configure file path!!!")
        if not os.path.exists(os.path.expanduser(file_path)):
            raise Exception("The configure file path is not exist!!!")
        file_path = os.path.expanduser(file_path)
        if file_path.endswith(".ini"):
            self.build_configuration_by_ini(file_path)
        elif file_path.endswith(".yaml"):
            self.build_configuration_by_yaml(file_path)
        else:
            raise Exception("The file path: {0} is not support!!!".format(file_path))

    def build_configuration_by_ini(self, ini_file_path):
        import os

        self.stdio.print("ini_file_path: ", os.path.expanduser(ini_file_path))

        def parse_config(file_content):
            config_ini_dict = {}
            current_section = None
            # all host
            observer_hosts = []
            config_ini_dict["observer_hosts"] = observer_hosts
            obproxy_hosts = []

            for line in file_content.splitlines():
                # 去除行首行尾空白
                line = line.strip()

                # 跳过空行和注释行
                if not line or line.startswith("#"):
                    continue
                # 分割键和值
                key, value = line.split('=', 1)
                key = key.strip()  # 移除键前后的空白

                value = value.strip()  # 移除值前后的空白
                if "obs_hosts" in key:
                    observer_hosts.append(value)
                    continue
                if "proxy_hosts" in key:
                    obproxy_hosts.append(value)
                    continue
                if "workdir" in key:
                    config_ini_dict[key] = value
                    continue
                if "clog_path" in key:
                    config_ini_dict[key] = value
                    continue
                if "data_path" in key:
                    config_ini_dict[key] = value
                    continue
                if "port_base" in key:
                    config_ini_dict[key] = value
                    continue
                if "proxy_port" in key:
                    config_ini_dict[key] = value
                    continue

                # 存储到字典中
                if current_section is not None:
                    config_ini_dict[current_section][key] = value
                else:
                    # 如果没有明确的节，则直接存储到顶层字典
                    config_ini_dict[key] = value
                observer_hosts = list(set(observer_hosts))
                obproxy_hosts = list(set(obproxy_hosts))
            config_ini_dict["obproxy_hosts"] = obproxy_hosts
            config_ini_dict["observer_hosts"] = observer_hosts
            return config_ini_dict

        with open(ini_file_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
            # get all useful config map
            config_ini_dict = parse_config(file_content)
            # print(json.dumps(config_ini_dict))

        config_yml_dict = {"obcluster": None, "obproxy": None}
        config_yml_obcluster = {
            "ob_cluster_name": "obcluster",
            "db_host": "127.0.0.1",
            "db_port": 2881,
            "tenant_sys": {"user": "root@sys", "password": ""},
            "db_user": "root@sys",
            "servers": {"nodes": [], "global": {}},
        }
        config_yml_obproxy = {
            "obproxy_cluster_name": "obcluster",
            "servers": {"nodes": [], "global": {}},
        }

        # set config_yml_obcluster ssh user and password
        import os

        local_user = os.getlogin()
        config_yml_obcluster["servers"]["global"]["ssh_username"] = local_user
        config_yml_obcluster["servers"]["global"]["ssh_password"] = ""
        config_yml_obcluster["servers"]["global"]["ssh_port"] = "22"
        config_yml_obcluster["servers"]["global"]["ssh_key_file"] = ""
        config_yml_obproxy["servers"]["global"]["ssh_username"] = local_user
        config_yml_obproxy["servers"]["global"]["ssh_password"] = ""
        config_yml_obproxy["servers"]["global"]["ssh_port"] = "22"
        config_yml_obproxy["servers"]["global"]["ssh_key_file"] = ""
        # get home_path on node
        # observer
        work_dir = config_ini_dict["workdir"]
        observer_hosts = config_ini_dict["observer_hosts"]
        if len(observer_hosts) == 0:
            self.stdio.error("observer_hosts is empty")
            raise Exception("observer_hosts is empty")
        else:
            for observer_host in observer_hosts:
                # get observer info by ssh
                ## get home_path
                node = {"ip": observer_host, "ssh_port": 22, "ssh_username": local_user, "ssh_password": ""}
                sshhelper = SsherClient(context=self.context, node=node)
                all_dir_name = sshhelper.run_cmd("ls {0}".format(work_dir))
                for dir_name in all_dir_name:
                    if "obs" in dir_name:
                        home_path = "{0}/{1}".format(work_dir, dir_name)
                        config_yml_obcluster["servers"]["nodes"].append({"host": observer_host, "home_path": home_path, "data_dir": home_path + "/store", "redo_dir": home_path + "/store"})
                        continue

        obproxy_hosts = config_ini_dict["obproxy_hosts"]
        if len(obproxy_hosts) == 0:
            print("obproxy_hosts is empty")
            raise Exception("obproxy_hosts is empty")
        else:
            for obproxy_host in obproxy_hosts:
                # get obproxy info by ssh
                node = {"ip": obproxy_host, "ssh_port": 22, "ssh_username": local_user, "ssh_password": ""}
                sshhelper = SsherClient(context=self.context, node=node)
                all_dir_name = sshhelper.exec_cmd("ls {0}".format(work_dir))
                for dir_name in all_dir_name:
                    if "obproxy" in dir_name:
                        home_path = "{0}/{1}".format(work_dir, dir_name)
                        config_yml_obproxy["servers"]["nodes"].append({"host": obproxy_host, "home_path": home_path})
                        continue

        # db_host
        db_host = config_yml_obcluster["servers"]["nodes"][0]["host"]
        port_base = config_ini_dict.get("port_base") or 2881 - 35
        config_yml_obcluster["db_host"] = db_host
        config_yml_obcluster["db_port"] = port_base

        # merge
        config_yml_dict["obcluster"] = config_yml_obcluster
        config_yml_dict["obproxy"] = config_yml_obproxy

        # print(json.dumps(config_yml_dict))
        # 将字典转换为YAML
        yaml_output = yaml.dump(config_yml_dict, default_flow_style=False)
        if os.path.exists(os.path.expanduser("~/.obdiag/config.yml")):
            if os.path.exists(os.path.expanduser("~/.obdiag/config.yml.d")):
                os.remove(os.path.expanduser("~/.obdiag/config.yml.d"))
            os.renames(os.path.expanduser("~/.obdiag/config.yml"), os.path.expanduser("~/.obdiag/config.yml.d"))
        with open(os.path.expanduser("~/.obdiag/config.yml"), "w", encoding="utf-8") as f:
            f.write(yaml_output)
            self.stdio.print("Build configuration success, please check ~/.obdiag/config.yml")
        return

    def build_configuration_by_yaml(self, file_path):
        import os

        self.stdio.print("yaml_file_path: ", os.path.expanduser(file_path))
        with open(file_path, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f)
        obcluster = None
        obproxy = None
        # get all ip
        if "oceanbase-ce" in yaml_data:
            obcluster = yaml_data["oceanbase-ce"]
        if "oceanbase" in yaml_data:
            obcluster = yaml_data["oceanbase"]

        if "obproxy-ce" in yaml_data:
            obproxy = yaml_data["obproxy-ce"]

        if "obproxy" in yaml_data:
            obproxy = yaml_data["obproxy"]
        data_obcluster_servers = []

        # get global info
        obcluster_global_info = obcluster.get("global") or {}
        # get server key
        obcluster_servers = obcluster["servers"]
        ob_conn_info = {}
        for obcluster_server_data in obcluster_servers:
            obcluster_server = {}
            obcluster_server_name = obcluster_server_data["name"]  # get server name
            # ob_conn_info host flush
            ob_conn_info["db_host"] = obcluster_server_data["ip"]
            # get ip
            obcluster_server["ip"] = obcluster_server_data["ip"]  # get server ip
            # ob_conn_info port flush
            ob_conn_info["db_port"] = obcluster[obcluster_server_name].get("mysql_port") or obcluster_global_info.get("mysql_port") or 2881
            # get home_path
            obcluster_server["home_path"] = obcluster[obcluster_server_name].get("home_path") or obcluster_global_info.get("home_path") or ""
            # set data_dir
            obcluster_server["data_dir"] = os.path.join(obcluster_server["home_path"], "store")
            # set redo_dir
            obcluster_server["redo_dir"] = os.path.join(obcluster_server["home_path"], "store")
            data_obcluster_servers.append(obcluster_server)
        config_yml_dict = {"obcluster": None, "obproxy": None}
        config_yml_obcluster = {
            "ob_cluster_name": "obcluster",
            "db_host": ob_conn_info["db_host"],
            "db_port": int(ob_conn_info["db_port"]),
            "tenant_sys": {"user": "root@sys", "password": ""},
            "db_user": "root@sys",
            "servers": {"nodes": data_obcluster_servers, "global": {}},
        }
        local_user = os.getlogin()
        config_yml_obcluster["servers"]["global"]["ssh_username"] = local_user
        config_yml_obcluster["servers"]["global"]["ssh_password"] = ""
        config_yml_obcluster["servers"]["global"]["ssh_port"] = "22"
        config_yml_obcluster["servers"]["global"]["ssh_key_file"] = ""
        config_yml_dict["obcluster"] = config_yml_obcluster
        # get obproxy info
        if obproxy is not None:
            config_yml_obproxy = {
                "obproxy_cluster_name": "obcluster",
                "servers": {"nodes": [], "global": {}},
            }
            obproxy_node = []
            obproxy_global = obproxy.get("global") or {}
            for obproxy_server in obproxy["servers"]:
                obproxy_node.append(obproxy_server)
            config_yml_obproxy["servers"]["nodes"] = obproxy_node
            config_yml_obproxy["servers"]["global"]["ssh_username"] = local_user
            config_yml_obproxy["servers"]["global"]["ssh_password"] = ""
            config_yml_obproxy["servers"]["global"]["ssh_port"] = "22"
            config_yml_obproxy["servers"]["global"]["ssh_key_file"] = ""
            config_yml_obproxy["servers"]["global"]["home_path"] = obproxy_global.get("home_path") or "~/"
            config_yml_dict["obproxy"] = config_yml_obproxy
        # dict to yaml
        yaml_output = yaml.dump(config_yml_dict, default_flow_style=False)
        if os.path.exists(os.path.expanduser("~/.obdiag/config.yml")):
            if os.path.exists(os.path.expanduser("~/.obdiag/config.yml.d")):
                os.remove(os.path.expanduser("~/.obdiag/config.yml.d"))
            os.renames(os.path.expanduser("~/.obdiag/config.yml"), os.path.expanduser("~/.obdiag/config.yml.d"))
        with open(os.path.expanduser("~/.obdiag/config.yml"), "w", encoding="utf-8") as f:
            f.write(yaml_output)
            self.stdio.print("Build configuration success, please check ~/.obdiag/config.yml")
        return

    def build_configuration(self):
        self.stdio.verbose("Getting all the node information of the cluster, please wait a moment ...")
        all_host_info_list = self.get_host_info_list_by_cluster()
        self.stdio.verbose("get node list %s", all_host_info_list)
        all_host_ip_list = []
        for host in all_host_info_list:
            all_host_ip_list.append(host["ip"])
        if len(all_host_ip_list) == 0:
            raise Exception("Failed to get the node ip list")
        nodes_config = []
        for i in all_host_ip_list:
            nodes_config.append({"ip": i})
        old_config = self.get_old_configuration(self.config_path)
        # backup old config
        self.save_old_configuration(old_config)
        # rewrite config
        ob_cluster_name = self.get_cluster_name()
        self.stdio.print("\033[33mPlease enter the following configuration !!!\033[0m")
        global_ssh_username = self.input_with_default("oceanbase host ssh username", "")
        use_password_tag = self.input_choice_by_nu("use password or key file (0:use password; 1:use key file) default: 0", 0)
        global_ssh_password = ""
        global_ssh_key_file = ""
        if use_password_tag == 0:
            global_ssh_password = self.input_password_with_default("oceanbase host ssh password", "")
        elif use_password_tag == 1:
            global_ssh_key_file = self.input_with_default("oceanbase host ssh key file", "~/.ssh/id_rsa")
        else:
            self.stdio.warn("Invalid input, use default: use password")
            global_ssh_password = self.input_password_with_default("oceanbase host ssh password", "")
        global_ssh_port = self.input_with_default("oceanbase host ssh_port", "22")
        global_home_path = self.input_with_default("oceanbase install home_path", const.OB_INSTALL_DIR_DEFAULT)
        default_data_dir = os.path.join(global_home_path, "store")
        global_data_dir = default_data_dir
        global_redo_dir = default_data_dir
        tenant_sys_config = {"user": self.sys_tenant_user, "password": self.sys_tenant_password}
        global_config = {"ssh_username": global_ssh_username, "ssh_password": global_ssh_password, "ssh_port": global_ssh_port, "ssh_key_file": global_ssh_key_file, "home_path": global_home_path, "data_dir": global_data_dir, "redo_dir": global_redo_dir}
        new_config = {"obcluster": {"ob_cluster_name": ob_cluster_name, "db_host": self.db_host, "db_port": self.db_port, "tenant_sys": tenant_sys_config, "servers": {"nodes": nodes_config, "global": global_config}}}
        YamlUtils.write_yaml_data(new_config, self.config_path)
        need_config_obproxy = self.input_choice_default("need config obproxy [y/N]", "N")
        if need_config_obproxy:
            self.build_obproxy_configuration(self.config_path)
        self.stdio.print("Node information has been rewritten to the configuration file {0}, and you can enjoy the journey !".format(self.config_path))

    def build_obproxy_configuration(self, path):
        obproxy_servers = self.input_with_default("obproxy server eg:'192.168.1.1;192.168.1.2;192.168.1.3'", "")
        obproxy_server_list = StringUtils.split_ip(obproxy_servers)
        if len(obproxy_server_list) > 0:
            nodes_config = []
            for server in obproxy_server_list:
                nodes_config.append({"ip": server})
            global_ssh_username = self.input_with_default("obproxy host ssh username", "")
            global_ssh_password = self.input_password_with_default("obproxy host ssh password", "")
            global_ssh_port = self.input_with_default("obproxy host ssh port", "22")
            global_home_path = self.input_with_default("obproxy install home_path", const.OBPROXY_INSTALL_DIR_DEFAULT)
            global_config = {
                "ssh_username": global_ssh_username,
                "ssh_password": global_ssh_password,
                "ssh_port": global_ssh_port,
                "ssh_key_file": "",
                "home_path": global_home_path,
            }
            new_config = {"obproxy": {"obproxy_cluster_name": "obproxy", "servers": {"nodes": nodes_config, "global": global_config}}}
            YamlUtils.write_yaml_data_append(new_config, path)

    def get_old_configuration(self, path):
        try:
            data = YamlUtils.read_yaml_data(path)
            return data
        except:
            pass

    def save_old_configuration(self, config):
        backup_config_dir = os.path.expanduser(self.inner_config["obdiag"]["basic"]["config_backup_dir"])
        filename = "config_backup_{0}.yml".format(TimeUtils.timestamp_to_filename_time(int(round(time.time() * 1000000))))
        backup_config_path = os.path.join(backup_config_dir, filename)
        DirectoryUtil.mkdir(path=backup_config_dir)
        YamlUtils.write_yaml_data(config, backup_config_path)

    def input_with_default(self, prompt, default):
        value = input("\033[32mEnter your {0} (default:'{1}'): \033[0m".format(prompt, default)).strip()
        if value == '' or value.lower() == "y" or value.lower() == "yes":
            return default
        else:
            return value

    def input_password_with_default(self, prompt, default):
        value = pwinput.pwinput(prompt="\033[32mEnter your {0} (default:'{1}'): \033[0m".format(prompt, default), mask='*')
        if value == '' or value.lower() == "y" or value.lower() == "yes":
            return default
        else:
            return value

    def input_choice_default(self, prompt, default):
        value = input("\033[32mEnter your {0} (default:'{1}'): \033[0m".format(prompt, default)).strip()
        if value.lower() == "y" or value.lower() == "yes":
            return True
        else:
            return False

    def input_choice_by_nu(self, prompt, default):
        value = input("\033[32mEnter your {0}: \033[0m".format(prompt)).strip()
        if value == '':
            return int(default)
        if not value.isdigit():
            self.stdio.error("The number is invalid! Please re-enter.")
            return self.input_choice_by_nu(prompt, default)
        return int(value)
