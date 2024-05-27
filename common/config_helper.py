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

from common.command import get_observer_version_by_sql
from common.constant import const
from common.ob_connector import OBConnector
from common.tool import DirectoryUtil
from common.tool import TimeUtils
from common.tool import StringUtils
from common.tool import YamlUtils
from common.tool import Util


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

    def get_cluster_name(self):
        ob_version = get_observer_version_by_sql(self.ob_cluster, self.stdio)
        obConnetcor = OBConnector(ip=self.db_host, port=self.db_port, username=self.sys_tenant_user, password=self.sys_tenant_password, stdio=self.stdio, timeout=100)
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
        ob_version = get_observer_version_by_sql(self.ob_cluster, self.stdio)
        obConnetcor = OBConnector(ip=self.db_host, port=self.db_port, username=self.sys_tenant_user, password=self.sys_tenant_password, stdio=self.stdio, timeout=100)
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
        print("\033[33mPlease enter the following configuration !!!\033[0m")
        global_ssh_username = self.input_with_default("oceanbase host ssh username", "")
        global_ssh_password = self.input_password_with_default("oceanbase host ssh password", "")
        global_ssh_port = self.input_with_default("oceanbase host ssh_port", "22")
        global_home_path = self.input_with_default("oceanbase install home_path", const.OB_INSTALL_DIR_DEFAULT)
        default_data_dir = os.path.join(global_home_path, "store")
        global_data_dir = self.input_with_default("oceanbase data_dir", default_data_dir)
        global_redo_dir = self.input_with_default("oceanbase redo_dir", default_data_dir)
        tenant_sys_config = {"user": self.sys_tenant_user, "password": self.sys_tenant_password}
        global_config = {"ssh_username": global_ssh_username, "ssh_password": global_ssh_password, "ssh_port": global_ssh_port, "ssh_key_file": "", "home_path": global_home_path, "data_dir": global_data_dir, "redo_dir": global_redo_dir}
        new_config = {"obcluster": {"ob_cluster_name": ob_cluster_name, "db_host": self.db_host, "db_port": self.db_port, "tenant_sys": tenant_sys_config, "servers": {"nodes": nodes_config, "global": global_config}}}
        YamlUtils.write_yaml_data(new_config, self.config_path)
        need_config_obproxy = self.input_choice_default("need config obproxy [y/N]", "N")
        if need_config_obproxy:
            self.build_obproxy_configuration(self.config_path)
        self.stdio.verbose("Node information has been rewritten to the configuration file {0}, and you can enjoy the journey !".format(self.config_path))

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
