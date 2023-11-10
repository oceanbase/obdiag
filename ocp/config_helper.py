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
import json
import os
import time
from collections import OrderedDict

from common.logger import logger
from common.ob_connector import OBConnector
from utils.decrypt_utils import AESCipher
from pick import pick
from ocp.ocp_base import OcpBase
from utils.file_utils import mkdir_if_not_exist
from utils.time_utils import timestamp_to_filename_time
from utils.yaml_utils import write_yaml_data, read_yaml_data, write_yaml_data_append, write_yaml_data_append_sorted


class ConfigHelper(object):
    def __init__(self, ocp_url, ocp_user, ocp_password, metadb_ip, metadb_port, metadb_user, metadb_password,
                 metadb_name):
        self.ocp_url = ocp_url
        self.ocp_user = ocp_user
        self.ocp_password = ocp_password
        self.metadb_ip = metadb_ip
        self.metadb_port = metadb_port
        self.metadb_user = metadb_user
        self.metadb_password = metadb_password
        self.metadb_name = metadb_name

    def __get__host_ip_list_by_cluster(self, args):
        obConnetcor = OBConnector(ip=self.metadb_ip,
                                  port=self.metadb_port,
                                  username=self.metadb_user,
                                  password=self.metadb_password,
                                  timeout=100)
        sql = "select rootserver_json from %s.ob_cluster where name = '%s' and ob_cluster_id = %d" \
              % (self.metadb_name, getattr(args, "cluster_name")[0], int(getattr(args, "cluster_id")[0]))
        res = obConnetcor.execute_sql(sql)
        if len(res) == 0:
            raise Exception("Failed to get the ocp host ip from ocp metadb by cluster name, "
                            "please check whether conf/config.yml correct!!!")
        rootserver_json_list = []
        for (row,) in res:
            rootserver_json_list.append(row)
        rslist_json_data = json.loads(rootserver_json_list[0])["RsList"]
        host_ip_list = []
        for data in rslist_json_data:
            host_ip_list.append(str(data["address"]).split(":")[0])
        return host_ip_list

    def __get_ocp_user_id_and_password(self):
        obConnetcor = OBConnector(ip=self.metadb_ip,
                                  port=self.metadb_port,
                                  username=self.metadb_user,
                                  password=self.metadb_password,
                                  timeout=100)
        sql = "select id,password from %s.iam_user where username='%s'" % (self.metadb_name, self.ocp_user)
        res = obConnetcor.execute_sql(sql)
        if len(res) == 0:
            raise Exception("Failed to get the ocp user id from conf/config.yml, "
                            "please check whether conf/config.yml correct!!!")
        return {"user_id": res[0][0], "password": res[0][1]}

    def __get_host_profile_credential_info(self, host_id):
        obConnetcor = OBConnector(ip=self.metadb_ip,
                                  port=self.metadb_port,
                                  username=self.metadb_user,
                                  password=self.metadb_password,
                                  timeout=100)
        user_id, password = self.__get_ocp_user_id_and_password()["user_id"], self.__get_ocp_user_id_and_password()[
            "password"]

        sql = "select a.target_id, c.secret from %s.profile_credential c inner join %s.profile_credential_access a " \
              "on c.id=a.credential_id where c.access_target='HOST' and c.user_id=%s and a.target_id = %d" % \
              (self.metadb_name, self.metadb_name, user_id, host_id)

        res = obConnetcor.execute_sql(sql)
        host_info = {}
        rslist_json_data = json.loads(res[0][1])
        host_info["ssh_type"] = rslist_json_data["sshType"]
        host_info["passphrase"] = rslist_json_data["passphrase"]
        host_info["allow_sudo"] = rslist_json_data["allowSudo"]
        host_info["user_name"] = rslist_json_data["username"]
        host_info["password"] = AESCipher(password).decrypt(rslist_json_data["passphrase"])
        return host_info

    def get_host_info_list_by_cluster(self, args):
        obConnetcor = OBConnector(ip=self.metadb_ip,
                                  port=self.metadb_port,
                                  username=self.metadb_user,
                                  password=self.metadb_password,
                                  timeout=100)
        sql = "select a.host_id, b.inner_ip_address, b.ssh_port from %s.compute_host_service a " \
              "inner join %s.compute_host b on a.host_id = b.id where a.name = '%s'" % (
                  self.metadb_name,
                  self.metadb_name,
                  getattr(args, "cluster_name")[0] + ":" + str(getattr(args, "cluster_id")[0]))
        res = obConnetcor.execute_sql(sql)
        if len(res) == 0:
            raise Exception("Failed to get the node from ocp metadb, "
                            "please check whether the cluster_name and cluster_id correct!!!")
        # get InstallPathlist attributes_json
        sql = "select  attributes_json from %s.ob_cluster where ob_cluster_id=%s" % (
            self.metadb_name, str(getattr(args, "cluster_id")[0]))

        res_attributes_json = obConnetcor.execute_sql(sql)
        if len(res_attributes_json) == 0:
            raise Exception("Failed to get attributes_json the node from ocp metadb, "
                            "please check whether the cluster_name and cluster_id correct!!!")
        attributes_json = res_attributes_json[0][0]
        if "InstallPath" not in json.loads(attributes_json):
            raise Exception("Failed to get InstallPath the node from ocp metadb, "
                            "please check whether the cluster_name and cluster_id correct!!!")
        install_path = json.loads(attributes_json)["InstallPath"]
        host_info_list = []
        for row in res:
            host_info = OrderedDict()
            host_info["ip"] = row[1]
            host_info["port"] = row[2]
            if int(row[0]) > 0:
                host_profile_credential_info = self.__get_host_profile_credential_info(row[0])
                host_info["user"] = host_profile_credential_info["user_name"]
                host_info["password"] = host_profile_credential_info["password"]
                host_info["private_key"] = ""
                host_info["home_path"] = install_path
            logger.debug("get host info: %s", host_info)
            host_info_list.append(host_info)
        return host_info_list

    def build_configuration(self, args, path):
        logger.info("Getting all the node information of the cluster, please wait a moment ...")
        try:
            ocp_base_init = OcpBase(self.ocp_url, self.ocp_user, self.ocp_password)
            ocp_base_init.check_ocp_site()
        except Exception as e:
            raise Exception("check login ocp failed, please check whether conf/config.yml is set correctly"
                            .format(e))
        all_host_info_list = self.get_host_info_list_by_cluster(args)
        logger.debug("get node list %s", all_host_info_list)
        all_host_ip_list = []
        selected_host_ip_list = []
        for host in all_host_info_list:
            all_host_ip_list.append(host["ip"])
        if len(all_host_ip_list) == 0:
            raise Exception("Failed to get the node ip list")
        title = 'press SPACE to mark, ENTER to continue'
        options = all_host_ip_list
        selected_host = pick(options, title, multiselect=True, min_selection_count=1)

        for host in selected_host:
            selected_host_ip_list.append(host[0])
        logger.info("You have selected the following nodes {0}".format(selected_host_ip_list))

        selected_host_info_list = []
        for host in all_host_info_list:
            if host["ip"] in selected_host_ip_list:
                selected_host_info_list.append(host)

        old_config = self.get_old_configuration(path)
        # backup old config
        self.save_old_configuration(old_config)
        # rewrite config
        obdiag_config = old_config["OBDIAG"]
        write_yaml_data({"OBDIAG": obdiag_config}, path)
        ocp_config = old_config["OCP"]
        write_yaml_data_append({"OCP": ocp_config}, path)
        ob_cluster_config = old_config["OBCLUSTER"]
        write_yaml_data_append({"OBCLUSTER": ob_cluster_config}, path)
        write_yaml_data_append({"NODES": selected_host_info_list}, path)
        # add checker conf
        checker_conf = old_config["CHECK"]
        write_yaml_data_append({"CHECK": checker_conf}, path)
        logger.info("Node information has been rewritten to the configuration file conf/config.yml, "
                    "and you can enjoy the gather journey !")

    def get_old_configuration(self, path):
        data = read_yaml_data(path)
        return data

    def save_old_configuration(self, config):
        backup_config_dir = os.path.abspath(config["OBDIAG"]["BASIC"]["config_backup_dir"])
        filename = "config_backup_{0}.yml".format(timestamp_to_filename_time(int(round(time.time() * 1000))))
        backup_config_path = os.path.join(backup_config_dir, filename)
        mkdir_if_not_exist(backup_config_dir)
        write_yaml_data(config, backup_config_path)
