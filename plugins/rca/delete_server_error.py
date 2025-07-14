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
@time: 2025/06/30
@file: delete_server_error.py
@desc: 诊断删除OceanBase集群中observer节点时遇到的问题
"""
import os
from typing import List, Dict, Optional, Any

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene
from src.common.tool import StringUtils


class DeleteServerError(RcaScene):
    def __init__(self):
        super().__init__()
        self.svr_ip = None
        self.svr_port = None
        self.work_path = None

    def init(self, context):
        super().init(context)
        ## observer version>4.0.0.0
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            self.stdio.error("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
            raise RCAInitException("observer version is {0}, which is less than 4.0.0.0.".format(observer_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.work_path = context.get_variable('store_dir')
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        self.svr_ip = self.input_parameters.get("svr_ip")
        self.svr_port = self.input_parameters.get("svr_port")
        if self.svr_ip is None or self.svr_port is None:
            raise RCAInitException("svr_ip or svr_port is None. Please check the --env.")

    def verbose(self, info):
        self.stdio.verbose("[DeleteServerError] {0}".format(info))

    def execute(self):
        try:
            if self.ob_connector is None:
                raise RCAInitException("ob_connector is None. Please check the NODES conf.")
            self.record.add_record("start check enable_rebalance by: show parameters like 'enable_rebalance';")
            sql = "show parameters like 'enable_rebalance';"
            self.verbose("get enable_rebalance execute_sql is {0}".format(sql))
            enable_rebalance_data = self.__execute_sql_with_save(sql, "enable_rebalance")
            enable_rebalance_false_nodes = []
            for enable_rebalance_data_item in enable_rebalance_data:
                value = enable_rebalance_data_item['value'].lower()
                if value == "true":
                    pass
                else:
                    enable_rebalance_false_nodes.append(enable_rebalance_data_item['svr_ip'])
            for enable_rebalance_false_node in enable_rebalance_false_nodes:
                self.record.add_record("node {0} enable_rebalance is false. when delete server, enable_rebalance should be True".format(enable_rebalance_false_node))

            # 获得server上对应的tenant列表，    	 select tenant_id from oceanbase.__all_resource_pool where resource_pool_id in (select resource_pool_id from oceanbase.__all_unit where svr_ip = '$DELETE_SERVER_PORT' and svr_port = '$DELETE_SERVER_PORT');
            sql = "select tenant_id from oceanbase.__all_resource_pool where resource_pool_id in (select resource_pool_id from oceanbase.__all_unit where svr_ip = '{0}' and svr_port = '{1}');".format(self.svr_ip, self.svr_port)
            self.record.add_record("start check tenant by: {0}".format(sql))
            self.verbose("get tenant execute_sql is {0}".format(sql))
            tenant_datas = self.__execute_sql_with_save(sql, "tenant")
            for tenant_data in tenant_datas:
                if tenant_data['tenant_id'] == 0:
                    continue
                # 此时还在进行unit迁移以及副本的迁移
                self.record.add_record("tenant {0} is in server {1}:{2}, Unit migration and replica migration are still in progress.".format(tenant_data['tenant_id'], self.svr_ip, self.svr_port))
                self.record.add_suggest("You can wait for the unit migration and replica migration to complete.")
                return
            # check node is only one of the zone
            ## get the zone of the node
            sql = "select zone from oceanbase.DBA_OB_SERVERS where svr_ip='{0}' and svr_port={1};".format(self.svr_ip, self.svr_port)
            self.verbose("get zone execute_sql is {0}".format(sql))
            zone_data = self.__execute_sql_with_save(sql, "node_zone")
            zone = zone_data[0]['zone']
            ## get the zone of all the nodes
            sql = "SELECT * FROM oceanbase.DBA_OB_SERVERS where zone='{0}';".format(zone)
            self.verbose("get zone execute_sql is {0}".format(sql))
            zone_datas = self.__execute_sql_with_save(sql, "zone_nodes")
            if len(zone_datas) <= 1:
                self.record.add_record("node {0} is only one of the zone {1}".format(self.svr_ip, zone))
                self.record.add_suggest("You can'not delete the node. because the node is only one of the zone")
                return
            # gather log and check "fail to get loads by server" exist
            self.gather_log.set_parameters("scope", "observer")

            self.gather_log.grep("fail to get loads by server")
            logs_name = self.gather_log.execute(save_path="all_logs")
            if len(logs_name) > 0:
                for log_name in logs_name:
                    with open(log_name, 'r', encoding="utf-8", errors="ignore") as f:
                        content = f.read()
                        if "fail to get loads by server" in content:
                            self.record.add_record("node {0} has log about 'fail to get loads by server'".format(self.svr_ip))
                            # 解决方法：1. 首先判断该server上是否还存在着partition。
                            #    2. 若无partition。那么先将该server kill掉。
                            #    3. 将__all_server 表中的记录delete掉。
                            #    4. reload一下内存状态。
                            self.record.add_suggest("Please send the log to the OceanBase community for further analysis.")
                            return
                return

        except RCANotNeedExecuteException as e:
            self.stdio.print("[Not Need Execute]DeleteServerError need not execute: {0}".format(e))
        except Exception as e:
            raise RCAExecuteException("DeleteServerError execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end DeleteServerError execute")

    def __execute_sql_with_save(self, sql: str, save_file_name: str):
        try:
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            data = cursor.fetchall()
            self.verbose("the sql:{1} .data is {0}".format(data, sql))
            if len(data) <= 0:
                self.record.add_record("sql: {0} execute result is empty.".format(sql))
                return []
            columns = [desc[0] for desc in cursor.description]
            data_save_path = os.path.join(self.work_path, "{}.txt".format(save_file_name))
            with open(data_save_path, 'w') as f:
                f.write('\t'.join(columns) + '\n')
                for row in data:
                    line = ""
                    for item in row:
                        line += "{}\t".format(row[item])
                    f.write(line + '\n')
            return data
        except Exception as e:
            raise RCAExecuteException("ReplayHoldScene __execute_with_save execute error: {0}".format(e))

    def get_scene_info(self):

        return {
            "name": "delete_server_error",
            "info_en": "Diagnose issues during observer node removal in the cluster",
            "info_cn": '排查删除 observer 节点时遇到的问题',
            "example": "obdiag rca run --scene=delete_server_error --env svr_ip=xxx.xxx.xxx.xxx --env svr_port=2881",
        }


delete_server_error = DeleteServerError()
