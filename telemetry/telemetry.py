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
@time: 2023/11/24
@file: telemetry.py
@desc:
"""
import hmac
import http.client
import json
import threading
import time
import hashlib
from io import open
from common.constant import const
from common.ob_connector import OBConnector
from common.tool import NetUtils
from common.tool import DateTimeEncoder
from common.version import get_obdiag_version
import ssl
from stdio import IO

ssl._create_default_https_context = ssl._create_unverified_context
class Telemetry():
    def __init__(self):
        self.obversion = None
        self.ob_connector = None
        self.reporter = "obdiag"
        self.cluster_info = None
        self.tenant_info = None
        self.cmd_info = None
        self.check_info = {}
        self.cluster_conn = None
        self.threads = []
        self.work_tag = True
        self.version = get_obdiag_version()
        self.stdio=IO(1)

    def set_cluster_conn(self, obcluster):
        try:
            if not self.work_tag:
                return
            if self.work_tag:
                self.work_tag = NetUtils.network_connectivity("https://" + const.TELEMETRY_URL + const.TELEMETRY_PATH)
            if not self.work_tag:
                return

            if obcluster is not None:
                try:

                    self.cluster_conn = OBConnector(ip=obcluster.get("db_host"),
                                                    port=obcluster.get("db_port"),
                                                    username=obcluster.get("tenant_sys").get("user"),
                                                    password=obcluster.get("tenant_sys").get("password"),
                                                    stdio=self.stdio,
                                                    timeout=10000)
                    self.threads.append(threading.Thread(None, self.get_cluster_info()))
                   # self.threads.append(threading.Thread(None, self.get_tenant_info()))
                    for thread in self.threads:
                        thread.start()
                except Exception as e:
                    pass
        except Exception as e:
            pass


    def get_cluster_info(self):
        if self.cluster_conn is not None:
            try:
                data = None
                version = str(self.cluster_conn.execute_sql("select version();")[0][0])
                if "-v4" in version:
                    cursor = self.cluster_conn.execute_sql_return_cursor_dictionary(
                        "select * from oceanbase.GV$OB_SERVERS;")
                    columns = [column[0] for column in cursor.description]
                    data = cursor.fetchall()
                    for data_one in data:
                        data_one["SVR_IP"] = ip_mix_by_sha256(data_one["SVR_IP"])
                elif version.startswith("3."):
                    cursor = self.cluster_conn.execute_sql_return_cursor_dictionary(
                        "select *from oceanbase.gv$unit u, oceanbase.__all_virtual_server_stat s where s.svr_ip=u.svr_ip and s.svr_port=u.svr_port")
                    columns = [column[0] for column in cursor.description]
                    data = cursor.fetchall()
                    for data_one in data:
                        data_one["svr_ip"] = ip_mix_by_sha256(data_one["svr_ip"])
                self.obversion=version
                self.cluster_info = json.dumps(data)
                self.cluster_info["obversion"] = version
            except Exception as e:
                pass
        return

    def get_tenant_info(self):
        if self.cluster_conn is not None:
            try:
                data = None
                version = str(self.cluster_conn.execute_sql("select version();")[0][0])
                if "-v4" in version:
                    cursor = self.cluster_conn.execute_sql_return_cursor_dictionary(
                        "SELECT * FROM OCEANBASE.DBA_OB_TENANTS t1,OCEANBASE.DBA_OB_UNITS t2,OCEANBASE.DBA_OB_UNIT_CONFIGS t3,OCEANBASE.DBA_OB_RESOURCE_POOLS t4 where t1.tenant_id = t4.tenant_id AND t4.resource_pool_id=t2.resource_pool_id AND t4.unit_config_id=t3.unit_config_id ORDER BY t1.tenant_name;")
                    columns = [column[0] for column in cursor.description]
                    data = cursor.fetchall()
                    for data_one in data:
                        if "SVR_IP" in data_one:
                            data_one["SVR_IP"] = ip_mix_by_sha256(data_one.get("SVR_IP"))
                elif version.startswith("3."):
                    cursor = self.cluster_conn.execute_sql_return_cursor_dictionary(
                        "SELECT * FROM OCEANBASE.gv$tenant t1,OCEANBASE.gv$unit t2 where t1.tenant_id = t2.tenant_id;")
                    columns = [column[0] for column in cursor.description]
                    data = cursor.fetchall()
                    for data_one in data:
                        if "svr_ip" in data_one:
                            data_one["svr_ip"] = ip_mix_by_sha256(data_one.get("svr_ip"))

                self.tenant_info = json.dumps(data, cls=DateTimeEncoder)
            except Exception as e:
                pass
        return

    def push_cmd_info(self, cmd_info):
        if not self.work_tag:
            return
        self.cmd_info = str(cmd_info)
        return

    def push_check_info(self, target, check_info):
        if not self.work_tag:
            return
        self.check_info[target] = check_info
        return

    def put_data(self):
        if not self.work_tag:
            return
        try:
            for thread in self.threads:
                thread.join()
            report_data = {"reporter": const.TELEMETRY_CONTENT_REPORTER, "eventId": ip_mix_by_sha256(str(time.time())), "obdiagVersion": get_obdiag_version()}
            if self.cluster_info is not None:
                report_data["cluster_info"] = self.cluster_info
            if self.tenant_info is not None:
                report_data["tenant_info"] = self.tenant_info
            if self.cmd_info is not None:
                report_data["cmd_info"] = self.cmd_info
            if self.check_info is not None:
                report_data["check_info"] = self.check_info
            if self.obversion is not None:
                report_data["obversion"]=self.obversion

            re = {"content": report_data,"component":"obdiag"}

            # put to /tmp
            with open(const.OBDIAG_TELEMETRY_FILE_NAME, 'w', encoding="utf8") as f:
                f.write(json.dumps(re, ensure_ascii=False))
            self.put_info_to_oceanbase()

        except Exception as e:
            pass
        return

    def put_info_to_oceanbase(self):
        if not self.work_tag:
            return
        try:
            conn = http.client.HTTPSConnection(const.TELEMETRY_URL, timeout=(5))
            with open(const.OBDIAG_TELEMETRY_FILE_NAME, 'rb') as file:
                payload = file.read()
            headers = {
                'Content-Encoding': 'application/gzip',
                'Content-Type': 'application/json'
            }
            conn.request("POST", const.TELEMETRY_PATH, payload, headers)
            res = conn.getresponse()
        except :
            pass




key="********"
def ip_mix_by_sha256(ip):
    ip = ip.encode('utf-8')
    return hmac.new(key.encode('utf-8'), ip, digestmod=hashlib.sha256).hexdigest().upper()
def ip_mix_by_sha1(ip=""):
    sha1 = hashlib.sha1()
    sha1.update(ip.encode())
    return sha1.hexdigest()


telemetry = Telemetry()


