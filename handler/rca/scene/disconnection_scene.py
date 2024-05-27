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
@time: 2024/03/11
@file: disconnection_scene.py
@desc:
"""
import re
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import StringUtils


class DisconnectionScene(RcaScene):
    def __init__(self):
        super().__init__()

    def init(self, context):
        super().init(context)
        if self.obproxy_nodes is None or len(self.obproxy_nodes) == 0:
            raise Exception("obproxy_nodes is empty")

        for node in self.obproxy_nodes:
            if "home_path" not in node or len(node["home_path"].strip()) == 0:
                self.stdio.warn("obproxy_node home_path is empty")
                raise Exception("obproxy_node home_path is empty")
            ssh_helper = node["ssher"]
            if ssh_helper is None:
                raise Exception("obproxy_node:{0} ssher is None".format(node["ip"]))
            obproxy_version = self.obproxy_version
            if obproxy_version is None or len(obproxy_version.strip()) == 0:
                raise Exception("obproxy version is None. Please check the NODES conf.")

            if not (obproxy_version == "4.2.2.0" or StringUtils.compare_versions_greater(obproxy_version, "4.2.2.0")):
                raise Exception("DisconnectionScene's obproxy version must be greater than 4.2.2.0. Please check the NODES conf.")

    def execute(self):
        for node in self.obproxy_nodes:
            self.__execute_obproxy_one_node(node)
        self.stdio.verbose("end disconnectionScene execute all nodes")

    def export_result(self):
        return self.Result.export()

    def get_scene_info(self):
        # 设定场景分析的返回场景使用说明，需要的参数等等
        return {
            "name": "disconnection",
            "info_en": "root cause analysis of disconnection",
            "info_cn": "针对断链接场景的根因分析",
        }

    def __execute_obproxy_one_node(self, node):
        self.gather_log.grep("CONNECTION](trace_type")
        self.gather_log.set_parameters("nodes_list", [node])
        self.gather_log.set_parameters("target", "obproxy")
        self.gather_log.set_parameters("scope", "obproxy_diagnosis")
        if self.input_parameters.get("since") is not None:
            since = self.input_parameters.get("since")
            self.gather_log.set_parameters("since", since)
        self.work_path = self.store_dir
        logs_name = self.gather_log.execute()
        if len(logs_name) == 0:
            self.stdio.warn("not found log about disconnection. On node: {0}".format(node["ip"]))
            return
        self.stdio.verbose("logs_name:{0}".format(logs_name))
        # read the log file
        for name in logs_name:
            self.stdio.verbose("read the log file: {0}".format(name))
            with open(name, 'r') as f:
                log_list = f.read().strip().split('\n')
                for line in log_list:
                    try:
                        record = RCA_ResultRecord()
                        record.add_record("node:{1} obproxy_diagnosis_log:{0}".format(line, node.get("ip")))
                        log_check = DisconnectionLog(self.context, line, record)
                        suggest = log_check.execute()
                        record.add_suggest(suggest)
                        self.stdio.verbose("suggest:{0}".format(suggest))
                        self.Result.records.append(record)
                    except Exception as e:
                        self.stdio.warn("line in log_list is error, log: {0} ,err:{1}".format(line, e))
                        continue


class DisconnectionLog:
    def __init__(self, context, log, record):
        self.context = context
        self.stdio = context.stdio
        self.record = record
        self.stdio.verbose("DisconnectionLog base:{0}".format(log))
        if log is None or len(log.strip()) == 0:
            self.stdio.verbose("log is None or len(log.strip()) == 0")
            raise Exception("log is None or len(log.strip()) == 0")

        self.timeout_event = ""
        try:
            self.log = log

            pattern = re.compile(r'trace_type="(.*?)".*' r'cs_id:(\d+).*' r'server_session_id:(\d+).*' r'error_code:([-0-9]+).*' r'error_msg:"(.*?)"')

            # 搜索日志条目
            matches = pattern.search(log)

            # 如果找到匹配项，则提取所需信息
            if matches:
                trace_type = matches.group(1)
                cs_id = matches.group(2)
                server_session_id = matches.group(3)
                error_code = matches.group(4)
                error_msg = matches.group(5)
                # 打印所需信息
                self.trace_type = trace_type
                self.error_code = error_code
                self.error_msg = error_msg
                timeout_event_pattern = re.compile(r'timeout_event:"(.*?)".*')
                timeout_event_matches = timeout_event_pattern.search(log)
                if timeout_event_matches and self.trace_type == "TIMEOUT_TRACE":
                    timeout_event = matches.group(1)
                    self.error_msg = timeout_event
                if self.trace_type == "SERVER_INTERNAL_TRACE":
                    self.trace_type = "PROXY_INTERNAL_TRACE"
                record.add_record("cs_id:{0}, server_session_id:{1}".format(cs_id, server_session_id))

        except Exception as e:
            self.stdio.error("DisconnectionLog err: {0}".format(e))

    def execute(self):
        # self.get_suggest()
        try:
            suggest = get_disconnectionSuggest(self.context, self.trace_type, self.error_code, self.error_msg, self.record)
            return suggest
        except Exception as e:
            raise Exception("DisconnectionLog execute err: {0}".format(e))


DisconnectionAllSuggest = {
    "LOGIN_TRACE": {
        "-4669": {
            "does not exist": "Ensure the existence of the corresponding cluster, which can be confirmed by directly connecting to ObServer",
            "cluster info is empty": "Directly connect to the Observer to execute the sql statement in the internal_sql field to confirm whether the cluster information returned by the Observer is empty",
        },
        "-4043": {"dummy entry is empty, please check if the tenant exists": "Ensure the existence of the corresponding tenant, which can be confirmed by directly connecting to ObServer"},
        "-8205": {"can not pass white list": "Confirm whether the ObProxy whitelist is configured correctly through OCP"},
        "-1227": {"Access denied": "Confirm if the ObServer whitelist is configured correctly"},
        "-5059": {
            "too many sessions": "You can adjust the global configuration client_max_connections of ObProxy to temporarily avoid it.",
            "hold too many connections": "Need to contact the public cloud platform to adjust the connection limit for cloud tenants",
        },
        "-8004": {
            "obproxy is configured to use ssl connection": "Modify the SSL protocol configuration enable_client_ssl, or use SSL protocol access",
        },
        "-10021": {
            "user proxyro is rejected while proxyro_check on": "Should not be used directly proxyro@sys Accessing databases",
            "connection with cluster name and tenant name is rejected while cloud_full_user_name_check off": "Should not be used directly proxyro@sys Accessing databases",
            "cluster name and tenant name is required while full_username_check on": "When non-cloud users turn off enable_full_user_name, ObProxy will restrict non-three-segment access",
        },
        "-10018": {
            "fail to check observer version, proxyro@sys access denied, error resp": "The password for deploying proxyro by default is not a problem.  If you manually change the password for proxyro user, please ensure that the configuration of the ObProxy startup parameter is correct",
            "fail to check observer version, empty result": "You can confirm whether the server ip configured when the ObProxy was started is available by directly connecting to the ObServer.",
            "fail to check observer version": "Directly connect to the Observer to execute the sql statement in the internal_sql field to confirm whether the cluster information returned by the Observer is empty",
            "fail to check cluster info": "Directly connect to the Observer to execute the sql statement in the internal_sql field to confirm whether the cluster information returned by the Observer is empty",
            "fail to init server state": "Directly connect to the Observer to execute the sql statement in the internal_sql field to confirm whether the cluster information returned by the Observer is empty",
        },
        "-10301": {
            "fail to fetch root server list from config server "
            "fail to fetch root server list from local": "You can manually pull the url of the config_server configured at startup to confirm whether the information returned by the config server is normal",
        },
    },
    "TIMEOUT_TRACE": {
        "-10022": {
            "CLIENT_DELETE_CLUSTER_RESOURCE": "You can temporarily avoid it by adjusting the obproxy cluster_ expire_time configuration.  The default expiration time is one day, and the new request will reset the expiration time.",
            "CLIENT_INTERNAL_CMD_TIMEOUT": "Unexpected timeout, requiring customer environment cooperation for diagnosis",
            "CLIENT_CONNECT_TIMEOUT": "Unexpected timeout, requiring customer environment cooperation for diagnosis",
            "CLIENT_NET_READ_TIMEOUT": "Modifying the observer net_read_timeout variable requires mainly modifying the global level configuration, which will not take effect on existing connections.",
            "CLIENT_NET_WRITE_TIMEOUT": "Modifying the observer net_read_timeout variable requires mainly modifying the global level configuration, which will not take effect on existing connections.",
            "CLIENT_WAIT_TIMEOUT": "Modify the observer wait_timeout variable to temporarily avoid it",
            "SERVER_QUERY_TIMEOUT": "Modify the observer ob_query_timeout variable to temporarily avoid or modify the obproxy observer_query_timeout_delta configuration to avoid it",
            "SERVER_TRX_TIMEOUT": "Modify the variable ob_trx_timeout to temporarily avoid it",
            "SERVER_WAIT_TIMEOUT": "Modify the observer wait_timeout variable to temporarily avoid it",
        },
    },
    "SERVER_VC_TRACE": {
        "-10013": {"Fail to build connection to observer": "Need the cooperation of the observer for diagnosis"},
        "-10014": {" received while proxy transferring request": "Need the cooperation of the observer for diagnosis"},
        "-10016": {" received while proxy reading response": "Need the cooperation of the observer for diagnosis"},
    },
    "CLIENT_VC_TRACE": {
        "-10010": {
            " received from client while obproxy reading request": "Need client cooperation for diagnosis",
        },
        "-10011": {
            " received from client while obproxy handling response": "Need client cooperation for diagnosis",
        },
        "-10012": {
            " received from client while obproxy transferring response": "Need client cooperation for diagnosis",
        },
    },
    "PROXY_INTERNAL_TRACE": {
        "-4664": {
            "dummy entry is empty, disconnect": "Unexpected error scenario",
        },
        "-10018": {
            "proxy execute internal request failed, received error resp, error_type:": "Unexpected error scenario",
        },
        "-10019": {
            "OBProxy reached the maximum number of retrying request": "Unexpected error scenario",
        },
        "-10001": {
            "target session is closed, disconnect": "Unexpected error scenario",
            "": "Unexpected error scenario",
            "ora fatal error": "Unexpected error scenario",
            "primary cluster switchover to standby, disconnect": "The possible connection loss problem during the switch between the primary and secondary databases, which is consistent with the expected scenario",
        },
        "-5065": {"connection was killed by user self, cs_id": "In line with the expected scenario, the diagnostic log is recorded", "connection was killed by user session": "In line with the expected scenario, the diagnostic log is recorded"},
    },
}


def get_disconnectionSuggest(context, trace_type, error_code, error_msg, record):
    stdio = context.stdio
    if trace_type == "" or error_code == "" or error_msg == "":
        raise Exception("not find the suggest. Please contact the community and upload the exception information.. trace_type:{0}, error_code:{1}, error_msg:{2}".format(trace_type, error_code, error_msg))
    Suggest_trace_type = DisconnectionAllSuggest.get(trace_type)
    record.add_record('trace_type:{0}'.format(trace_type))
    if Suggest_trace_type:
        Suggest_error_code = Suggest_trace_type.get(error_code)
        record.add_record('error_code:{0}'.format(error_code))
        if Suggest_error_code:
            suggest = ""
            error_msgs = Suggest_error_code.keys()
            for suggest_error_msg in error_msgs:
                # 子串
                if suggest_error_msg in error_msg:
                    stdio.verbose("find the suggest. trace_type:{0}, error_code:{1}, error_msg:{2}".format(trace_type, error_code, error_msg))
                    suggest += "\n"
                    suggest += Suggest_error_code.get(suggest_error_msg)
            if suggest.strip() != "":
                stdio.verbose("find the suggest. trace_type:{0}, error_code:{1}, error_msg:{2}, suggest:{3}".format(trace_type, error_code, error_msg, suggest.strip()))
                return suggest.strip()
            else:

                suggest = "not find the suggest. Please contact the community and upload the exception information.. trace_type:{0}, error_code:{1}, error_msg:{2}. The suggestions are as follows. You can try using the following suggestions or submit the logs to the Oceanbase community.".format(
                    trace_type, error_code, error_msg
                )
                suggest += "\n"

                for error_msg_by_Suggest_error_code in Suggest_error_code:
                    suggest += Suggest_error_code.get(error_msg_by_Suggest_error_code) + "\n"
                return suggest
        else:
            raise Exception("the disconnection error_code :{0} ,not support.".format(error_code))
    else:
        raise Exception("the disconnection trace_type :{0} ,not support.".format(trace_type))


disconnection = DisconnectionScene()
