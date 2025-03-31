# !/usr/bin/env python
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
@time: 2024/3/3
@file: oms_obcdc.py
@desc:
"""
import os.path
import re
import sys
from urllib.parse import urlparse, parse_qs

import requests

from src.common.command import LocalClient, upload_file
from src.common.constant import const
from src.common.tool import StringUtils
from src.handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
)
from src.handler.rca.rca_handler import RcaScene


class OMSOBcdcScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.check_obcdc_init_succeed_tag = None
        self.obcluster_version = None
        self.oms_cdc_logs_name = None
        self.meta_data_refresh_mode = None
        self.obcdc_pid_data = None
        self.obcdc_pid = None
        self.oms_node = None
        self.obcdc_version = None
        self.obcdc_id = None
        self.component_id = None

    def init(self, context):
        super().init(context)
        self.stdio.warn("This scene is only allowed to be used within OMS container. Please check the environment.")
        self.component_id = self.input_parameters.get("component_id")
        if self.component_id is None:
            self.record.add_record("component_id is not exist. please check component_id.")
            raise RCAInitException("component_id is not exist. please check component_id.")
        if not self.oms_nodes:
            self.record.add_record("oms_nodes is not exist. please check oms_nodes.")
            raise RCAInitException("oms_nodes is not exist. please check oms_nodes.")
        if len(self.oms_nodes) > 1:
            self.record.add_record("this scene just support one oms node. please check oms_nodes.")
            raise RCAInitException("this scene just support one oms node. please check oms_nodes.")
        self.oms_node = self.oms_nodes[0]
        if "store_path" not in self.oms_node:
            self.record.add_record("store_path is not exist. please check oms_node.")
            raise RCAInitException("store_path is not exist. please check oms_node.")
        if "run_path" not in self.oms_node:
            self.record.add_record("run_path is not exist. please check oms_node.")
            raise RCAInitException("run_path is not exist. please check oms_node.")
        if "log_path" not in self.oms_node:
            self.record.add_record("log_path is not exist. please check oms_node.")
            raise RCAInitException("log_path is not exist. please check oms_node.")
        if "ssher" not in self.oms_node or not self.oms_node["ssher"]:
            self.record.add_record("ssher is not exist. please check oms_node.")
            raise RCAInitException("ssher is not exist. please check oms_node.")

    def mock(self):
        # IF YOU WANT TO MOCK, PLEASE USE THE FOLLOWING CODE
        # self.check_obcdc_init_succeed_tag = False
        # self.oms_cdc_logs_name = self.get_all_filenames("./obcdclog/")
        # self.meta_data_refresh_mode = "data_dict"
        pass

    def execute(self):
        try:
            self.mock()
            # get obcdc_id by component_id
            self.get_obcdc_id_by_component_id()
            self.record.add_record("get obcdc_id by component_id: {0}".format(self.obcdc_id))
            # get obcdc version
            self.get_obcdc_version()
            obcdc_store_path = "{0}/store{1}".format(self.oms_node.get("store_path"), self.obcdc_id)
            # gather obcdc conf
            download_file_path = os.path.join(obcdc_store_path, "etc/libobcdc.conf")
            local_libobcdc_conf_path = os.path.join(self.store_dir, "libobcdc.conf")
            self.meta_data_refresh_mode = self.get_obcdc_meta_data_refresh_mode()
            self.record.add_record("meta_data_refresh_mode is {0}".format(self.meta_data_refresh_mode))
            self.oms_node["ssher"].download(download_file_path, local_libobcdc_conf_path)
            self.record.add_record("download libobcdc.conf to {0}".format(local_libobcdc_conf_path))
            # get obcluster version on crawler.conf, the data like dbversion=4.3.4.0
            self.get_ob_version_by_crawler_conf()
            # check obcdc version and oceanbase version
            if self.obcdc_version or self.obcluster_version:
                if not StringUtils.compare_versions_greater(self.obcdc_version, self.obcluster_version):
                    self.record.add_record("[Warn] obcdc version is less than obcluster version. obcdc version: {0}, obcluster version: {1}".format(self.obcdc_version, self.obcluster_version))
            # download obcdc log from remote oms node
            self.gather_obcdc_log()
            # check ret in OBCDC-KBA
            self.check_KBA()
            # check obcdc is online
            self.record.add_record("do check obcdc is online")
            # get obcdc pid
            self.record.add_record("start get obcdc pid")
            self.get_obcdc_obcdc_pid()
            if self.obcdc_pid_data:
                # when obcdc pid is exist
                self.record.add_record("get obcdc pid is exist. {0}".format(self.obcdc_pid))
                self.record.add_record("obcdc_pid_data: {0}".format(self.obcdc_pid_data))
                self.record.add_record("start check obcdc status is normal, delayed or stuck")
                self.record.add_record("do check obcdc init succeed or not.")
                obcdc_init_succeed_tag = False
                if self.check_obcdc_init_succeed_tag:
                    self.record.add_record("get stack info about obcdc pid")
                    obcdc_stack_info_file = self.get_obcdc_stack_info()
                    with open(obcdc_stack_info_file, "r", encoding='utf-8', errors='ignore') as f:
                        for line in f:
                            if "init_common_" in line:
                                obcdc_init_succeed_tag = True
                                self.record.add_record("find \"init_common_\" in obcdc stack info. it means obcdc init succeed.")
                                break
                if not obcdc_init_succeed_tag:
                    self.record.add_record("can not find \"init_common_\" in obcdc stack info. it means obcdc init failed.")
                    self.obcdc_init_failed()
                    return
                self.record.add_record("do check obcdc status stuck")
                self.record.add_record("gather log about \"HEARTBEAT\" on libobcdc.log")
                HEARTBEAT_log_file = os.path.join(self.store_dir, "heartbeat.log")
                for oms_cdc_log_name in self.oms_cdc_logs_name:
                    if not oms_cdc_log_name.endswith("libobcdc.log"):
                        continue
                    local_ssh = LocalClient(stdio=self.stdio)
                    local_ssh.run("grep \"HEARTBEAT\" {0} | grep \"TLOG.COMMITTER\" > {1}".format(oms_cdc_log_name, HEARTBEAT_log_file))
                # check HEARTBEAT log
                heartbeat_list = []
                with open(HEARTBEAT_log_file, "r", encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        pattern = r'heartbeat="\[([^\]]+)\]"'
                        # Search for the pattern in the log line
                        match = re.search(pattern, line)
                        if match:
                            heartbeat_value = match.group(1)
                            heartbeat_list.append(heartbeat_value)
                hold_flag = False
                hold_time = 0
                heartbeat_old = None
                for heartbeat_value in heartbeat_list:
                    if heartbeat_value == heartbeat_old:
                        hold_time += 1
                    else:
                        hold_time = 0
                        heartbeat_old = heartbeat_value
                    if hold_time >= 3:
                        hold_flag = True
                        self.record.add_record("find obcdc stuck. hold_time: {0}".format(hold_time))
                        break
                if hold_flag:
                    # check NEXT_RECORD_RPS=0 in obcdc log
                    self.record.add_record("do check NEXT_RECORD_RPS=0 in obcdc log")
                    for oms_cdc_log_name in self.oms_cdc_logs_name:
                        if not oms_cdc_log_name.endswith("libobcdc.log"):
                            continue
                        with open(oms_cdc_log_name, "r", encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "NEXT_RECORD_RPS=0" in line:
                                    self.record.add_record("find \"NEXT_RECORD_RPS=0\" in {0}".format(oms_cdc_log_name))
                                    self.record.add_suggest("obcdc is hold. find \"NEXT_RECORD_RPS=0\" and heartbeat is not change. the log in {0}.".format(oms_cdc_log_name))
                                    return
                self.record.add_record("do check obcdc status delayed")
                delay_tag = False
                with open(HEARTBEAT_log_file, "r", encoding='utf-8', errors='ignore') as f:
                    for line in f:
                        try:
                            delay_seconds = self.check_delayed(line)
                            if delay_seconds > 10:
                                delay_tag = True
                                self.record.add_record("DELAY value exceeds 10 seconds. on {0}".format(line))
                                break
                        except Exception as e:
                            self.stdio.warn("not warn DELAY value is not exist or type is error ")
                if delay_tag:
                    self.record.add_suggest("obcdc is delayed. DELAY value exceeds 10 seconds. the log in {0}.".format(HEARTBEAT_log_file))
                    return
                else:
                    self.record.add_record("not find HEARTBEAT DELAY value exceeds 10 seconds.")

            else:
                # when obcdc pid is not exist
                self.record.add_record("get obcdc pid is not exist.")
                # check "libobcdc end" in libobcdc end
                self.record.add_record("do check \"libobcdc end\" in libobcdc end")
                libobcdc_end_tag = False
                for oms_cdc_log_name in self.oms_cdc_logs_name:
                    with open(oms_cdc_log_name, "r", encoding='utf-8', errors='ignore') as f:
                        for line in f:
                            if "libobcdc end" in line:
                                self.record.add_record("find \"libobcdc end\" in libobcdc end in {0}".format(oms_cdc_log_name))
                                libobcdc_end_tag = True
                                break
                if not libobcdc_end_tag:
                    self.record.add_record("can not find \"libobcdc end\" in libobcdc end. need check coredump or disk is full or KILL by sys.")
                    # check libobcdc coredump
                    self.record.add_record("do check libobcdc coredump")
                    # get coredump path by cat /proc/sys/kernel/core_pattern
                    coredump_data = self.oms_node["ssher"].exec_cmd("cat /proc/sys/kernel/core_pattern")
                    # get coredump dir
                    coredump_dir = os.path.dirname(coredump_data)
                    # get coredump file
                    coredump_file = self.oms_node["ssher"].exec_cmd("ls -lt {0} | grep core | head -n 1".format(coredump_dir))
                    if coredump_file:
                        self.record.add_record("find coredump file: {0}".format(coredump_file))
                        self.record.add_suggest("find coredump file: {0}. Please check the coredump file. If the core coredump file is old file, please clear it and run this scene again.".format(coredump_file))
                        return
                    else:
                        self.record.add_record("can not find coredump file. need check disk is full or KILL by sys.")
                    self.record.add_record("do check disk is full or not")
                    disk_full_tag = False
                    for oms_cdc_log_name in self.oms_cdc_logs_name:
                        with open(oms_cdc_log_name, "r", encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "-4009" in line:
                                    disk_full_tag = True
                                    self.record.add_record("find \"-4009\" in {0}".format(oms_cdc_log_name))
                                    self.record.add_suggest("find \"-4009\" in {0}. Please check disk is full or not.".format(oms_cdc_log_name))
                                    break
                                elif "OB_IO_ERROR" in line:
                                    disk_full_tag = True
                                    self.record.add_record("find \"OB_IO_ERROR\" in {0}".format(oms_cdc_log_name))
                                    self.record.add_suggest("find \"OB_IO_ERROR\" in {0}. Please check disk is full or not.".format(oms_cdc_log_name))
                                    break
                    if disk_full_tag:
                        self.record.add_record("disk is full. save tsar data.")
                        tsar_file_name = "/tmp/tsar_oms"
                        self.oms_node["ssher"].exec_cmd("tsar -o {0}".format(tsar_file_name))
                        self.oms_node["ssher"].download(tsar_file_name, os.path.join(self.store_dir, os.path.basename("{0}".format(tsar_file_name))))
                        self.record.add_suggest("disk is full. save tsar data. Please check disk is full or not. If check the disk is full, you can restart the process after clearing disk space")
                        return
                    self.record.add_record("disk is not full. need check KILL by sys.")
                    # check KILL by sys
                    self.record.add_record("do check KILL by sys")
                    kill_by_sys_tag = False
                    self.record.add_record("do check \"stop/destroy\" in obcdc log. just check libobcdc.log")
                    for oms_cdc_log_name in self.oms_cdc_logs_name:
                        if not oms_cdc_log_name.endswith("libobcdc.log"):
                            continue
                        with open(oms_cdc_log_name, "r", encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if " stop " in line or " destroy " in line:
                                    self.record.add_record("find \"stop/destroy\" in {0}".format(oms_cdc_log_name))
                                    kill_by_sys_tag = True
                                    break
                    if kill_by_sys_tag:
                        # todo
                        self.record.add_record("find \"stop/destroy\" in obcdc log. save tsar data and dmesg data and 'grep NEED_SLOW libobcdc.log'.")

                        tsar_file_name = "/tmp/tsar_oms"
                        self.oms_node["ssher"].exec_cmd("tsar -o {0}".format(tsar_file_name))
                        self.oms_node["ssher"].download(tsar_file_name, os.path.join(self.store_dir, os.path.basename("{0}".format(tsar_file_name))))
                        self.record.add_record("save tsar data on {0}".format(os.path.join(self.store_dir, os.path.basename("{0}".format(tsar_file_name)))))

                        self.oms_node["ssher"].exec_cmd("dmesg -T > /tmp/dmesg_{0}.log".format(self.oms_node["ssher"].get_name()))
                        local_file_path = os.path.join(os.path.join(self.store_dir, "dmesg_log"), "dmesg_{0}.log".format(self.oms_node["ssher"].get_name()))
                        self.oms_node["ssher"].download("/tmp/dmesg_{0}.log".format(self.oms_node["ssher"].get_name()), local_file_path)
                        self.record.add_record("download /tmp/dmesg_{0}.log to {1}".format(self.oms_node["ssher"].get_name(), local_file_path))
                        self.record.add_record("save dmesg data on {0}".format(local_file_path))

                        self.record.add_suggest("obcdc killed by sys.")
                        self.record.add_suggest("suggest_1: Lower the memory_imit of some stores")
                        self.record.add_suggest("suggest_2: Move processes with relatively high memory usage to run on other node")
                        return
                    else:
                        self.record.add_record("can not find \"stop/destroy\" in obcdc log. need check other reason.")
                        self.record.add_suggest("can not find the reason. please contact with oms team with {0}".format(self.store_dir))
                        return
                else:
                    self.record.add_record("find \"libobcdc end\" in obcdc log. ")
                    self.record.add_record("do check OBCDC exited due to unexpected internal abnormalities")
                    self.record.add_record("do check \"HANDLE_ERR\" in obcdc log")
                    HANDLE_ERR_tag = False
                    HANDLE_ERR_trace_id = None
                    for oms_cdc_log_name in self.oms_cdc_logs_name:
                        if not oms_cdc_log_name.endswith("libobcdc.log"):
                            continue
                        with open(oms_cdc_log_name, "r", encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "HANDLE_ERR" in line:
                                    HANDLE_ERR_tag = True
                                    self.record.add_record("find \"HANDLE_ERR\" in {0}".format(oms_cdc_log_name))
                                    HANDLE_ERR_trace_id = re.search(r"traceID: (\d+)", line).group(1)
                                    break
                    if HANDLE_ERR_trace_id:
                        self.record.add_record("find \"HANDLE_ERR\" in obcdc log. traceID: {0}".format(HANDLE_ERR_trace_id))
                        self.record.add_record("gather log save on {0}".format(os.path.join(self.store_dir, HANDLE_ERR_trace_id)))
                        HANDLE_ERR_trace_id_log_path = "{0}.log".format(os.path.join(self.store_dir, HANDLE_ERR_trace_id))
                        LocalClient().run("grep \"{0}\" {1}/* >{2}".format(HANDLE_ERR_trace_id, os.path.join(self.store_dir, os.path.dirname(self.oms_cdc_logs_name[0])), HANDLE_ERR_trace_id_log_path))
                        self.record.add_record("save HANDLE_ERR traceID log on {0}".format(HANDLE_ERR_trace_id_log_path))
                        self.record.add_suggest("find \"HANDLE_ERR\" in {0}. Process exit triggered by internal exception in OBCDC. Contact OBCDC support".format(HANDLE_ERR_trace_id_log_path))
                        return
                    ERROR_CALLBACK_tag = False
                    self.record.add_record("find \"stop_reason=ERROR_CALLBACK\" in obcdc log.")
                    for oms_cdc_log_name in self.oms_cdc_logs_name:
                        if not oms_cdc_log_name.endswith("libobcdc.log"):
                            continue
                        with open(oms_cdc_log_name, "r", encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "stop_reason=ERROR_CALLBACK" in line:
                                    ERROR_CALLBACK_tag = True
                                    self.record.add_record("find \"stop_reason=ERROR_CALLBACK\" in {0}".format(oms_cdc_log_name))
                                    break
                    if HANDLE_ERR_tag:
                        self.record.add_suggest("find \"stop_reason=ERROR_CALLBACK\" in {0}. Process exit triggered by internal exception in OBCDC. Please check the error log.".format(oms_cdc_log_name))
                        return
                    self.record.add_record("can not find \"HANDLE_ERR\" in obcdc log.")
                    self.record.add_record("do check \"stop_reason=xxx\" without \"ERROR_CALLBACK\" in obcdc log.")
                    for oms_cdc_log_name in self.oms_cdc_logs_name:
                        if not oms_cdc_log_name.endswith("libobcdc.log"):
                            continue
                        with open(oms_cdc_log_name, "r", encoding='utf-8', errors='ignore') as f:
                            for line in f:
                                if "stop_reason=" in line and "ERROR_CALLBACK" not in line:
                                    self.record.add_record("find \"stop_reason=xxx\" without \"ERROR_CALLBACK\" in {0}".format(oms_cdc_log_name))
                                    # 下游进程触发的进程退出
                                    self.record.add_suggest("find \"stop_reason=xxx\" without \"ERROR_CALLBACK\" in {0}. Downstream process triggered exit".format(oms_cdc_log_name))
                                    return

        except Exception as e:
            self.record.add_record("execute oms obcdc analyzer error: {0}".format(e))
            raise RCAExecuteException(e)
        finally:
            self.record.add_suggest("if you want to know more about the result, please contact with oms team with {0}".format(self.store_dir))

    def gather_obcdc_log(self):
        # set self.oms_cdc_logs_name
        if self.oms_cdc_logs_name:
            self.record.add_record("oms_cdc_logs_name is mock, not gather obcdc log")
            return
        self.record.add_record("download obcdc log from remote oms node")
        self.gather_log.set_parameters("target", "oms_cdc")
        oms_obcdc_log_dir = os.path.join(self.store_dir, "oms")
        os.makedirs(oms_obcdc_log_dir, exist_ok=True)
        self.gather_log.set_parameters("oms_component_id", self.component_id)
        self.record.add_record("get oms cdc log about connector by component_id: {0}".format(self.component_id))
        self.gather_log.set_parameters("store_dir", oms_obcdc_log_dir)
        oms_cdc_logs_name = self.gather_log.execute()
        if not oms_cdc_logs_name:
            self.record.add_record("get oms cdc log about connector by component_id: {0} failed.".format(self.component_id))
            self.record.add_suggest("get oms cdc log about connector by component_id: {0} failed.".format(self.component_id))
            return
        self.oms_cdc_logs_name = oms_cdc_logs_name

    def check_KBA(self):
        for oms_cdc_log_name in self.oms_cdc_logs_name:
            with open(oms_cdc_log_name, "r", encoding='utf-8', errors='ignore') as f:
                for line in f:
                    if "no valid tenant is in serve, skip get next_heartbeat_timestamp_" in line:
                        self.record.add_record("find the log: {0}".format(line))
                        self.record.add_suggest("ERROR: KBA-001-no valid tenant is in serve")
                        self.record.add_suggest("1. Confirm if there are tenants who meet the criteria. If not, adjust the blacklist or activation site;")
                        self.record.add_suggest("2. If you only want tenants to be able to perceive new tenants even after they have been deleted, you need to")
                        self.record.add_suggest("a. Change metadata refresh mode to online schema;")
                        self.record.add_suggest("b. And configure enable_filter.sys_tenant=0")
                        return True
                    elif "request start lsn from all server fail" in line or "start lsn locate fail" in line:
                        self.record.add_record("find the log: {0}".format(line))
                        self.record.add_suggest("ERROR: KBA-002-4016-request start lsn from all server")
                        self.record.add_suggest("1. Choose the appropriate starting point and starting mode")
                        return True
                    elif "fetch log fail" in line:
                        self.record.add_record("find the log: {0}".format(line))
                        self.record.add_suggest("ERROR: KBA-003, fetch log fail")
                        self.record.add_suggest("1. Under normal circumstances, it can be automatically restored;")
                        self.record.add_suggest(
                            "2. If there is no recovery for a long time and the same error occurs, it is necessary to consider that a known problem may have been hit. Technical support should be contacted to upgrade the OB version to the latest version under the current upgrade path."
                        )

                        return True
                    elif "row data is not full recorded" in line:
                        self.record.add_record("find the log: {0}".format(line))
                        if self.obcdc_version and not StringUtils.compare_versions_greater(self.obcdc_version, "4.3.2"):
                            self.record.add_suggest("obcdc version is less than 4.3.2, Need update it to 4.3.2 or higher")
                        if self.obcluster_version and not StringUtils.compare_versions_greater(self.obcluster_version, "4.3.1"):
                            self.record.add_suggest("obcluster version is less than 4.3.1, Need to confirm whether the tenant has enabled minimal mode")
                        self.record.add_suggest("ERROR: KBA-004, row data is not full recorded")
                        self.record.add_suggest("please contact with oms team.")
                        return True
                    elif "get_table_info_of_tablet_ failed" in line:
                        self.record.add_record("find the log: {0}".format(line))
                        self.record.add_suggest("ERROR: KBA-005, get_table_info_of_tablet_ failed")
                        self.record.add_record("1. To eliminate known issues, it is recommended to upgrade OB and OBCDC to the latest version of the current upgrade path;")
                        self.record.add_record(
                            "2. OBCDC provides an emergency plan: restart OBCDC and specify the configuration item skip_delete_table_op=1 to OBCDC during restart; After emergency recovery, it is necessary to pay attention to whether downstream data is consistent with upstream data and upgrade OB and OBCDC versions as soon as possible"
                        )
                        self.record.add_record("3. When the above solutions cannot solve the problem, please contact OBCDC technical support for assistance;")
                        return True
                    elif "parse_tablet_change_mds_ failed" in line:
                        self.record.add_record("find the log: {0}".format(line))
                        self.record.add_suggest("ERROR: KBA-006, parse_tablet_change_mds_failed")
                        self.record.add_record("1. Upgrade OBCDC version according to version compatibility rules")
                        self.record.add_suggest("please contact with oms team.")
                        return True
                    elif "next_dict_entry for table_meta failed" in line:
                        self.record.add_record("find the log: {0}".format(line))
                        self.record.add_suggest("ERROR: KBA-007, next_dict_entry for table_meta failed")
                        self.record.add_record("1. Upgrade OBCDC version according to version compatibility rules")
                        self.record.add_suggest("please contact with oms team.")
                        return True
        return

    def execute_hold_or_delayed(self):
        try:
            self.record.add_record("start execute_hold_or_delayed")
            hold_or_delayed_work_dir = os.path.join(self.store_dir, "hold_or_delayed")
            self.record.add_record("step1 gather some log,save on {0}".format(hold_or_delayed_work_dir))
            os.makedirs(hold_or_delayed_work_dir, exist_ok=True)
            # HEARTBEAT日志
            HEARTBEAT_log_dir = os.path.join(hold_or_delayed_work_dir, "HEARTBEAT")
            os.makedirs(HEARTBEAT_log_dir, exist_ok=True)
            # OUTPUT日志
            OUTPUT_log_dir = os.path.join(hold_or_delayed_work_dir, "OUTPUT")
            os.makedirs(OUTPUT_log_dir, exist_ok=True)
            # NEXT_RECORD_RPS
            NEXT_RECORD_RPS_log_dir = os.path.join(hold_or_delayed_work_dir, "NEXT_RECORD_RPS")
            os.makedirs(NEXT_RECORD_RPS_log_dir, exist_ok=True)
            # NEED_SLOW_DOWN日志和NEED_PAUSE_REDO_DISPATCH日志
            NEED_SLOW_DOWN_log_dir = os.path.join(hold_or_delayed_work_dir, "NEED_SLOW_DOWN")
            os.makedirs(NEED_SLOW_DOWN_log_dir, exist_ok=True)
            # TASK_COUNT_STAT日志
            TASK_COUNT_STAT_log_dir = os.path.join(hold_or_delayed_work_dir, "TASK_COUNT_STAT")
            os.makedirs(TASK_COUNT_STAT_log_dir, exist_ok=True)
            # EDIAG && WDIAG
            EDIAG_log_dir = os.path.join(hold_or_delayed_work_dir, "EDIAG")
            os.makedirs(EDIAG_log_dir, exist_ok=True)

            local_ssh = LocalClient(stdio=self.stdio)
            for log in self.oms_cdc_logs_name:
                # HEARTBEAT日志
                local_ssh.run("grep  \"HEARTBEAT\" {0} >  {1}".format(log, os.path.join(HEARTBEAT_log_dir, os.path.basename(log))))
                # OUTPUT日志
                local_ssh.run("grep  \"\[OUTPUT]\" {0} >  {1}".format(log, os.path.join(OUTPUT_log_dir, os.path.basename(log))))
                # NEXT_RECORD_RPS
                local_ssh.run("grep  \"NEXT_RECORD_RPS\" {0} >  {1}".format(log, os.path.join(NEXT_RECORD_RPS_log_dir, os.path.basename(log))))
                # NEED_SLOW_DOWN日志和NEED_PAUSE_REDO_DISPATCH日志
                local_ssh.run("grep  -e \"NEED_SLOW_DOWN\" -e \"NEED_PAUSE_REDO_DISPATCH\"  {0} >  {1}".format(log, os.path.join(NEED_SLOW_DOWN_log_dir, os.path.basename(log))))
                # TASK_COUNT_STAT日志
                local_ssh.run("grep  \"TASK_COUNT_STAT\" {0} >  {1}".format(log, os.path.join(TASK_COUNT_STAT_log_dir, os.path.basename(log))))
                # EDIAG && WDIAG
                local_ssh.run("grep  -e \"EDIAG\" -e \"WDIAG\"  {0} >  {1}".format(log, os.path.join(EDIAG_log_dir, os.path.basename(log))))

            self.record.add_record("step2 analysis of the problem")
            # 1. check sql error in EDIAG_log_dir
            EDIAG_logs = self.get_all_filenames(EDIAG_log_dir)
            for EDIAG_log in EDIAG_logs:
                with open(EDIAG_log, "r", encoding='utf-8', errors='ignore') as f:
                    file_data = f.read()
                    if "execute sql failed" in file_data or "sql read failed from sql_proxy, ret=" in file_data:
                        self.record.add_record("find sql error in EDIAG log: {0}".format(EDIAG_log))
                        self.record.add_suggest("find sql error in EDIAG log: {0}. Please check it".format(EDIAG_log))
                        return

            # 2. check log for delay
            COMMITTER_delay_tag = False
            SEQUENCER_delay_tag = False
            FETCHER_delay_tag = False
            # HEARTBEAT_logs for COMMITTER and FETCHER
            HEARTBEAT_logs = self.get_all_filenames(HEARTBEAT_log_dir)
            for HEARTBEAT_log in HEARTBEAT_logs:
                with open(HEARTBEAT_log, "r", encoding='utf-8', errors='ignore') as f:
                    if COMMITTER_delay_tag and FETCHER_delay_tag:
                        break
                    for line in f:
                        delay_time = self.check_delayed(line)
                        if delay_time > 10:
                            # check model name
                            if "TLOG.COMMITTER" in line:
                                COMMITTER_delay_tag = True
                            elif "FETCHER" in line:
                                FETCHER_delay_tag = True
            # OUTPUT_logs for SEQUENCER
            OUTPUT_logs = self.get_all_filenames(OUTPUT_log_dir)
            for OUTPUT_log in OUTPUT_logs:
                with open(OUTPUT_log, "r", encoding='utf-8', errors='ignore') as f:
                    if SEQUENCER_delay_tag:
                        break
                    for line in f:
                        delay_time = self.check_delayed(line)
                        if delay_time > 10:
                            SEQUENCER_delay_tag = True
            self.record.add_record("COMMITTER_delay_tag: {0}, SEQUENCER_delay_tag: {1}, FETCHER_delay_tag: {2}".format(COMMITTER_delay_tag, SEQUENCER_delay_tag, FETCHER_delay_tag))
            if COMMITTER_delay_tag and not SEQUENCER_delay_tag:
                self.record.add_record("COMMITTER is delayed, SEQUENCER is not delayed")
                self.record.add_suggest("Pay attention to whether there is a backlog of TRANS tasks in the SEQUENCER module SEQ, and whether there is REDO_DISPatch stuck")
            elif SEQUENCER_delay_tag and not FETCHER_delay_tag:
                self.record.add_record("SEQUENCER is delayed, FETCHER is not delayed")
                self.record.add_suggest("Pay attention to whether there is flow control for REDO_DISPOCH and pay attention to the flow control reasons given in the flow control log")
            elif FETCHER_delay_tag:
                self.record.add_record("FETCHER is delayed")
                self.record.add_suggest("Please check there flow control for NEED_SLOW_DOWN")
            else:
                self.record.add_suggest("No delay found in the log. please contact with oms team.")

        except Exception as e:
            self.record.add_record("execute execute_hold_or_delayed error: {0}".format(e))

    def get_all_filenames(self, path):
        try:
            # List all entries in the given directory
            entries = os.listdir(path)
            # Filter out only files
            filenames = [entry for entry in entries if os.path.isfile(os.path.join(path, entry))]
            new_path = []
            for filename in filenames:
                new_path.append(os.path.join(path, filename))
            return new_path
        except Exception as e:
            self.stdio.verbose(f"An error occurred: {e}")
            return []

    def check_delayed(self, log):
        if "DELAY=" in log:
            try:
                # for COMMITTER
                match = re.search(r'DELAY="\[([^\]]+)\]"', log).group(1)
                if match:
                    delay_value = match
                    delay_seconds = float(delay_value.split()[0])
                    return delay_seconds
                # for FETCHER
                match = re.search(r'DELAY=\[([0-9.]+)', log).group(1)
                if match:
                    delay_value = match
                    delay_seconds = float(delay_value.split()[0])
                    return delay_seconds
                return 0
            except Exception as e:
                self.stdio.warn("not warn DELAY value is not exist or type is error ")
                return 0
        return 0

    def get_ob_version_by_crawler_conf(self):
        # set self.obcluster_version
        try:
            obcluster_version_data = self.oms_node["ssher"].exec_cmd("cat {0}/store{1}/conf/crawler.conf | grep dbversion".format(self.oms_node.get("store_path"), self.obcdc_id))
            self.stdio.verbose("obversion_data: {0}".format(obcluster_version_data))
            obcluster_version = obcluster_version_data.split("=")[1].strip()
            self.obcluster_version = obcluster_version
            self.record.add_record("get obversion: {0}".format(self.obcluster_version))
        except Exception as e:
            self.record.add_record("node:{0} get obversion_by_crawler error: {1}".format(self.oms_node["ssher"].get_ip(), e))
            return

    def get_obcdc_obcdc_pid(self):
        # set self.obcdc_pid
        # need obcdc_id
        ssh_client = self.oms_node["ssher"]
        try:
            store_path = self.oms_node.get("store_path")
            self.obcdc_pid = ssh_client.exec_cmd("cat {0}/store{1}/run/libobcdc.pid ".format(store_path, self.obcdc_id))
            self.record.add_record("node:{0} get obcdc pid is {1}".format(ssh_client.get_name(), self.obcdc_pid))
            # check obcdc pid is exist
            if self.obcdc_pid:
                self.obcdc_pid_data = ssh_client.exec_cmd("ps -ef | grep {0} | grep -v \"grep\"".format(self.obcdc_id))
                if self.obcdc_pid_data:
                    self.record.add_record("node:{0} obcdc pid({2}) is exist: {1}".format(ssh_client.get_name(), self.obcdc_pid_data, self.obcdc_pid))
                else:
                    self.record.add_record("node:{0} obcdc pid({1}) is not exist. maybe is not run in oms container".format(ssh_client.get_name(), self.obcdc_pid))
        except Exception as e:
            self.record.add_record("node:{0} get obcdc pid error: {1}".format(ssh_client, e))
            raise RCAExecuteException("get obcdc pid error: {0}".format(e))

    def get_obcdc_version(self):
        # set self.obcdc_version
        self.record.add_record("do check obcdc version")
        ssh_client = self.oms_node["ssher"]
        try:
            self.stdio.verbose("node:{0} find obcdc".format(ssh_client.get_name()))
            obcdc_path = None
            obcdc_path_data = 'find {0}/store{1}/ -name "libobcdc.so.4"'.format(self.oms_node.get("store_path"), self.obcdc_id)
            obcdc_path_data = ssh_client.exec_cmd(obcdc_path_data).strip()
            if "\n" in obcdc_path_data:
                self.stdio.warn("find more than one obcdc path in {0}/store{1}/, please check. obdiag will use the first".format(self.oms_node.get("store_path"), self.obcdc_id))
                obcdc_path_list = obcdc_path_data.split("\n")
                for obcdc_path_list_data in obcdc_path_list:
                    if "libobcdc.so.4" in obcdc_path_list_data:
                        obcdc_path = obcdc_path_list_data
                        break
            self.record.add_record("node:{0} find obcdc path is {1}".format(ssh_client.get_name(), obcdc_path))
            # get obcdc dir
            obcdc_dir = os.path.dirname(obcdc_path)
            obcdc_version_data = ssh_client.exec_cmd("export LD_LIBRARY_PATH={0} && {1} -V".format(obcdc_dir, obcdc_path))
            # get version in obcdc_version_data
            obcdc_version = re.search(r"(\d+\.\d+\.\d+\.\d+)", obcdc_version_data).group(1)
            self.record.add_record("node:{0} obcdc version is {1}".format(ssh_client.get_ip(), obcdc_version))
            self.obcdc_version = obcdc_version
        except Exception as e:
            self.record.add_record("node:{0} check obcdc version error: {1}".format(ssh_client.get_ip(), e))
            self.record.add_suggest("do check obcdc version error, please contact with oms team.")
            return

    def get_obcdc_id_by_component_id(self):
        self.obcdc_id = None
        match = re.search(r"\d+\.\d+\.\d+\.\d+-(\d+)", self.component_id)
        if match:
            number = match.group(1)
            self.obcdc_id = number
        else:
            self.stdio.warn("can not get obcdc_id by component_id. please check component_id.")

    def get_obcdc_stack_info(self):
        self.stdio.verbose("get obcdc stack info. obcdc_pid: {0}".format(self.obcdc_pid))
        ssh_client = self.oms_node["ssher"]
        self.stdio.verbose("upload_file obstack.")
        if getattr(sys, 'frozen', False):
            absPath = os.path.dirname(sys.executable)
        else:
            absPath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        # check node is x86_64 or aarch64
        node_arch = ssh_client.exec_cmd("arch")
        if node_arch == "aarch64":
            obstack2_local_stored_full_path = os.path.join(absPath, const.OBSTACK2_LOCAL_STORED_PATH_AARCH64)
        elif node_arch == "x86_64":
            obstack2_local_stored_full_path = os.path.join(absPath, const.OBSTACK2_LOCAL_STORED_PATH_X86_64)
        else:
            self.stdio.warn("node:{1} arch {0} not support gather obstack. obdiag will try use x86 obstack".format(node_arch, ssh_client.get_name()))
            obstack2_local_stored_full_path = os.path.join(absPath, const.OBSTACK2_LOCAL_STORED_PATH_X86_64)

        upload_file(ssh_client, obstack2_local_stored_full_path, const.OBSTACK2_DEFAULT_INSTALL_PATH, self.context.stdio)
        self.stdio.verbose("chmod obstack.")
        cmd = "chmod a+x {file}".format(file=const.OBSTACK2_DEFAULT_INSTALL_PATH)
        ssh_client.exec_cmd(cmd)
        cmd = "{obstack} {pid} > {store_dir}/obcdc_{pid}_obstack.txt".format(obstack=const.OBSTACK2_DEFAULT_INSTALL_PATH, pid=self.obcdc_pid, store_dir=self.store_dir)
        self.stdio.verbose("gather obstack info on server {0}, run cmd = [{1}]".format(ssh_client.get_name(), cmd))
        ssh_client.exec_cmd(cmd)
        return "{store_dir}/obcdc_{pid}_obstack.txt".format(store_dir=self.store_dir, pid=self.obcdc_pid)

    def obcdc_init_failed_on_data_dict(self):
        ssh_client = self.oms_node["ssher"]
        self.record.add_record("step1: get the location information of the nearest baseline by obcdc log")
        # check the log. by shell
        get_data_dict_in_log_info_success_log = ssh_client.exec_cmd("grep \"get_data_dict_in_log_info success\" {0}/store{1}/log/libobcdc.log*".format(self.oms_node.get("store_path"), self.obcdc_id))
        if get_data_dict_in_log_info_success_log:
            self.record.add_record("step1 end: find the log: {0}".format(get_data_dict_in_log_info_success_log))
        else:
            self.record.add_record("[need check] can not find \"get_data_dict_in_log_info success\" in libobcdc.log* may be this step is fail or the log refresh.")
        self.record.add_record("step2: Get and parse the CLOG logs of the baseline dictionary")
        CLOG_success_log = ssh_client.exec_cmd("grep \"The last log of the baseline data has been fetched\" {0}/store{1}/log/libobcdc.log*".format(self.oms_node.get("store_path"), self.obcdc_id))
        if CLOG_success_log:
            self.record.add_record("step2 end: find the log: {0}".format(CLOG_success_log))
        else:
            self.record.add_record("[need check] can not find \"The last log of the baseline data has been fetched\" in libobcdc.log* may be this step is fail or the log refresh.")
        self.record.add_record("step3: Get and parse the CLOG logs from the baseline dictionary to the starting point, and obtain the incremental dictionary from it.")
        self.record.add_record("do check \"[LOG_META_DATA] [REPLAYER] end tenant_id=%xxx start_timestamp_ns=yyy\" in obcdc log")
        check_log_meta_data = ssh_client.exec_cmd("grep \"LOG_META_DATA] [REPLAYER] end tenant_id=\" {0}/store{1}/log/libobcdc.log*".format(self.oms_node.get("store_path"), self.obcdc_id))
        if check_log_meta_data.strip():
            self.record.add_record("step4 end. find the log: {0}".format(check_log_meta_data))
            return
        else:
            self.record.add_record("can not find log")
            self.record.add_record("do check 'ObDataDictStorage parse_dict_metas failed' in log.")
            parse_dict_metas_log = ssh_client.exec_cmd("grep \"ObDataDictStorage parse_dict_metas failed\" {0}/store{1}/log/libobcdc.log*".format(self.oms_node.get("store_path"), self.obcdc_id))
            if parse_dict_metas_log.strip():
                self.record.add_record("find the log: {0}".format(parse_dict_metas_log))
                self.record.add_record("Unsuccessful deserialization of incremental data dictionary")
                self.record.add_suggest("update obcdc version. please contact with oms team.")
            else:
                self.record.add_record("do check 'fetch log fail' in log.")
                fetch_log_fail = ssh_client.exec_cmd("grep \"fetch log fail\" {0}/store{1}/log/libobcdc.log*".format(self.oms_node.get("store_path"), self.obcdc_id))
                if fetch_log_fail.strip():
                    self.record.add_record("find the log: {0}".format(fetch_log_fail))
                    self.record.add_record("Failed to pull log")
                    self.record.add_suggest("please contact with oms team ,and the error is 003-'Failed to pull log'.")
                else:
                    self.record.add_record("can not check the init error. please contact with oms team.")
                    self.record.add_suggest("please contact with oms team.")
            return

    def obcdc_init_failed_on_online(self):
        # 获取RS/租户ENDPOINT信息
        self.record.add_record("step1: Get RS/Tenant ENDPOINT information".format(self.meta_data_refresh_mode))
        self.record.add_record("get rootserver_list by etc/libobcdc.conf")
        ssh_client = self.oms_node["ssher"]
        # check rootserver_list and tenant_endpoint
        rootserver_list_info = None
        tenant_endpoint_info = None
        rootserver_list_data = ssh_client.exec_cmd("cat {0}/store{1}/etc/libobcdc.conf | grep rootserver_list".format(self.oms_node.get("store_path"), self.obcdc_id))
        self.record.add_record("rootserver_list_data: {0}".format(rootserver_list_data))
        rootserver_list = rootserver_list_data.split("=")[1].strip()
        if rootserver_list == "|":
            self.record.add_record("rootserver_list is useless")
        else:
            rootserver_list_info = rootserver_list.split("|")
            self.record.add_record("rootserver_list: {0}".format(rootserver_list_info))
        if not rootserver_list_info:
            # get tenant_endpoint
            tenant_endpoint_data = ssh_client.exec_cmd("cat {0}/store{1}/etc/libobcdc.conf | grep tenant_endpoint".format(self.oms_node.get("store_path"), self.obcdc_id))
            self.record.add_record("tenant_endpoint_data: {0}".format(tenant_endpoint_data))
            tenant_endpoint = tenant_endpoint_data.split("=")[1].strip()
            if tenant_endpoint == "|":
                self.record.add_record("tenant_endpoint is useless")
            else:
                self.record.add_record("tenant_endpoint: {0}".format(tenant_endpoint))
                tenant_endpoint_info = tenant_endpoint.split("|")
                self.record.add_record("tenant_endpoint_info: {0}".format(tenant_endpoint_info))

        if not rootserver_list_info and not tenant_endpoint_info:
            # check cluster_url
            cluster_url_data = ssh_client.exec_cmd("cat {0}/store{1}/etc/libobcdc.conf | grep cluster_url".format(self.oms_node.get("store_path"), self.obcdc_id))
            self.record.add_record("cluster_url_data: {0}".format(cluster_url_data))
            cluster_url = cluster_url_data.split("=")[1].strip()
            match = re.search(r'cluster_url=(http[^\s]+)', cluster_url_data)
            if match:
                cluster_url = match.group(1)
            if cluster_url == "|":
                self.record.add_record("cluster_url is useless")
            else:
                self.record.add_record("cluster_url: {0}".format(cluster_url))
                cluster_url_info = cluster_url
                self.record.add_record("tenant_endpoint_info: {0}".format(cluster_url_info))
                cluster_info = self.get_cluster_info_by_cluster_url(cluster_url_info)
                if not cluster_info:
                    self.record.add_record("get cluster info by cluster_url is fail. please check cluster_url.")
                    self.record.add_suggest("get cluster info by cluster_url is fail. please check cluster_url.")
                    return
                self.record.add_record("step1 end: get cluster info by cluster_url is success. {0}".format(cluster_info))

        # 发SQL获取集群元信息
        self.record.add_record("step2: Get the baseline dictionary information")
        # check auto_switch_mode_and_refresh_schema failed in log
        self.record.add_record("do check auto_switch_mode_and_refresh_schema failed in log")
        auto_switch_mode_and_refresh_schema = ssh_client.exec_cmd("grep \"auto_switch_mode_and_refresh_schema failed\" {0}/store{1}/log/libobcdc.log*".format(self.oms_node.get("store_path"), self.obcdc_id))
        if auto_switch_mode_and_refresh_schema.strip():
            self.record.add_record("find the log about auto_switch_mode_and_refresh_schema")
            self.record.add_record("do check error The schema has been reclaimed ")
            schema_tag = False
            schema_error = ssh_client.exec_cmd("grep \"unexpected result cnt(ret=-4016, schema_keys.size\" {0}/store{1}/log/libobcdc.log*".format(self.oms_node.get("store_path"), self.obcdc_id))
            if schema_error.strip():
                self.record.add_record("find the log about The schema has been reclaimed")
                schema_tag = True
                self.record.add_suggest(
                    "The schema has been reclaimed. The startup site is too early, and the corresponding schema has been reclaimed; You can view the schema save time and adjust the CDC startup site to the save time range by showing parameters like '% schema_historyexpiring time%'; please contact with oms team."
                )
            self.record.add_record("do check error SQL execution failed")
            sql_failed_tag = False
            sql_error = ssh_client.exec_cmd("grep \"execute sql failed\" {0}/store{1}/log/libobcdc.log*".format(self.oms_node.get("store_path"), self.obcdc_id))
            if sql_error.strip():
                self.record.add_record("find the log about SQL execution failed")
                sql_failed_tag = True
                self.record.add_suggest("SQL execution failed. Please check the SQL statement and contact with oms team.")
            if not schema_tag and not sql_failed_tag:
                self.record.add_record("can not check schema or sql error")
                self.record.add_suggest("maybe the other schema mode is failed. please contact with oms team.")

        else:
            self.record.add_record("step2 end: can not find \"auto_switch_mode_and_refresh_schema failed\" in libobcdc.log* may be this step is fail or the log refresh.")
            self.record.add_suggest("not find error on init failed")

        pass

    def get_cluster_info_by_cluster_url(self, cluster_url):
        try:
            # Parse the URL
            parsed_url = urlparse(cluster_url)
            query_params = parse_qs(parsed_url.query)
            # Convert query parameters to a dictionary with single values
            params = {k: v[0] for k, v in query_params.items()}
            # Make the HTTP GET request
            response = requests.get(parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path, params=params, timeout=3)
            # Check the response
            if response.status_code == 200:
                json_response = response.json()
                return json_response.get("Data", {}).get("RsList", [])
            else:
                Exception("Failed to fetch data from the URL")
        except requests.exceptions.Timeout:
            self.record.add_record("get cluster info by cluster_url timeout")
            self.record.add_suggest("do check if the interface name specified by cluster_url is correct")
            return
        except Exception as e:
            self.record.add_record("get cluster info by cluster_url error: {0}".format(e))
            return

    def obcdc_init_failed(self):
        self.record.add_record("start the reason of obcdc init failed with meta_data_refresh_mode: {0}".format(self.meta_data_refresh_mode))
        # meta_data_refresh_mode: data_dict or online
        if self.meta_data_refresh_mode == "data_dict":
            self.record.add_record("obcdc meta_data_refresh_mode is data_dict schema.")
            self.obcdc_init_failed_on_data_dict()
        elif self.meta_data_refresh_mode == "online":
            self.record.add_record("obcdc meta_data_refresh_mode is online schema.")
            self.obcdc_init_failed_on_online()
        else:
            self.record.add_record("obcdc meta_data_refresh_mode is not data_dict or online. please check libobcdc.conf")
            self.record.add_suggest("obcdc meta_data_refresh_mode is not data_dict or online. please check libobcdc.conf")
        return

    def get_obcdc_meta_data_refresh_mode(self):
        # set self.meta_data_refresh_mode
        if self.meta_data_refresh_mode:
            return self.meta_data_refresh_mode
        # check meta_data_refresh_mode in obcdc {store}/etc/libobcdc.conf
        self.record.add_record("do check meta_data_refresh_mode in obcdc {store}/etc/libobcdc.conf")
        ssh_client = self.oms_node["ssher"]
        try:
            store_path = self.oms_node.get("store_path")
            obcdc_meta_data_refresh_mode_data = ssh_client.exec_cmd("cat {0}/store{1}/etc/libobcdc.conf | grep meta_data_refresh_mode".format(store_path, self.obcdc_id))
            self.stdio.verbose("obcdc_meta_data_refresh_mode_data: {0}".format(obcdc_meta_data_refresh_mode_data))
            obcdc_meta_data_refresh_mode = obcdc_meta_data_refresh_mode_data.split("=")[1].strip()
            return obcdc_meta_data_refresh_mode
        except Exception as e:
            self.record.add_record("node:{0} get obcdc meta_data_refresh_mode error: {1}".format(ssh_client.get_name(), e))
            raise RCAExecuteException("get obcdc meta_data_refresh_mode error: {0}".format(e))

    def get_scene_info(self):
        return {
            "name": "oms_obcdc",
            "info_en": "OMS obcdc log",
            "info_cn": "oms obcdc 组件问题分析",
        }


oms_obcdc = OMSOBcdcScene()
