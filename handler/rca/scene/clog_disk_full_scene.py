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
@time: 2024/05/20
@file: clog_disk_full_scene.py
@desc:
"""
import datetime
import os
import re

from handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import StringUtils


class ClogDiskFullScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.work_path = None
        self.index_table_id = None
        self.estimated_size = None
        self.estimated_data_size = None

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

    def verbose(self, info):
        self.stdio.verbose("[ClogDiskFullScene] {0}".format(info))

    def execute(self):
        try:
            # get log_disk_utilization_threshold
            sql = "select SVR_IP,SVR_PORT,TENANT_ID,value/100 as value from oceanbase.GV$OB_PARAMETERS where name = 'log_disk_utilization_threshold';"
            self.verbose("get log_disk_utilization_threshold execute_sql is {0}".format(sql))
            cursor_log_disk_utilization_threshold = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            log_disk_utilization_threshold_data = cursor_log_disk_utilization_threshold.fetchall()
            self.verbose("the log_disk_utilization_threshold_data is {0}".format(log_disk_utilization_threshold_data))
            tenant_ids_data = []
            for log_disk_utilization_threshold_data_item in log_disk_utilization_threshold_data:
                svr_ip = log_disk_utilization_threshold_data_item['SVR_IP']
                svr_port = log_disk_utilization_threshold_data_item['SVR_PORT']
                tenant_id = log_disk_utilization_threshold_data_item['TENANT_ID']
                log_disk_utilization_threshold = log_disk_utilization_threshold_data_item['value']
                # get the tenant_ids clog_disk_full
                sql = "select *  from oceanbase.gv$ob_units where LOG_DISK_IN_USE > LOG_DISK_SIZE*{0} and SVR_IP='{1}' and SVR_PORT={2} and tenant_id={3};".format(log_disk_utilization_threshold, svr_ip, svr_port, tenant_id)
                self.verbose("get the tenant_ids clog_disk_full execute_sql is {0}".format(sql))
                tenant_ids_data_sql = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if len(tenant_ids_data_sql) <= 0:
                    continue
                else:
                    self.verbose("the tenant_id clog_disk_full is {0}".format(tenant_ids_data_sql))
                    tenant_ids_data = tenant_ids_data_sql
            if len(tenant_ids_data) <= 0:
                self.record.add_record("Not find tenant_ids about clog_disk_full.")
                raise RCANotNeedExecuteException("Not find tenant_ids about clog_disk_full.")
            self.verbose("tenant_ids about clog_disk_full is {0}".format(tenant_ids_data))
            # find ls,node by tenant_id
            tenant_ls_datas = []
            for tenant_id_data in tenant_ids_data:
                tenant_id = tenant_id_data["TENANT_ID"]
                svr_port = tenant_id_data["SVR_PORT"]
                svr_ip = tenant_id_data["SVR_IP"]
                sql = "select * from oceanbase.__all_virtual_log_stat where tenant_id = {0} and end_lsn-base_lsn >128*1024*1024 and svr_ip='{1}' and svr_port={2} ;".format(tenant_id, svr_ip, svr_port)
                self.verbose("find ls,node by tenant_id execute_sql is {0}".format(sql))
                ls_datas = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
                for ls_data in ls_datas:
                    tenant_ls_datas.append({"tenant_id": tenant_id, "ls_id": ls_data["ls_id"], "ip": ls_data["svr_ip"], "port": ls_data["svr_port"]})
            if len(tenant_ls_datas) <= 0:
                self.record.add_record("Not find tenant_ls_datas about clog_disk_full.")
                raise RCANotNeedExecuteException("Not find tenant_ls_datas about clog_disk_full.")
            self.verbose("tenant_ls_datas is {0}".format(tenant_ls_datas))
            self.record.add_record("tenant_ls_datas is {0}".format(tenant_ls_datas))
            self.record.add_suggest("init data end. Please check the other record.")
            for tenant_ls_data in tenant_ls_datas:
                record = RCA_ResultRecord(self.stdio)
                record.add_record("check error tenant_ls_data is {0}".format(tenant_ls_data))
                self.verbose("start to get estimated_data_size...")
                check = ClogDiskFullChecker(context=self.context, tenant_id=tenant_ls_data['tenant_id'], ls_id=tenant_ls_data['ls_id'], record=record, work_path=self.work_path, stdio=self.stdio)
                check.execute()
                if record.suggest_is_empty():
                    record.add_suggest("not found stuck about clog disk full. You can package the files under '{0}' and upload them to the OceanBase community for further analysis.".format(self.work_path))
                self.Result.records.append(record)
        except RCANotNeedExecuteException as e:
            self.stdio.print("[Not Need Execute]ClogDiskFullScene need not execute: {0}".format(e))
        except Exception as e:
            raise RCAExecuteException("ClogDiskFullScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end ClogDiskFullScene execute")

    def get_scene_info(self):

        return {
            "name": "clog_disk_full",
            "info_en": "Identify the issue of clog disk space being full.",
            "info_cn": 'clog日志磁盘空间满的问题',
        }


class ClogDiskFullChecker:
    def __init__(self, context, tenant_id, ls_id, work_path, stdio, record=None):
        self.context = context
        self.gather_log = context.get_variable('gather_log')
        self.stdio = self.context.stdio
        self.tenant_id = tenant_id
        if self.tenant_id is None:
            raise Exception('tenant_id can not be none')
        self.ls_id = ls_id
        if self.ls_id is None:
            raise Exception('ls_id can not be none')
        self.record = record or RCA_ResultRecord(stdio=self.stdio)
        self.work_path = work_path or "./ClogDiskFull"
        self.work_path = self.work_path + "/tenant_id_{0}/ls_id_{1}".format(self.tenant_id, self.ls_id)
        if not os.path.exists(work_path):
            os.makedirs(work_path)
        self.stdio.verbose("work_path is {0}".format(self.work_path))
        self.stdio = stdio
        self.input_parameters = context.get_variable("input_parameters") or {}

    def execute(self):
        try:
            work_path = self.work_path
            if not os.path.exists(work_path):
                os.makedirs(work_path)
            # __check_checkpoint
            self.record.add_record("__check_checkpoint")
            work_path_checkpoint = work_path + "/checkpoint/"
            # gather log about tenant_id, ls, "clog checkpoint no change".
            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("{0}".format(self.tenant_id))
            self.gather_log.grep("{0}".format(self.ls_id))
            self.gather_log.grep("clog checkpoint no change")
            if self.input_parameters.get("since") is not None:
                since = self.input_parameters.get("since")
                self.gather_log.set_parameters("since", since)
            logs_name = self.gather_log.execute(save_path=work_path_checkpoint)
            if logs_name is None or len(logs_name) <= 0:
                self.record.add_record("no log_disk_full about checkpoint")
                return False
            else:
                self.record.add_record("find log_disk_full about checkpoint in {0}".format(work_path_checkpoint))
            is_clog_checkpoint_stuck = False
            stuck_service_type = ""
            for log_name in logs_name:
                if is_clog_checkpoint_stuck:
                    break
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        self.stdio.verbose("line is {0}".format(line))
                        checkpoint_scn = self.parse_checkpoint_scn(line)
                        scn_time = datetime.datetime.fromtimestamp(float(checkpoint_scn) / 1000000000)
                        log_time = self.parse_log_time(line)
                        stuck_delta = datetime.timedelta(minutes=2)
                        if log_time - scn_time > stuck_delta:
                            is_clog_checkpoint_stuck = True
                            self.record.add_record("is_clog_checkpoint_stuck is {0}".format(is_clog_checkpoint_stuck))
                            self.record.add_record("the log is {0}".format(line))
                            stuck_service_type = self.get_stuck_mod(line).get('service_type') or "unknown"
                            self.record.add_record("stuck_service_type is {0}".format(stuck_service_type))
                            break
            if is_clog_checkpoint_stuck is False:
                self.record.add_record("is_clog_checkpoint_stuck is {0}".format(is_clog_checkpoint_stuck))
                return False
            self.record.add_record("__check_checkpoint end")
            self.record.add_record("__get_min_ckpt_type start")
            if stuck_service_type != "" and stuck_service_type != 'TRANS_SERVICE':
                self.record.add_record("stuck_service_type  is {0}, not 'TRANS_SERVICE'. pass __get_min_ckpt_type".format(stuck_service_type))
                pass
            else:
                work_path_get_min_ckpt_type = work_path + "/get_min_ckpt_type/"
                # gather log about tenant_id, ls, "ObLSTxService::get_rec_scn"
                self.gather_log.set_parameters("scope", "observer")
                self.gather_log.grep("{0}".format(self.tenant_id))
                self.gather_log.grep("{0}".format(self.ls_id))
                self.gather_log.grep("ObLSTxService::get_rec_scn")
                if self.input_parameters.get("since") is not None:
                    since = self.input_parameters.get("since")
                    self.gather_log.set_parameters("since", since)
                logs_name = self.gather_log.execute(save_path=work_path_get_min_ckpt_type)
                check_min_ckpt_type = False
                for log_name in logs_name:
                    if check_min_ckpt_type:
                        break
                    with open(log_name, "r") as f:
                        lines = f.readlines()
                        for line in lines:
                            if "ObLSTxService::get_rec_scn" in line:
                                check_min_ckpt_type = True
                                self.record.add_record("get min ckpt type is on {0}".format(line))
                                min_checkpoint_tx_log_type = self.get_stuck_mod(line).get('common_checkpoint_type') or "unknown"
                                self.record.add_record("min_checkpoint_tx_log_type is {0}".format(min_checkpoint_tx_log_type))
                                min_checkpoint_scn = self.parse_min_checkpoint_scn(line)
                                self.record.add_record("min_checkpoint_scn is {0}".format(min_checkpoint_scn))
                                self.record.add_suggest("min_checkpoint_tx_log_type is {0}. please check it.".format(min_checkpoint_tx_log_type))
                                break
                self.record.add_record("check_min_ckpt_type is {0}".format(check_min_ckpt_type))
            self.record.add_record("__get_min_ckpt_type end")
            self.record.add_record("__check_replay_stuck start")
            if stuck_service_type != 'TRANS_SERVICE' and stuck_service_type != 'MAX_DECIDED_SCN':
                self.record.add_record("stuck_service_type is {0} (not TRANS_SERVICE or MAX_DECIDED_SCN). pass __check_replay_stuck. ".format(stuck_service_type))
                pass
            else:
                work_path_check_replay_stack = work_path + "/check_replay_stuck/"
                # gather log about tenant_id, ls, "get_min_unreplayed_log_info"
                self.gather_log.set_parameters("scope", "observer")
                self.gather_log.grep("{0}".format(self.tenant_id))
                self.gather_log.grep("{0}".format(self.ls_id))
                self.gather_log.grep("get_min_unreplayed_log_info")
                if self.input_parameters.get("since") is not None:
                    since = self.input_parameters.get("since")
                    self.gather_log.set_parameters("since", since)
                logs_name = self.gather_log.execute(save_path=work_path_check_replay_stack)
                check_replay_stuck = False
                for log_name in logs_name:
                    if check_replay_stuck:
                        break
                    with open(log_name, "r") as f:
                        lines = f.readlines()
                        for line in lines:
                            if check_replay_stuck:
                                break
                            if "get_min_unreplayed_log_info" in line and self.get_stuck_modV2(line).get('role_') is not None:

                                replay_scn = self.parse_replay_scn(line)
                                replay_scn_time = datetime.datetime.fromtimestamp(float(replay_scn) / 1000000000)
                                log_time = self.parse_log_time(line)
                                check_replay_stuck = log_time - replay_scn_time > datetime.timedelta(minutes=0.5)
                                if check_replay_stuck:
                                    self.record.add_record("check_replay_stuck is True. the line: {0}".format(line))
                                    self.record.add_record("get min unreplayed log info is {0}".format(line))
                                    self.record.add_record("log_time - replay_scn_time : {0} - {1}".format(log_time, replay_scn_time))
                                    self.record.add_record("datetime.timedelta(minutes=0.5): {0}".format(datetime.timedelta(minutes=0.5)))
                                    self.record.add_record("log_time - replay_scn_time > datetime.timedelta(minutes=0.5) is {0}".format(check_replay_stuck))
                                break
                self.record.add_record("check_replay_stuck is {0}".format(check_replay_stuck))
                if check_replay_stuck:
                    self.record.add_record("check_replay_stuck is True. Please check replay status")
            self.record.add_record("__check_replay_stuck end")
            self.record.add_record("__check_dump_stuck start")
            if stuck_service_type != 'TRANS_SERVICE':
                self.record.add_record("stuck_service_type is {0} (not TRANS_SERVICE ). pass __check_dump_stuck.".format(stuck_service_type))
            else:
                work_path_check_dump_stuck = work_path + "/check_dump_stuck/"
                # gather log about tenant_id, "log_frozen_memstore_info_if_need_", "[TenantFreezer] oldest frozen memtable"
                self.gather_log.set_parameters("scope", "observer")
                self.gather_log.grep("{0}".format(self.tenant_id))
                self.gather_log.grep("log_frozen_memstore_info_if_need_")
                self.gather_log.grep("[TenantFreezer] oldest frozen memtable")
                if self.input_parameters.get("since") is not None:
                    since = self.input_parameters.get("since")
                    self.gather_log.set_parameters("since", since)
                logs_name = self.gather_log.execute(save_path=work_path_check_dump_stuck)
                check_dump_stuck = False
                for log_name in logs_name:
                    if check_dump_stuck:
                        break
                    with open(log_name, "r") as f:
                        lines = f.readlines()
                        for line in lines:
                            if check_dump_stuck:
                                break
                            if "log_frozen_memstore_info_if_need_" in line and "[TenantFreezer] oldest frozen memtable" in line:
                                # check_ready_for_flush_stuck
                                ready_for_flush_time_list = re.findall(r'ready_for_flush_time:(\d+)', line)
                                frozen_time_list = re.findall(r'frozen_time:(\d+)', line)
                                nu = min(len(ready_for_flush_time_list), len(frozen_time_list))
                                for i in range(nu):
                                    ready_flush_time = datetime.datetime.fromtimestamp(float(ready_for_flush_time_list[i]) / 1000000000)
                                    frozen_time = datetime.datetime.fromtimestamp(float(frozen_time_list[i]) / 1000000000)
                                    stuck_time = ready_flush_time - frozen_time
                                    if stuck_time > stuck_delta:
                                        check_dump_stuck = True
                                        self.record.add_record("check_dump_stuck is {0}, line is {1}".format(check_dump_stuck, line))
                                        break
                self.record.add_record("check_dump_stuck is {0}".format(check_dump_stuck))
                if check_dump_stuck:
                    self.record.add_suggest("Dump stuck, please check dump status.")
            self.record.add_record("__check_dump_stuck end")
            self.record.add_record("__check_data_disk_full start")
            check_data_disk_full = False
            work_path_check_data_disk_full = work_path + "/check_data_disk_full/"
            # gather log about tenant_id, "Server out of disk space"
            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("{0}".format(self.tenant_id))
            self.gather_log.grep("Server out of disk space")
            if self.input_parameters.get("since") is not None:
                since = self.input_parameters.get("since")
                self.gather_log.set_parameters("since", since)
            logs_name = self.gather_log.execute(save_path=work_path_check_data_disk_full)
            for log_name in logs_name:
                if check_data_disk_full:
                    break
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "Server out of disk space" in line:
                            check_data_disk_full = True
                            self.record.add_record("check_data_disk_full is {0}, line is {1}".format(check_data_disk_full, line))
                            break
            self.record.add_record("check_data_disk_full is {0}".format(check_data_disk_full))
            if check_data_disk_full:
                self.record.add_suggest("Data disk full, please check data disk usage.")
            self.record.add_record("__check_data_disk_full end")
            self.record.add_record("__check_too_many_sstable start")
            check_too_many_sstable = False
            work_path_check_too_many_sstable = work_path + "/check_too_many_sstable/"
            # gather log about tenant_id, "Too many sstables in tablet, cannot schdule mini compaction, retry later"
            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("{0}".format(self.tenant_id))
            self.gather_log.grep("Too many sstables in tablet, cannot schdule mini compaction, retry later")
            if self.input_parameters.get("since") is not None:
                since = self.input_parameters.get("since")
                self.gather_log.set_parameters("since", since)
            logs_name = self.gather_log.execute(save_path=work_path_check_too_many_sstable)
            for log_name in logs_name:
                if check_too_many_sstable:
                    break
                with open(log_name, "r") as f:
                    lines = f.readlines()
                    for line in lines:
                        if "Too many sstables in tablet, cannot schdule mini compaction, retry later" in line:
                            check_too_many_sstable = True
                            self.record.add_record("check_too_many_sstable is {0}, line is {1}".format(check_too_many_sstable, line))
                            break
            self.record.add_record("check_too_many_sstable is {0}".format(check_too_many_sstable))
            if check_too_many_sstable:
                self.record.add_suggest("Too many sstables in tablet, please check the number of sstables in the tablet.")
            self.record.add_record("__check_too_many_sstable end")
            self.record.add_record("check end")
            return True
        except Exception as e:
            self.record.add_record("check error: {0}".format(e))
            return False

    def get_stuck_mod(self, line):
        d = dict()
        # service_type="TRANS_SERVICE"
        p = '(?P<key>[\w|_]+)=\"(?P<value>\w+)\"'
        m = re.finditer(p, line)
        for i in m:
            d[i.group('key')] = i.group('value')
        return d

    def get_stuck_modV2(self, line):
        d = dict()
        # service_type="TRANS_SERVICE"
        p = '(?P<key>[\w|_]+):(?P<value>\w+)'
        m = re.finditer(p, line)
        for i in m:
            d[i.group('key')] = i.group('value')
        return d

    def parse_checkpoint_scn(self, line):
        p = "checkpoint_scn=\{val:(?P<checkpoint_scn>\d+)\},"
        p1 = "checkpoint_scn=\{val:(?P<checkpoint_scn>\d+),"
        m = re.search(p, line)
        if not m:
            m = re.search(p1, line)
        scn = 0
        if m:
            scn = int(m.group('checkpoint_scn'))
        return scn

    def parse_log_time(self, line):
        p = "\[(?P<date_time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]"
        m = re.search(p, line)
        t = None
        if m:
            t = datetime.datetime.strptime(m.group('date_time'), "%Y-%m-%d %H:%M:%S.%f")
        return t

    def parse_min_checkpoint_scn(self, line):
        p = "min_rec_scn=\{val:(?P<checkpoint_scn>\d+)\},"
        p1 = "min_rec_scn=\{val:(?P<checkpoint_scn>\d+),"
        m = re.search(p, line)
        if not m:
            m = re.search(p1, line)
        scn = 0
        if m:
            scn = int(m.group('checkpoint_scn'))
        return scn

    def parse_replay_scn(self, line):
        p = "scn=\{val:(?P<scn>\d+)\},"
        p1 = "scn=\{val:(?P<scn>\d+),"
        m = re.search(p, line)
        if not m:
            m = re.search(p1, line)
        scn = 0
        if m:
            scn = int(m.group('scn'))
        return scn


clog_disk_full = ClogDiskFullScene()
