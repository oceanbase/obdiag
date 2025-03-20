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
@time: 2025/01/09
@file: suspend_transaction.py
@desc:
"""
import datetime
import os
import re

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import StringUtils
class SuspendTransactionScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.work_path = None
        self.work_path = self.store_dir
        self.tenant_id = None
        self.phase = None
        self.tx_id = None
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
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        self.stdio.warn("[SuspendTransactionScene] is a beta scene. It may not work well.")
        # tenant_name
        self.tenant_name = self.input_parameters.get("tenant_name")   
    def verbose(self, info):
        self.stdio.verbose("[SuspendTransactionScene] {0}".format(info))
    def execute(self):
        record = RCA_ResultRecord()
        #find SuspendTransaction
        record.add_record("tenant_name is {0}".format(self.tenant_name))
        sql = "select count(1) as suspend_transaction_count from oceanbase.GV$OB_TRANSACTION_PARTICIPANTS t,oceanbase.__all_tenant a where t.tenant_id=a.tenant_id and t.LAST_REQUEST_TIME < date_sub(now(), INTERVAL 600 SECOND) and a.tenant_name='{0}' and t.ROLE = 'LEADER' and t.ACTION <> 'START';".format(self.tenant_name)
        self.verbose("get suspend_transaction execute_sql is {0}".format(sql))
        try:
            suspend_transaction_count_data=self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("the suspend_transaction_count_data is {0}".format(suspend_transaction_count_data))
        except Exception as e:
            self.verbose("An error occurred while executing the query: {0}".format(str(e))) 
            return 
        suspend_transaction_count=suspend_transaction_count_data[0]['suspend_transaction_count']
        self.verbose("the suspend_transaction_count is {0}".format(suspend_transaction_count))
        if suspend_transaction_count==0:
            self.record.add_record("Not find suspend_transaction.")
            raise RCANotNeedExecuteException("Not find suspend_transaction.") 

     
        #find no_leader Log Stream
        sql="select ls_id from oceanbase.GV$OB_LOG_STAT a,oceanbase.__all_tenant t where a.tenant_id=t.tenant_id and t.tenant_name='{0}' group by a.ls_id having count(CASE WHEN a.role = 'LEADER' THEN 1 END)=0;".format(self.tenant_name)
        self.verbose("get no_leaderon execute_sql is {0}".format(sql))
        try:
            no_leader_ls=self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("the no_leader_ls is {0}".format(no_leader_ls))
            if  not no_leader_ls:
                self.verbose("exist leader_ls")
            else:
                self.verbose("exist no_leader_ls")
        except Exception as e:
            self.verbose("An error occurred while executing the query: {0}".format(str(e))) 
            return 
        if len(no_leader_ls)>0:
            self.record.add_record("fond {0} no_leader,which is a very serious issue. Please contact the OceanBase community immediately.".format(no_leader_ls[0]['ls_id']))
            self.record.add_suggest("fond {0} no_leader,which is a very serious issue. Please contact the OceanBase community immediately.".format(no_leader_ls[0]['ls_id']))
            raise RCANotNeedExecuteException("fond no_leader_ls,which is a very serious issue.")   
        #find memstore_use
        # sql="select t.tenant_name as tenant_name,g.svr_ip as svr_ip,round(SUM(CASE WHEN g.STAT_ID = 130000 THEN VALUE ELSE 0 END)/SUM(CASE WHEN g.STAT_ID = 130002 THEN VALUE ELSE 0 END),2) AS memstore_use_ratio from oceanbase.gv$sysstat g, oceanbase.__all_tenant t where t.tenant_id=g.con_id and g.STAT_ID IN (130000, 130002) and t.tenant_name='{0}' group by g.svr_ip;".format(self.tenant_name)
        sql="SELECT t.tenant_name,m.svr_ip,m.svr_port,round(m.active_span / 1024 / 1024 / 1024, 5) active_gb,round(m.freeze_trigger / 1024 / 1024 / 1024, 5) trigger_gb,round(m.memstore_used / 1024 / 1024 / 1024, 5) used_gb,round(m.memstore_limit / 1024 / 1024 / 1024, 5) limit_gb,round(m.memstore_used/m.memstore_limit,5) as memstore_use_ratio,m.freeze_cnt freeze_count FROM oceanbase.__all_virtual_tenant_memstore_info m INNER JOIN oceanbase.__all_tenant t ON t.tenant_id = m.tenant_id WHERE t.tenant_name = '{0}';".format(self.tenant_name)
        self.verbose("get memstore_use execute_sql is {0}".format(sql))            
        try:
            memstore_use_data=self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("the memstore_use_data is {0}".format(memstore_use_data))
        except Exception as e:
            self.verbose("An error occurred while executing the query: {0}".format(str(e)))
            return
        for memstore_use_data_item in memstore_use_data:
            svr_ip = memstore_use_data_item['svr_ip']
            tenant_name = memstore_use_data_item['tenant_name']
            memstore_use_ratio = memstore_use_data_item['memstore_use_ratio']
            if memstore_use_ratio <1:
                continue
            self.verbose("{0} in this {1} exist memstore_use full,memstore_use is {2}".format(tenant_name,svr_ip,memstore_use_ratio))
            self.record.add_record("{0} in this {1} exist memstore_use full,memstore_use is {2}".format(tenant_name,svr_ip,memstore_use_ratio))
            self.record.add_suggest("{0} in this {1} exist memstore_use full, memstore_use_ratio is {2},maybe you can try expanding the tenant's memory,If expanding the tenant's memory doesn't work, you can try restarting the observer:{3}".format(tenant_name,svr_ip,memstore_use_ratio,svr_ip))
            return
        #find cluster disk 
        sql="select svr_ip,  round(DATA_DISK_IN_USE/DATA_DISK_CAPACITY,2) as data_ratio from oceanbase.gv$ob_servers group by svr_ip;"
        self.verbose("get cluster disk execute_sql is {0}".format(sql))            
        try:
            cluster_disk_use_data=self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("the cluster_disk_use_data is {0}".format(cluster_disk_use_data))
        except Exception as e:
            self.verbose("An error occurred while executing the query: {0}".format(str(e)))
            return
        for cluster_disk_use_data_item in cluster_disk_use_data:
            svr_ip = cluster_disk_use_data_item['svr_ip']
            data_disk_ratio = cluster_disk_use_data_item['data_ratio']
            if data_disk_ratio <0.9:
                continue
            self.verbose(" this {0} exist data disk full,data_disk_ratio is {1}".format(svr_ip,data_disk_ratio))
            self.record.add_record(" this {0} exist data disk full,data_disk_ratio is {1}".format(svr_ip,data_disk_ratio))
            self.record.add_suggest("this {0} exist data disk full,data_disk_ratio is {1},maybe you can try expanding data_disk,please contact the  OcenBase community".format(svr_ip,data_disk_ratio))               
            return
        # find tenant clog_disk_full
        sql="select t.tenant_name as tenant_name,a.svr_ip as svr_ip,  round(a.LOG_DISK_IN_USE/a.LOG_DISK_SIZE,2)as clog_disk_ratio from oceanbase.gv$ob_units a ,oceanbase.__all_tenant t where a.tenant_id=t.tenant_id  and t.tenant_name='{0}';".format(self.tenant_name)
        self.verbose("get clog_disk_full execute_sql is {0}".format(sql))            
        try:
            clog_disk_use_data=self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("the clog_disk_use_data is {0}".format(clog_disk_use_data))
        except Exception as e:
            self.verbose("An error occurred while executing the query: {0}".format(str(e)))
            return
        for clog_disk_use_data_item in clog_disk_use_data:
            tenant_name = clog_disk_use_data_item['tenant_name']
            svr_ip = clog_disk_use_data_item['svr_ip']
            clog_disk_ratio = clog_disk_use_data_item['clog_disk_ratio']
            if clog_disk_ratio <0.85:
                continue
            self.verbose(" {0} on this {1} exist clog disk full,clog_disk_ratio is {2}".format(tenant_name,svr_ip,clog_disk_ratio))
            self.record.add_record(" {0} on this {1} exist clog disk full,clog_disk_ratio is {2}".format(tenant_name,svr_ip,clog_disk_ratio))
            self.record.add_suggest("{0} on this {1} exist clog disk full,clog_disk_ratio is {2},maybe you can try expanding clog_disk ,If expanding the tenant's clog_disk doesn't work,you can invoke clog_disk_full.py".format(tenant_name,svr_ip,clog_disk_ratio))          
            return
                
        #find clog reply
        sql ="select a.tenant_name,b.tenant_id,b.svr_ip,b.ls_id,now(),b.unsubmitted_log_scn, DATE_FORMAT(scn_to_timestamp(b.unsubmitted_log_scn), '%Y-%m-%d %H:%i:%s') as formatted_timestamp, TIMESTAMPDIFF(MINUTE, scn_to_timestamp(b.unsubmitted_log_scn),now()) as replay_delay_minutes, b.pending_cnt  from oceanbase.__all_virtual_replay_stat b,oceanbase.__all_tenant a where b.tenant_id=a.tenant_id  and a.tenant_name='{0}' and role='FOLLOWER' order by b.ls_id;".format(tenant_name)
        self.verbose("get clog reply execute_sql is {0}".format(sql))            
        try:
            clog_reply_data=self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            self.verbose("the clog_reply_data is {0}".format(clog_reply_data))
        except Exception as e:
            self.verbose("An error occurred while executing the query: {0}".format(str(e)))
            return
        for clog_reply_data_item in clog_reply_data:
            tenant_name = clog_reply_data_item['tenant_name']
            svr_ip=clog_reply_data_item['svr_ip']
            ls_id=clog_reply_data_item['ls_id']
            replay_delay_minutes=clog_reply_data_item['replay_delay_minutes']
            pending_cnt=clog_reply_data_item['pending_cnt']
            if replay_delay_minutes <3:
                continue
            self.verbose(" {0}'s {1} on this {2} exist replay_delay,replay_delay_minutes is {3}".format(tenant_name,ls_id,svr_ip,replay_delay_minutes))
            self.record.add_record(" {0}'s {1} on this {2} exist replay_delay,replay_delay_minutes is {3}".format(tenant_name,ls_id,svr_ip,replay_delay_minutes))
            self.record.add_suggest("{0}'s {1} on this {2} exist replay_delay,replay_delay_minutes is {3},you can try expanding tenant's specifications,if expanding the tenant's specifications doesn't work,please contact the  OcenBase community".format(tenant_name,ls_id,svr_ip,replay_delay_minutes))                          
            return
        #other scene
        self.record.add_record("This suspended transaction is not within the existing scenarios. Please contact the OceanBase community.")
        self.record.add_suggest("This suspended transaction is not within the existing scenarios. Please contact the OceanBase community.")


        # except Exception as e:
        #     raise RCAExecuteException("SuspendTransactionScene execute error: {0}".format(e))
        # finally:
        #     self.stdio.verbose("end SuspendTransactionScene execute")
    def get_scene_info(self):
        return {
            "name": "suspend_transaction",
            "info_en": "root cause analysis of suspend transaction",
            "info_cn": "悬挂事务",
        }
suspend_transaction = SuspendTransactionScene()

