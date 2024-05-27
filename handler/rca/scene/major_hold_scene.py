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
@time: 2024/1/2
@file: major_hold_scene.py
@desc:
"""
import json
import re
from handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from common.tool import DateTimeEncoder
from common.tool import StringUtils


class MajorHoldScene(RcaScene):
    def __init__(self):
        super().__init__()
        self.local_path = ""

    def init(self, context):
        try:
            super().init(context)
            self.local_path = context.get_variable('result_path')

            if self.observer_version is None:
                raise Exception("obproxy version is None. Please check the NODES conf.")

            if not (self.observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0")):
                raise Exception("observer version must be greater than 4.0.0.0. Please check the NODES conf.")

        except Exception as e:
            raise RCAInitException("MajorHoldScene RCAInitException: {0}".format(e))

    def execute(self):
        # 前置条件确认
        need_tag = False
        first_record = RCA_ResultRecord()
        err_tenant_ids = []
        # 合并任务是否有报错
        try:
            COMPACTING_data = self.ob_connector.execute_sql('select * from oceanbase.CDB_OB_MAJOR_COMPACTION where IS_ERROR="YES";')
            if len(COMPACTING_data) == 0:
                first_record.add_record("CDB_OB_MAJOR_COMPACTION is not exist IS_ERROR='YES'")
            else:
                need_tag = True
                CDB_OB_MAJOR_COMPACTION_err_tenant_ids = []
                for data in COMPACTING_data:
                    CDB_OB_MAJOR_COMPACTION_err_tenant_ids.append(str(data[0]))

                first_record.add_record("CDB_OB_MAJOR_COMPACTION have IS_ERROR='YES',the tenant_ids are {0}".format(err_tenant_ids))
                err_tenant_ids.extend(CDB_OB_MAJOR_COMPACTION_err_tenant_ids)

        except Exception as e:
            self.stdio.warn("MajorHoldScene execute CDB_OB_MAJOR_COMPACTION panic:  {0}".format(e))
            raise RCAExecuteException("MajorHoldScene execute CDB_OB_MAJOR_COMPACTION panic:  {0}".format(e))
        # __all_virtual_compaction_diagnose_info里存在status=FAILED的记录
        try:
            diagnose_data = self.ob_connector.execute_sql('select * from oceanbase.__all_virtual_compaction_diagnose_info where status="FAILED";')
            if len(diagnose_data) == 0:
                first_record.add_record('__all_virtual_compaction_diagnose_info is not exist status="FAILED";')
            else:
                need_tag = True
                __all_virtual_compaction_diagnose_info_err_tenant_ids = []
                for data in COMPACTING_data:
                    __all_virtual_compaction_diagnose_info_err_tenant_ids.append(str(data[0]))

                first_record.add_record("__all_virtual_compaction_diagnose_info have status='FAILED',the tenant is {0}".format(__all_virtual_compaction_diagnose_info_err_tenant_ids))
                err_tenant_ids.extend(__all_virtual_compaction_diagnose_info_err_tenant_ids)
        except Exception as e:
            self.stdio.error("MajorHoldScene execute CDB_OB_MAJOR_COMPACTION panic:  {0}".format(e))
            raise RCAExecuteException("MajorHoldScene execute CDB_OB_MAJOR_COMPACTION panic:  {0}".format(e))
        # GV$OB_COMPACTION_PROGRESS表中，根据上一次合并记录中的data_size/(estimated_finish_time-start_time)与当前合并版本记录中(data_size-unfinished_data_size)/(当前时间-start_time)相比，如果差距过大（当前合并比上一次合并慢很多，以5倍为指标）
        try:
            running_data = self.ob_connector.execute_sql("select * from oceanbase.GV$OB_COMPACTION_PROGRESS where  STATUS <> 'FINISH'  and START_TIME <= NOW() - INTERVAL 20 minute GROUP BY COMPACTION_SCN DESC;")
            if len(running_data) == 0:
                first_record.add_record('No merge tasks that have not ended beyond the expected time')
            else:

                time_out_merge_err_tenant_ids = []
                need_tag = True
                for data in running_data:
                    time_out_merge_err_tenant_ids.append(str(data[2]))
                first_record.add_record("merge tasks that have not ended beyond the expected time,the tenant_id is {0}".format(time_out_merge_err_tenant_ids))
                self.stdio.verbose("merge tasks that have not ended beyond the expected time,the tenant_id is {0}".format(time_out_merge_err_tenant_ids))
                err_tenant_ids.extend(time_out_merge_err_tenant_ids)
        except Exception as e:
            self.stdio.error("MajorHoldScene execute GV$OB_COMPACTION_PROGRESS panic:  {0}".format(e))
            raise RCAExecuteException("MajorHoldScene execute GV$OB_COMPACTION_PROGRESS panic:  {0}".format(e))
        if not need_tag:
            first_record.add_suggest("major merge abnormal situation not need execute")
            self.Result.records.append(first_record)
            raise RCANotNeedExecuteException("MajorHoldScene not need execute")
        else:
            err_tenant_ids = list(set(err_tenant_ids))
            first_record.add_suggest("some tenants need execute MajorHoldScene. :{0}".format(err_tenant_ids))
        self.stdio.verbose("On CDB_OB_MAJOR_COMPACTION")

        # execute record need more
        for err_tenant_id in err_tenant_ids:
            tenant_record = RCA_ResultRecord()
            first_record_records = first_record.records.copy()
            tenant_record.records.extend(first_record_records)
            self.stdio.verbose("tenant_id is {0}".format(err_tenant_id))
            tenant_record.add_record("tenant_id is {0}".format(err_tenant_id))
            # 1
            try:
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary('SELECT * FROM oceanbase.CDB_OB_MAJOR_COMPACTION WHERE TENANT_ID= "{0}" AND (IS_ERROR = "NO" OR IS_SUSPENDED = "NO");'.format(err_tenant_id))
                OB_MAJOR_COMPACTION_data = cursor.fetchall()
                if len(OB_MAJOR_COMPACTION_data) == 0:
                    tenant_record.add_record("on CDB_OB_MAJOR_COMPACTION where status='COMPACTING'; " "result:{0} , need not next step".format(str(OB_MAJOR_COMPACTION_data)))

                else:
                    tenant_record.add_record("on CDB_OB_MAJOR_COMPACTION where status='COMPACTING';" "result:{0}".format(str(OB_MAJOR_COMPACTION_data)))

            except Exception as e:
                tenant_record.add_record("#1 on CDB_OB_MAJOR_COMPACTION get data failed")
                self.stdio.warn("MajorHoldScene execute exception: {0}".format(e))
                pass
            # 2
            try:
                compaction_diagnose_info = self.ob_connector.execute_sql('SELECT * FROM oceanbase.__all_virtual_compaction_diagnose_info WHERE status="FAILED";')

                if len(compaction_diagnose_info) == 0:
                    tenant_record.add_record("on __all_virtual_compaction_diagnose_info no data status=FAILED")
                else:
                    tenant_record.add_record("on __all_virtual_compaction_diagnose_info;" "result:{0}".format(str(compaction_diagnose_info)))

                    for COMPACTING_data in compaction_diagnose_info:
                        self.diagnose_info_switch(COMPACTING_data, tenant_record)

            except Exception as e:
                tenant_record.add_record("#2&3 on __all_virtual_compaction_diagnose_info get data failed")
                self.stdio.warn("#2&3 MajorHoldScene execute exception: {0}".format(e))
                pass

            # 4
            try:
                global_broadcast_scn = self.ob_connector.execute_sql("select * from oceanbase.CDB_OB_MAJOR_COMPACTION where TENANT_ID='{0}';".format(err_tenant_id))[0][3]
                tenant_record.add_record("global_broadcast_scn is {0}".format(global_broadcast_scn))
                last_scn = self.ob_connector.execute_sql("select LAST_SCN from oceanbase.CDB_OB_MAJOR_COMPACTION where TENANT_ID='{0}';".format(err_tenant_id))[0]
                tenant_record.add_record("last_scn is {0}".format(last_scn))

                sql = "select * from oceanbase.GV$OB_COMPACTION_PROGRESS where TENANT_ID='{0}' and COMPACTION_SCN='{1}';".format(err_tenant_id, global_broadcast_scn)
                OB_COMPACTION_PROGRESS_data_global_broadcast_scn = self.ob_connector.execute_sql(sql)
                file_name = "{0}/rca_major_hold_{1}_OB_COMPACTION_PROGRESS_data_global_broadcast_scn".format(self.local_path, err_tenant_id)
                with open(file_name, 'w') as f:
                    f.write(str(OB_COMPACTION_PROGRESS_data_global_broadcast_scn))
                tenant_record.add_record("tenant_id:{0} OB_COMPACTION_PROGRESS_data_global_broadcast_scn save on {1}".format(err_tenant_id, file_name))

                sql = "select * from oceanbase.GV$OB_COMPACTION_PROGRESS where TENANT_ID='{0}' and COMPACTION_SCN='{1}';".format(err_tenant_id, last_scn)
                OB_COMPACTION_PROGRESS_data_last_scn = self.ob_connector.execute_sql(sql)
                file_name = "{0}/rca_major_hold_{1}_OB_COMPACTION_PROGRESS_data_last_scn".format(self.local_path, err_tenant_id)
                with open(file_name, 'w') as f:
                    f.write(str(OB_COMPACTION_PROGRESS_data_last_scn))
                tenant_record.add_record("tenant_id:{0} OB_COMPACTION_PROGRESS_data_last_scn save on {1}".format(err_tenant_id, file_name))

                sql = "select * from oceanbase.GV$OB_COMPACTION_PROGRESS where TENANT_ID='{0}' and STATUS<>'FINISH';".format(err_tenant_id, global_broadcast_scn)
                finish_data = self.ob_connector.execute_sql(sql)
                if len(finish_data) == 0:
                    tenant_record.add_record("sql:{0},len of result is 0;result:{1}".format(sql, finish_data))
                    sql = "select * from oceanbase. where TENANT_ID='{0}' and LS_ID=1".format(err_tenant_id)
                    svrs = self.ob_connector.execute_sql(sql)
                    svr_ip = svrs[0][4]
                    svr_port = svrs[0][5]
                    node = None
                    ssh_helper = None
                    for observer_node in self.observer_nodes:
                        if observer_node["ip"] == svr_ip and observer_node["port"] == svr_port:
                            node = observer_node
                            ssh_helper = observer_node["ssher"]
                    if node == None:
                        self.stdio.error("can not find ls_svr by TENANT_ID:{2} ip:{0},port:{1}".format(svr_ip, svr_port, err_tenant_id))
                        break

                    log_name = "/tmp/major_hold_scene_4_major_merge_progress_checker_{0}.log".format(err_tenant_id)
                    ssh_helper.ssh_exec_cmd('grep "major_merge_progress_checker" {0}/log/rootservice.log* | grep T{1} -m500 >{2}'.format(node.get("home_path"), err_tenant_id, log_name))
                    ssh_helper.download(log_name, self.local_path)
                    tenant_record.add_record("download {0} to {1}".format(log_name, self.local_path))
                    ssh_helper.ssh_exec_cmd("rm -rf {0}".format(log_name))
            except Exception as e:
                self.stdio.error("MajorHoldScene execute 4 exception: {0}".format(e))
                raise RCAExecuteException("MajorHoldScene execute 4 exception: {0}".format(e))

            # 5
            try:
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary('select * from oceanbase.GV$OB_COMPACTION_SUGGESTIONS where tenant_id="{0}";'.format(err_tenant_id))
                columns = [column[0] for column in cursor.description]
                OB_COMPACTION_SUGGESTIONS_data = cursor.fetchall()
                OB_COMPACTION_SUGGESTIONS_info = json.dumps(OB_COMPACTION_SUGGESTIONS_data, cls=DateTimeEncoder)
                file_name = "{0}/rca_major_hold_{1}_OB_COMPACTION_SUGGESTIONS_info".format(self.local_path, err_tenant_id)
                with open(file_name, 'w') as f:
                    f.write(str(OB_COMPACTION_SUGGESTIONS_info))
                tenant_record.add_record("tenant_id:{0} OB_COMPACTION_PROGRESS_data_last_scn save on {1}".format(err_tenant_id, file_name))

            except Exception as e:
                self.stdio.warn("MajorHoldScene execute 5 exception: {0}".format(e))
            tenant_record.add_suggest("send the {0} to the oceanbase community".format(self.local_path))
            self.Result.records.append(tenant_record)

    def get_info__all_virtual_compaction_diagnose_info(self, tenant_record):
        try:
            COMPACTING_datas = self.ob_connector.execute_sql("SELECT * FROM oceanbase.__all_virtual_compaction_diagnose_info WHERE IS_ERROR = 'NO' OR IS_SUSPENDED = 'NO';")
            if len(COMPACTING_datas) == 0:
                tenant_record.add_record("sql:select * from oceanbase.__all_virtual_compaction_diagnose_info; no data")
                return
            else:
                tenant_record.add_record("sql:select * from oceanbase.CDB_OB_MAJOR_COMPACTION where status=COMPACTING; " "result:{0}".format(str(COMPACTING_datas)))
            for index, COMPACTING_data in COMPACTING_datas:
                self.diagnose_info_switch(COMPACTING_data)
        except Exception as e:
            raise RCAExecuteException("MajorHoldScene execute get_info__all_virtual_compaction_diagnose_info exception: {0}".format(e))

    def diagnose_info_switch(self, sql_data, tenant_record):
        svr_ip = sql_data[0]
        svr_port = sql_data[1]
        tenant_id = sql_data[2]
        ls_id = sql_data[4]
        table_id = sql_data[5]
        create_time = sql_data[7]
        diagnose_info = sql_data[8]
        if "schedule medium failed" in diagnose_info:
            node = None
            ssh_helper = None
            for observer_node in self.observer_nodes:
                if svr_ip == observer_node.get("ip"):
                    node = observer_node
                    ssh_helper = observer_node["ssher"]
            if node is None:
                raise RCAExecuteException("can not find observer node by ip:{0}, port:{1}".format(svr_ip, svr_port))

            log_name = "/tmp/rca_major_hold_schedule_medium_failed_{1}_{2}_{0}.txt".format(tenant_id, svr_ip, svr_port)
            tenant_record.add_record("diagnose_info type is 'schedule medium failed'. time is {0},observer is {1}:{2},the log is {3}".format(create_time, svr_ip, svr_port, log_name))
            ssh_helper.ssh_exec_cmd('grep "schedule_medium_failed" {1}/log/observer.log* |grep -P  "\[\d+\]" -m 1 -o >{0}'.format(log_name, node.get("home_path")))
            ssh_helper.download(log_name, local_path=self.local_path)
            tenant_record.add_record("download {0} to {1}".format(log_name, self.local_path))
            ssh_helper.ssh_exec_cmd("rm -rf {0}".format(log_name))
            return
        elif "error_no=" in diagnose_info and "error_trace=" in diagnose_info:
            err_no = re.search("\berror_no=(\d+)\b", diagnose_info).group(1)
            err_trace = re.search("\berror_trace=(.+)\b", diagnose_info).group(1)

            global_broadcast_scn = self.ob_connector.execute_sql("select * from oceanbase.CDB_OB_MAJOR_COMPACTION where TENANT_ID='{0}';".format(tenant_id))[0][3]
            compaction_scn = self.ob_connector.execute_sql("select * from oceanbase.__all_virtual_tablet_meta_table where tablet_id='{0}' and tenant_id='{1}';".format(table_id, tenant_id))[0][7]
            if compaction_scn > global_broadcast_scn:
                tenant_record.add_record(
                    "diagnose_info type is error_no. error_no: {0}, err_trace: {1} , table_id:{2}, tenant_id:{3}, compaction_scn: {4}, global_broadcast_scn: {5}. compaction_scn>global_broadcast_scn".format(
                        err_no, err_trace, table_id, tenant_id, compaction_scn, global_broadcast_scn
                    )
                )
                return
            else:
                tenant_record.add_record(
                    "diagnose_info type is error_no. error_no: {0}, err_trace:{1}, table_id:{2}, tenant_id:{3}, compaction_scn: {4}, global_broadcast_scn: {5}. compaction_scn<global_broadcast_scn".format(
                        err_no, err_trace, table_id, tenant_id, compaction_scn, global_broadcast_scn
                    )
                )
                node = None
                ssh_helper = None
                for observer_node in self.observer_nodes:
                    if svr_ip == observer_node.get("ip"):
                        node = observer_node
                        ssh_helper = observer_node["ssher"]
                if node is None:
                    raise RCAExecuteException("can not find observer node by ip:{0}, port:{1}".format(svr_ip, svr_port))

                log_name = "/tmp/rca_error_no_{1}_{2}_{0}.txt".format(tenant_id, svr_ip, svr_port)
                ssh_helper.ssh_exec_cmd("grep \"{0}\" {1}/log/observer.log* >{2}".format(err_trace, node.get("home_path"), log_name))
                ssh_helper.download(log_name, local_path=self.local_path)
                tenant_record.add_record("download {0} to {1}".format(log_name, self.local_path))
                ssh_helper.ssh_exec_cmd("rm -rf {0}".format(log_name))
            node = None
            ssh_helper = None
            for observer_node in self.observer_nodes:
                if svr_ip == observer_node.get("ip"):
                    node = observer_node
                    ssh_helper = observer_node["ssher"]
            if node is None:
                raise RCAExecuteException("can not find observer node by ip:{0}, port:{1}".format(svr_ip, svr_port))

            tenant_record.add_record("diagnose_info type is 'error_no'. time is {0},observer is {1}:{2},the log is {3}".format(create_time, svr_ip, svr_port, log_name))
            ssh_helper.ssh_exec_cmd('cat observer.log* |grep "{1}" > /tmp/{0}'.format(log_name, err_trace))
            ssh_helper.download(log_name, local_path=self.local_path)
            tenant_record.add_record("download {0} to {1}".format(log_name, self.local_path))
            ssh_helper.ssh_exec_cmd("rm -rf {0}".format(log_name))
            return
        elif "weak read ts is not ready" in diagnose_info:
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.__all_virtual_ls_info where tenant_id='{0}' and ls_id='{1}';".format(tenant_id, ls_id))
            columns = [column[0] for column in cursor.description]
            all_virtual_ls_info_data = cursor.fetchall()
            self.all_virtual_ls_info = json.dumps(all_virtual_ls_info_data, cls=DateTimeEncoder)
            tenant_record.add_record("sql:" + "select * from oceanbase.__all_virtual_ls_info where tenant_id='{0}' and ls_id='{1}';".format(tenant_id, ls_id) + "result:{0}".format(str(self.all_virtual_ls_info)))
            return
        elif "memtable can not create dag successfully" in diagnose_info:
            tenant_record.add_record("diagnose_info type is memtable can not create dag successfully.")

            global_broadcast_scn = self.ob_connector.execute_sql("select * from oceanbase.CDB_OB_MAJOR_COMPACTION where TENANT_ID='{0}';".format(tenant_id))[0][3]
            compaction_scn = self.ob_connector.execute_sql("select * from oceanbase.__all_virtual_tablet_meta_table where tablet_id='{0}' and tenant_id='{1}';".format(table_id, tenant_id))[0][7]
            if compaction_scn > global_broadcast_scn:
                tenant_record.add_record(
                    "diagnose_info type is memtable can not create dag successfully.   table_id:{0}, tenant_id:{1}, compaction_scn: {2}, global_broadcast_scn: {3}. compaction_scn>global_broadcast_scn".format(
                        table_id, tenant_id, compaction_scn, global_broadcast_scn
                    )
                )
                return
            else:
                cursor = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.__all_virtual_dag_scheduler where svr_ip='{0}' and svr_port='{1}' and tenant_id='{2}';".format(svr_ip, svr_port, tenant_id))
                columns = [column[0] for column in cursor.description]
                all_virtual_ls_info_data = cursor.fetchall()
                self.all_virtual_ls_info = json.dumps(all_virtual_ls_info_data, cls=DateTimeEncoder)
                tenant_record.add_record("sql:" + "select * from oceanbase.__all_virtual_dag_scheduler where svr_ip='{0}' and svr_port='{1}' and tenant_id='{2}';".format(svr_ip, svr_port, tenant_id) + "result:{0}".format(str(self.all_virtual_ls_info)))

            return
        elif "medium wait for freeze" in diagnose_info or "major wait for freeze" in diagnose_info:
            tenant_record.add_record("diagnose_info type is medium wait for freeze or major wait for freeze.")
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary("select * from oceanbase.__all_virtual_dag_scheduler where svr_ip='{0}' and svr_port='{1}' and tenant_id='{2}';".format(svr_ip, svr_port, tenant_id))
            columns = [column[0] for column in cursor.description]
            all_virtual_ls_info_data = cursor.fetchall()
            self.all_virtual_ls_info = json.dumps(all_virtual_ls_info_data, cls=DateTimeEncoder)
            tenant_record.add_record("sql:" + "select * from oceanbase.__all_virtual_dag_scheduler where svr_ip='{0}' and svr_port='{1}' and tenant_id='{2}';".format(svr_ip, svr_port, tenant_id) + "result:{0}".format(str(self.all_virtual_ls_info)))
            return
        elif "major not schedule for long time" in diagnose_info:
            tenant_record.add_record("diagnose_info type is major not schedule for long time")
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(
                "select * from oceanbase.__all_virtual_tablet_compaction_info where svr_ip='{0}' and svr_port='{1}' and tenant_id='{2}' and ls_id='{3}' and tablet_id='{4}';".format(svr_ip, svr_port, tenant_id, ls_id, table_id)
            )
            columns = [column[0] for column in cursor.description]
            all_virtual_ls_info_data = cursor.fetchall()
            all_virtual_tablet_compaction_info = json.dumps(all_virtual_ls_info_data, cls=DateTimeEncoder)
            tenant_record.add_record(
                "sql:"
                + "select * from oceanbase.__all_virtual_tablet_compaction_info where svr_ip='{0}' and svr_port='{1}' and tenant_id='{2}' and ls_id='{3}' and tablet_id='{4}';".format(svr_ip, svr_port, tenant_id, ls_id, table_id)
                + "result:{0}".format(str(all_virtual_tablet_compaction_info))
            )
            node = None
            ssh_helper = None
            for observer_node in self.observer_nodes:
                if svr_ip == observer_node.get("ip"):
                    node = observer_node
                    ssh_helper = observer_node["ssher"]
            if node is None:
                raise RCAExecuteException("can not find observer node by ip:{0}, port:{1}".format(svr_ip, svr_port))

            log_name = "/tmp/rca_major_hold_major_not_schedule_for_long_time_{1}_{2}_{0}.txt".format(create_time, svr_ip, svr_port)
            tenant_record.add_record("diagnose_info type is 'major not schedule for long time'. time is {0},observer is {1}:{2},the log is {3}".format(create_time, svr_ip, svr_port, log_name))
            thread_id = ssh_helper.ssh_exec_cmd('cat {0}/log/observer.log* |grep "MediumLoo" -m 1 |grep -P  "\[\d+\]" -m 1 -o | grep -oP "\d+"'.format(node["home_path"], tenant_id)).strip()
            ssh_helper.ssh_exec_cmd('cat {0}/log/observer.log | grep "{1}" -m 100> {2}'.format(node["home_path"], thread_id, log_name))
            ssh_helper.download(log_name, local_path=self.local_path)
            tenant_record.add_record("download {0} to {1}".format(log_name, self.local_path))
            ssh_helper.ssh_exec_cmd("rm -rf {0}".format(log_name))

        else:
            tenant_record.add_record("diagnose_info type is Unknown.")

    def export_result(self):
        return self.Result.export()

    def get_scene_info(self):
        return {
            "name": "major_hold",
            "info_en": "root cause analysis of major hold",
            "info_cn": "针对卡合并场景的根因分析",
        }


major_hold = MajorHoldScene()
