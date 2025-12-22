#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
@time: 2025/12/08
@file: rollback_transaction.py
@desc: Collect rollback transaction information
"""
import os
import datetime

from src.common.stdio import SafeStdio
from src.handler.gather.step.sql import StepSQLHandler
from src.common.ob_connector import OBConnector
from src.common.tool import TimeUtils, StringUtils
from src.common.command import get_observer_version
from tabulate import tabulate


class RollbackTransaction(SafeStdio):
    def init(self, context, scene_name, report_path, task_variable_dict=None, env={}):
        self.context = context
        self.stdio = context.stdio
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.report_path = report_path
        self.env = self.context.get_variable("env") or {}
        self.is_ssh = True
        self.nodes = self.context.cluster_config['servers']
        self.cluster = self.context.cluster_config
        self.ob_nodes = self.context.cluster_config['servers']

    def execute(self):
        """Execute rollback transaction collection"""
        self.__gather_cluster_architecture()
        self.__gather_tenant_memory_usage()
        self.__gather_top_memory_modules()
        self.__gather_rollback_transactions()

    def __gather_cluster_architecture(self):
        """Collect current cluster architecture"""
        try:
            self.stdio.print("gather cluster architecture start")
            # Get version to determine which SQL to use
            try:
                ob_version = get_observer_version(self.context)
                is_ob4 = StringUtils.compare_versions_greater(ob_version, "4.0.0.0") or ob_version.startswith("4.")
            except Exception as e:
                self.stdio.warn("Failed to get OceanBase version, assuming 4.x: {0}".format(e))
                is_ob4 = True

            # Collect cluster architecture based on version
            if is_ob4:
                # OceanBase 4.x SQL queries
                steps = [
                    {'type': 'sql', 'sql': "show variables like 'version_comment';", 'global': True},
                    {'type': 'sql', 'sql': "SELECT * FROM oceanbase.DBA_OB_ZONES ORDER BY ZONE;", 'global': True},
                    {'type': 'sql', 'sql': "SELECT * FROM oceanbase.DBA_OB_SERVERS ORDER BY ZONE;", 'global': True},
                    {
                        'type': 'sql',
                        'sql': "SELECT SVR_IP,SVR_PORT,ZONE,SQL_PORT,CPU_CAPACITY,CPU_CAPACITY_MAX,CPU_ASSIGNED,CPU_ASSIGNED_MAX, concat(ROUND(MEM_CAPACITY/1024/1024/1024,0), 'G') as MEM_CAPACITY, concat(ROUND(MEM_ASSIGNED/1024/1024/1024,0), 'G') as MEM_ASSIGNED, concat(ROUND(LOG_DISK_CAPACITY/1024/1024/1024,0), 'G') as LOG_DISK_CAPACITY, concat(ROUND(LOG_DISK_ASSIGNED/1024/1024/1024,0), 'G') as LOG_DISK_ASSIGNED, concat(ROUND(LOG_DISK_IN_USE/1024/1024/1024,0), 'G') as LOG_DISK_IN_USE, concat(ROUND(DATA_DISK_CAPACITY/1024/1024/1024,0), 'G') as DATA_DISK_CAPACITY,concat(ROUND(DATA_DISK_IN_USE/1024/1024/1024,0), 'G') as DATA_DISK_IN_USE,concat(ROUND(MEMORY_LIMIT/1024/1024/1024,0), 'G') as MEMORY_LIMIT FROM oceanbase.GV$OB_SERVERS;",
                        'global': True,
                    },
                    {'type': 'sql', 'sql': "SELECT * FROM oceanbase.DBA_OB_UNIT_CONFIGS;", 'global': True},
                    {'type': 'sql', 'sql': "SELECT * FROM oceanbase.DBA_OB_RESOURCE_POOLS;", 'global': True},
                    {'type': 'sql', 'sql': "SELECT * FROM oceanbase.DBA_OB_TENANTS;", 'global': True},
                    {
                        'type': 'sql',
                        'sql': "SELECT c.TENANT_ID, e.TENANT_NAME, concat(c.NAME, ': ', d.NAME) `pool:conf`,concat(c.UNIT_COUNT, ' unit: ', d.min_cpu, 'C/', ROUND(d.MEMORY_SIZE/1024/1024/1024,0), 'G') unit_info FROM oceanbase.DBA_OB_RESOURCE_POOLS c, oceanbase.DBA_OB_UNIT_CONFIGS d, oceanbase.DBA_OB_TENANTS e WHERE c.UNIT_CONFIG_ID=d.UNIT_CONFIG_ID AND c.TENANT_ID=e.TENANT_ID AND c.TENANT_ID>1000 ORDER BY c.TENANT_ID;",
                        'global': True,
                    },
                    {'type': 'sql', 'sql': "SELECT a.TENANT_NAME,a.TENANT_ID,b.SVR_IP FROM oceanbase.DBA_OB_TENANTS a, oceanbase.GV$OB_UNITS b WHERE a.TENANT_ID=b.TENANT_ID;", 'global': True},
                ]
            else:
                # OceanBase 3.x SQL queries
                steps = [
                    {'type': 'sql', 'sql': "show variables like 'version_comment';", 'global': True},
                    {'type': 'sql', 'sql': "SELECT * FROM oceanbase.v$ob_cluster", 'global': True},
                    {'type': 'sql', 'sql': "SELECT * FROM oceanbase.__all_zone WHERE name='idc';", 'global': True},
                    {'type': 'sql', 'sql': "select svr_ip,zone,with_rootserver,status,block_migrate_in_time,start_service_time,stop_time,build_version from oceanbase.__all_server order by zone;", 'global': True},
                    {
                        'type': 'sql',
                        'sql': "SELECT zone, concat(svr_ip, ':', svr_port) observer, cpu_capacity, cpu_total, cpu_assigned, cpu_assigned_percent, mem_capacity, mem_total, mem_assigned, mem_assigned_percent, unit_Num, round(`load`, 2) `load`, round(cpu_weight, 2) cpu_weight, round(memory_weight, 2) mem_weight, leader_count FROM oceanbase.__all_virtual_server_stat ORDER BY zone,svr_ip;",
                        'global': True,
                    },
                    {'type': 'sql', 'sql': "select tenant_id,tenant_name,primary_zone,compatibility_mode from oceanbase.__all_tenant;", 'global': True},
                    {'type': 'sql', 'sql': "select count(*),tenant_id,zone_list,unit_count from oceanbase.__all_resource_pool group by tenant_id,zone_list,unit_count;", 'global': True},
                ]

            for step in steps:
                try:
                    handler = StepSQLHandler(self.context, step, self.cluster, self.report_path, self.task_variable_dict, self.env)
                    handler.execute()
                except Exception as e:
                    self.stdio.warn("Failed to execute SQL step: {0}, error: {1}".format(step.get('sql', 'unknown'), e))

            self.stdio.print("gather cluster architecture end")
        except Exception as e:
            self.stdio.error("gather cluster architecture failed, error: {0}".format(e))

    def __gather_tenant_memory_usage(self):
        """Collect tenant memory usage"""
        try:
            self.stdio.print("gather tenant memory usage start")
            try:
                ob_version = get_observer_version(self.context)
                is_ob4 = StringUtils.compare_versions_greater(ob_version, "4.0.0.0") or ob_version.startswith("4.")
            except Exception as e:
                self.stdio.warn("Failed to get OceanBase version, assuming 4.x: {0}".format(e))
                is_ob4 = True

            if is_ob4:
                # OceanBase 4.x: Query tenant memory usage
                step = {
                    'type': 'sql',
                    'sql': "select tenant_name, svr_ip,  memstore_limit /(1024 * 1024 * 1024) as memstore_limit_GB,  freeze_trigger /(1024 * 1024 * 1024) as freeze_trigger_GB,  memstore_used /(1024 * 1024 * 1024) as memstore_used_GB,  concat((memstore_used * 100 / memstore_limit), '%') as memstore_used_percent,  active_span /(1024 * 1024 * 1024) as active_span_GB,  freeze_cnt   from oceanbase.GV$OB_MEMSTORE memstore_info  inner join oceanbase.DBA_OB_TENANTS tenant on  memstore_info.tenant_id = tenant.tenant_id  ORDER BY tenant.tenant_name,svr_ip;",
                    'global': True,
                }
            else:
                # OceanBase 3.x: Query tenant memory usage
                step = {
                    'type': 'sql',
                    'sql': "select NOW() 'check_time',b.tenant_id,b.tenant_name, a.SVR_IP , round(sum(case when a.STAT_ID='140003' then a.VALUE else 0 end)/1024/1024/1024,2) 'mem_usage/G', round(sum(case when a.STAT_ID='140002' then a.VALUE else 0 end)/1024/1024/1024,2) 'mem_max/G' ,round(100*(sum(case when STAT_ID='140003' then a.VALUE else 0 end)/sum(case when STAT_ID='140002' then a.VALUE else 0 end)),2) 'mem_percent' from   oceanbase.gv$sysstat a inner join oceanbase.__all_tenant b on a.con_id = b.tenant_id where a.stat_id IN (140003,140002) and (a.con_id > 1000 or a.con_id = 1) and a.class < 1000 group by  b.tenant_id,b.tenant_name, a.SVR_IP order by b.tenant_name,a.SVR_IP;",
                    'global': True,
                }

            handler = StepSQLHandler(self.context, step, self.cluster, self.report_path, self.task_variable_dict, self.env)
            handler.execute()
            self.stdio.print("gather tenant memory usage end")
        except Exception as e:
            self.stdio.error("gather tenant memory usage failed, error: {0}".format(e))

    def __gather_top_memory_modules(self):
        """Collect top 10 memory modules in tenant"""
        try:
            self.stdio.print("gather top memory modules start")
            try:
                ob_version = get_observer_version(self.context)
                is_ob4 = StringUtils.compare_versions_greater(ob_version, "4.0.0.0") or ob_version.startswith("4.")
            except Exception as e:
                self.stdio.warn("Failed to get OceanBase version, assuming 4.x: {0}".format(e))
                is_ob4 = True

            if is_ob4:
                # OceanBase 4.x: Query top memory modules
                step = {
                    'type': 'sql',
                    'sql': "SELECT CASE WHEN tenant_name IS NULL THEN TENANT_ID ELSE tenant_name END tenant_name, host,ctx_name, mod_name, hold, used, count FROM ( SELECT tenant_name,tenant_id,HOST,ctx_name,mod_name,hold,used,COUNT, ROW_NUMBER () OVER ( PARTITION BY tenant_name, HOST ORDER BY hold desc) rnum FROM   (SELECT  b.tenant_name, a.tenant_id, concat(a.svr_ip, ':', a.svr_port) HOST,  a.ctx_name, a.mod_name,  round(a.hold / 1024 / 1024 / 1024) hold, round(a.used / 1024 / 1024 / 1024) used, a.COUNT FROM  oceanbase.__all_virtual_memory_info a  LEFT JOIN oceanbase.__all_tenant b ON a.TENANT_ID = b.TENANT_ID  WHERE a.hold > 1024 * 1024 * 1024 )) WHERE rnum <= 10 ORDER BY tenant_name, HOST, hold DESC;",
                    'global': True,
                }
            else:
                # OceanBase 3.x: Query top memory modules
                step = {'type': 'sql', 'sql': "select `CONTEXT`, ROUND(USED / 1024 / 1024 / 1024, 2) as USED_GB from oceanbase.gv$memory group by `CONTEXT` ORDER BY USED DESC limit 10;", 'global': True}

            handler = StepSQLHandler(self.context, step, self.cluster, self.report_path, self.task_variable_dict, self.env)
            handler.execute()
            self.stdio.print("gather top memory modules end")
        except Exception as e:
            self.stdio.error("gather top memory modules failed, error: {0}".format(e))

    def __get_time_range(self):
        """Get time range for SQL queries - parameters are passed through env"""
        try:
            # Get from_time and to_time from env (passed via --from and --to parameters)
            from_time = self.env.get("from_time")
            to_time = self.env.get("to_time")
            since = self.env.get("since", "30m")

            if from_time and to_time:
                from_timestamp = TimeUtils.datetime_to_timestamp(from_time, self.stdio)
                to_timestamp = TimeUtils.datetime_to_timestamp(to_time, self.stdio)
            else:
                # If not provided, use default 30 minutes
                now_time = datetime.datetime.now()
                to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
                from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since))).strftime('%Y-%m-%d %H:%M:%S')
                from_timestamp = TimeUtils.datetime_to_timestamp(from_time_str, self.stdio)
                to_timestamp = TimeUtils.datetime_to_timestamp(to_time_str, self.stdio)

            return from_timestamp, to_timestamp
        except Exception as e:
            self.stdio.warn("Failed to parse time range, using default 30 minutes: {0}".format(e))
            now_time = datetime.datetime.now()
            to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            from_timestamp = TimeUtils.datetime_to_timestamp(from_time_str, self.stdio)
            to_timestamp = TimeUtils.datetime_to_timestamp(to_time_str, self.stdio)
            return from_timestamp, to_timestamp

    def __get_ob_connector(self):
        """Get OBConnector instance"""
        try:
            return OBConnector(
                context=self.context, ip=self.cluster.get("db_host"), port=self.cluster.get("db_port"), username=self.cluster.get("tenant_sys").get("user"), password=self.cluster.get("tenant_sys").get("password"), timeout=10000, database="oceanbase"
            )
        except Exception as e:
            self.stdio.error("Failed to create OBConnector: {0}".format(e))
            raise

    def __report_sql_result(self, file_path, sql, columns, data):
        """Report SQL query result to file"""
        try:
            table_data = [list(row) for row in data] if data else []
            formatted_table = tabulate(table_data, headers=columns, tablefmt="grid")

            with open(file_path, 'a', encoding='utf-8') as f:
                f.write('\n\n' + 'obclient > ' + sql + '\n')
                f.write(formatted_table)
                f.write('\n')
        except Exception as e:
            self.stdio.error("report sql result to file: {0} failed, error: {1}".format(file_path, str(e)))

    def __gather_rollback_transactions(self):
        """Collect rollback transactions with trace_id and SQL"""
        try:
            self.stdio.print("gather rollback transactions start")
            from_timestamp, to_timestamp = self.__get_time_range()
            ob_connector = self.__get_ob_connector()

            # Get OceanBase version to determine SQL audit view name
            try:
                ob_version = get_observer_version(self.context)
                is_ob4 = StringUtils.compare_versions_greater(ob_version, "4.0.0.0") or ob_version.startswith("4.")
            except Exception as e:
                self.stdio.warn("Failed to get OceanBase version, assuming 4.x: {0}".format(e))
                is_ob4 = True

            # Use different view names for 3.x and 4.x
            if is_ob4:
                sql_audit_view = "oceanbase.gv$ob_sql_audit"
            else:
                sql_audit_view = "oceanbase.gv$sql_audit"

            file_path = os.path.join(self.report_path, "rollback_transactions.txt")

            # Get rollback transactions from sql_audit view
            # ret_code = '-6002' means OB_TRANS_ROLLBACKED
            sql_rollback_transactions = """
            SELECT 
                trace_id,
                query_sql,
                tenant_name,
                svr_ip,
                svr_port,
                from_unixtime(REQUEST_TIME/1000000) as request_time,
                from_unixtime((REQUEST_TIME + ELAPSED_TIME)/1000000) as end_time,
                ELAPSED_TIME/1000 as elapsed_time_ms,
                RET_CODE,
                user_name,
                db_name,
                sql_id
            FROM {2}
            WHERE REQUEST_TIME BETWEEN {0} AND {1}
            AND RET_CODE = '-6002'
            AND query_sql IS NOT NULL
            AND query_sql != ''
            ORDER BY REQUEST_TIME DESC
            LIMIT 1000
            """.format(
                from_timestamp, to_timestamp, sql_audit_view
            )

            self.stdio.verbose("Executing SQL: {0}".format(sql_rollback_transactions))
            columns, data = ob_connector.execute_sql_return_columns_and_data(sql_rollback_transactions)

            if data and len(data) > 0:
                self.__report_sql_result(file_path, sql_rollback_transactions, columns, data)
                self.stdio.print("Found {0} rollback transactions in the time range".format(len(data)))

                # Also create a summary file with just trace_ids for easy reference
                summary_path = os.path.join(self.report_path, "rollback_transactions_summary.txt")
                with open(summary_path, 'w', encoding='utf-8') as f:
                    f.write("Rollback Transaction Summary\n")
                    f.write("=" * 80 + "\n")
                    f.write("Time Range: {0} to {1}\n".format(datetime.datetime.fromtimestamp(from_timestamp / 1000000).strftime('%Y-%m-%d %H:%M:%S'), datetime.datetime.fromtimestamp(to_timestamp / 1000000).strftime('%Y-%m-%d %H:%M:%S')))
                    f.write("Total Rollback Transactions: {0}\n\n".format(len(data)))
                    f.write("Trace IDs (can be used with SQL diagnostic function):\n")
                    f.write("-" * 80 + "\n")
                    for row in data:
                        if row[0]:  # trace_id
                            f.write("{0}\n".format(row[0]))
            else:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write("No rollback transactions found in the specified time range.\n")
                    f.write("Time Range: {0} to {1}\n".format(datetime.datetime.fromtimestamp(from_timestamp / 1000000).strftime('%Y-%m-%d %H:%M:%S'), datetime.datetime.fromtimestamp(to_timestamp / 1000000).strftime('%Y-%m-%d %H:%M:%S')))
                self.stdio.print("No rollback transactions found in the time range")

            self.stdio.print("gather rollback transactions end")
        except Exception as e:
            self.stdio.error("gather rollback transactions failed, error: {0}".format(e))


rollback_transaction = RollbackTransaction()
