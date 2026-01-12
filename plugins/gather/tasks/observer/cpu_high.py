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
@time: 2024/01/17
@file: cpu_high.py
@desc:
"""
import os
import datetime

from src.common.ssh_client.ssh import SshClient
from src.handler.gather.gather_component_log import GatherComponentLogHandler
from src.common.stdio import SafeStdio
from src.handler.gather.gather_obstack2 import GatherObstack2Handler
from src.handler.gather.gather_perf import GatherPerfHandler
from src.handler.gather.step.sql import StepSQLHandler
from src.common.ob_connector import OBConnector
from src.common.tool import TimeUtils, StringUtils, SQLTableExtractor
from src.common.command import get_observer_version
from tabulate import tabulate


class CPUHigh(SafeStdio):
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
        self.__gather_obstack()
        self.__gather_perf()
        self.__gather_current_clocksource()
        self.__gather_log()
        self.__get_processlist()
        # New enhancements for CPU high analysis
        self.__gather_top_sql_info()
        self.__gather_sql_audit_full_info()
        self.__gather_qps_distribution()

    def __gather_obstack(self):
        self.stdio.print("gather obstack start")
        obstack = GatherObstack2Handler(self.context, self.report_path, is_scene=True)
        obstack.handle()
        self.stdio.print("gather obstack end")

    def __gather_perf(self):
        self.stdio.print("gather perf start")
        perf_sample_count = self.env.get("perf_count")
        if perf_sample_count:
            self.context.set_variable('gather_perf_sample_count', perf_sample_count)
        perf = GatherPerfHandler(self.context, self.report_path, is_scene=True)
        perf.handle()
        self.stdio.print("gather perf end")

    def __gather_current_clocksource(self):
        try:
            self.stdio.print("gather current_clocksource start")
            for node in self.nodes:
                ssh_client = SshClient(self.context, node)
                cmd = 'cat /sys/devices/system/clocksource/clocksource0/current_clocksource'
                self.stdio.verbose("gather current_clocksource, run cmd = [{0}]".format(cmd))
                result = ssh_client.exec_cmd(cmd)
                file_path = os.path.join(self.report_path, "current_clocksource_{ip}_result.txt".format(ip=str(node.get("ip")).replace('.', '_')))
                self.report(file_path, cmd, result)
            self.stdio.print("gather current_clocksource end")
        except Exception as e:
            self.stdio.error("SshHandler init fail. Please check the node conf. Exception : {0} .".format(e))

    def __gather_log(self):
        try:
            self.stdio.print("gather observer log start")
            handler = GatherComponentLogHandler()
            handler.init(self.context, store_dir=self.report_path, target="observer", is_scene=True)
            handler.handle()
            self.stdio.print("gather observer log end")
        except Exception as e:
            self.stdio.error("gather observer log failed, error: {0}".format(e))
            raise Exception("gather observer log failed, error: {0}".format(e))

    def report(self, file_path, command, data):
        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write('\n\n' + 'shell > ' + command + '\n')
                f.write(data + '\n')
        except Exception as e:
            self.stdio.error("report sql result to file: {0} failed, error: ".format(file_path))

    def __get_processlist(self):
        try:
            self.stdio.print("gather processlist start")
            step = {'global': 'true', 'type': 'sql', 'sql': 'show full processlist'}
            handler = StepSQLHandler(self.context, step, self.cluster, self.report_path, self.task_variable_dict, self.env)
            handler.execute()
            self.stdio.print("gather processlist end")
        except Exception as e:
            self.stdio.error("gather processlist failed, error: {0}".format(e))
            raise Exception("gather processlist failed, error: {0}".format(e))

    def __get_time_range(self):
        """Get time range for SQL queries, default to last 30 minutes"""
        try:
            from_time = self.env.get("from_time")
            to_time = self.env.get("to_time")
            since = self.env.get("since", "30m")

            if from_time and to_time:
                from_timestamp = TimeUtils.datetime_to_timestamp(from_time, self.stdio)
                to_timestamp = TimeUtils.datetime_to_timestamp(to_time, self.stdio)
            else:
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

    def __get_sql_audit_view(self):
        """Get the correct SQL audit view name based on OceanBase version"""
        try:
            ob_version = get_observer_version(self.context)
            is_ob4 = StringUtils.compare_versions_greater(ob_version, "4.0.0.0") or ob_version.startswith("4.")
            if is_ob4:
                return "oceanbase.gv$ob_sql_audit"
            else:
                return "oceanbase.gv$sql_audit"
        except Exception as e:
            self.stdio.warn("Failed to get OceanBase version, defaulting to 3.x view name: {0}".format(e))
            return "oceanbase.gv$sql_audit"

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

    def __gather_top_sql_info(self):
        """Gather TOP SQL information by CPU usage and physical disk read rows"""
        try:
            self.stdio.print("gather top sql info start")
            from_timestamp, to_timestamp = self.__get_time_range()
            ob_connector = self.__get_ob_connector()
            sql_audit_view = self.__get_sql_audit_view()

            file_path = os.path.join(self.report_path, "top_sql_info.txt")

            # Get TOP SQL by CPU usage (execute_time)
            sql_top_cpu = """
            SELECT 
                sql_id,
                SUM(execute_time) as total_execute_time,
                SUM(QUEUE_TIME) as total_queue_time,
                COUNT(*) as execution_count,
                SUM(MEMSTORE_READ_ROW_COUNT) as total_memstore_read_rows,
                SUM(SSSTORE_READ_ROW_COUNT) as total_ssstore_read_rows
            FROM {2}
            WHERE REQUEST_TIME BETWEEN {0} AND {1}
            GROUP BY sql_id 
            ORDER BY total_execute_time DESC 
            LIMIT 10
            """.format(
                from_timestamp, to_timestamp, sql_audit_view
            )

            self.stdio.verbose("Executing SQL: {0}".format(sql_top_cpu))
            columns, data = ob_connector.execute_sql_return_columns_and_data(sql_top_cpu)
            self.__report_sql_result(file_path, sql_top_cpu, columns, data)

            # Get TOP SQL by physical disk read rows
            sql_top_disk_read = """
            SELECT 
                sql_id,
                SUM(execute_time) as total_execute_time,
                SUM(QUEUE_TIME) as total_queue_time,
                COUNT(*) as execution_count,
                SUM(MEMSTORE_READ_ROW_COUNT) as total_memstore_read_rows,
                SUM(SSSTORE_READ_ROW_COUNT) as total_ssstore_read_rows
            FROM {2}
            WHERE REQUEST_TIME BETWEEN {0} AND {1}
            GROUP BY sql_id 
            ORDER BY total_ssstore_read_rows DESC 
            LIMIT 10
            """.format(
                from_timestamp, to_timestamp, sql_audit_view
            )

            self.stdio.verbose("Executing SQL: {0}".format(sql_top_disk_read))
            columns, data = ob_connector.execute_sql_return_columns_and_data(sql_top_disk_read)
            self.__report_sql_result(file_path, sql_top_disk_read, columns, data)

            # Get SQL text and execution plans for top SQLs
            if data and len(data) > 0:
                sql_ids = [row[0] for row in data if row[0]]
                for sql_id in sql_ids[:5]:  # Process top 5 SQLs
                    self.__gather_sql_plan_info(ob_connector, file_path, sql_id, from_timestamp, to_timestamp)

            self.stdio.print("gather top sql info end")
        except Exception as e:
            self.stdio.error("gather top sql info failed, error: {0}".format(e))

    def __can_explain_sql(self, query_sql):
        """Check if a SQL statement supports EXPLAIN"""
        if not query_sql:
            return False

        # Normalize SQL for checking
        sql_upper = query_sql.strip().upper()

        # Statements that don't support EXPLAIN
        unsupported_prefixes = [
            'SHOW',
            'DESCRIBE',
            'DESC',
            'SET ',
            'USE ',
            'GRANT',
            'REVOKE',
            'CREATE USER',
            'DROP USER',
            'ALTER USER',
            'FLUSH',
            'KILL',
            'COMMIT',
            'ROLLBACK',
            'BEGIN',
            'START TRANSACTION',
        ]

        for prefix in unsupported_prefixes:
            if sql_upper.startswith(prefix):
                return False

        # Statements that support EXPLAIN
        supported_prefixes = [
            'SELECT',
            'INSERT',
            'UPDATE',
            'DELETE',
            'REPLACE',
            'WITH',
        ]

        for prefix in supported_prefixes:
            if sql_upper.startswith(prefix):
                return True

        # For other statements, return False to be safe
        return False

    def __gather_sql_plan_info(self, ob_connector, file_path, sql_id, from_timestamp, to_timestamp):
        """Gather execution plan and GV$OB_SQL_PLAN info for a specific SQL"""
        try:
            sql_audit_view = self.__get_sql_audit_view()
            # Get SQL text
            sql_get_sql_text = """
            SELECT DISTINCT query_sql 
            FROM {3}
            WHERE sql_id = '{0}' 
            AND REQUEST_TIME BETWEEN {1} AND {2}
            AND query_sql IS NOT NULL 
            AND query_sql != ''
            LIMIT 1
            """.format(
                sql_id.replace("'", "''"), from_timestamp, to_timestamp, sql_audit_view
            )

            columns, data = ob_connector.execute_sql_return_columns_and_data(sql_get_sql_text)
            if data and len(data) > 0:
                query_sql = data[0][0]
                self.__report_sql_result(file_path, "SQL Text for sql_id={0}".format(sql_id), ["query_sql"], [[query_sql]])

                # Get execution plan using EXPLAIN (only for statements that support it)
                if self.__can_explain_sql(query_sql):
                    try:
                        explain_sql = "EXPLAIN {0}".format(query_sql)
                        columns, data = ob_connector.execute_sql_return_columns_and_data(explain_sql)
                        self.__report_sql_result(file_path, "Execution Plan for sql_id={0}".format(sql_id), columns, data)
                    except Exception as e:
                        self.stdio.warn("Failed to get execution plan for sql_id={0}: {1}".format(sql_id, e))
                else:
                    self.stdio.verbose("Skipping EXPLAIN for sql_id={0} (statement type doesn't support EXPLAIN)".format(sql_id))

            # Get GV$OB_SQL_PLAN information (only available in OceanBase 4.2.0.0 and later)
            try:
                ob_version = get_observer_version(self.context)
                is_ob420_or_later = StringUtils.compare_versions_greater(ob_version, "4.2.0.0") or ob_version == "4.2.0.0"
            except Exception as e:
                self.stdio.warn("Failed to get OceanBase version for GV$OB_SQL_PLAN check: {0}".format(e))
                is_ob420_or_later = False

            if is_ob420_or_later:
                sql_get_plan_info = """
                SELECT * 
                FROM oceanbase.GV$OB_SQL_PLAN 
                WHERE sql_id = '{0}'
                LIMIT 20
                """.format(
                    sql_id.replace("'", "''")
                )

                columns, data = ob_connector.execute_sql_return_columns_and_data(sql_get_plan_info)
                self.__report_sql_result(file_path, "GV$OB_SQL_PLAN for sql_id={0}".format(sql_id), columns, data)
            else:
                self.stdio.verbose("Skipping GV$OB_SQL_PLAN query for sql_id={0} (requires OceanBase 4.2.0.0 or later)".format(sql_id))

            # Get partition distribution for tables in the SQL
            self.__gather_partition_distribution(ob_connector, file_path, sql_id, from_timestamp, to_timestamp)

        except Exception as e:
            self.stdio.error("gather sql plan info failed for sql_id={0}, error: {1}".format(sql_id, e))

    def __gather_partition_distribution(self, ob_connector, file_path, sql_id, from_timestamp, to_timestamp):
        """Gather cluster-level and table-level partition distribution"""
        try:
            # Get OceanBase version to determine SQL syntax
            try:
                ob_version = get_observer_version(self.context)
                is_ob4 = StringUtils.compare_versions_greater(ob_version, "4.0.0.0") or ob_version.startswith("4.")
            except Exception as e:
                self.stdio.warn("Failed to get OceanBase version, assuming 4.x: {0}".format(e))
                is_ob4 = True

            # Get SQL text from sql_audit to parse table names
            # Use different view names for 3.x and 4.x
            sql_audit_view = self.__get_sql_audit_view()

            sql_get_sql_text = """
            SELECT DISTINCT 
                db_name,
                query_sql
            FROM {0}
            WHERE sql_id = '{1}' 
            AND REQUEST_TIME BETWEEN {2} AND {3}
            AND db_name IS NOT NULL 
            AND query_sql IS NOT NULL
            AND query_sql != ''
            LIMIT 5
            """.format(
                sql_audit_view, sql_id.replace("'", "''"), from_timestamp, to_timestamp
            )

            columns, data = ob_connector.execute_sql_return_columns_and_data(sql_get_sql_text)
            if not data or len(data) == 0:
                return

            # Parse SQL to extract table names
            parser = SQLTableExtractor()
            tables_found = set()

            for row in data:
                db_name = row[0]
                query_sql = row[1]

                # Parse SQL to get table names
                try:
                    parse_tables = parser.parse(query_sql)
                    for parsed_db, parsed_table in parse_tables:
                        # Use parsed database name if available, otherwise use db_name from audit
                        final_db_name = parsed_db if parsed_db else db_name
                        if parsed_table:
                            tables_found.add((final_db_name, parsed_table))
                except Exception as e:
                    self.stdio.warn("Failed to parse SQL for table names: {0}".format(e))
                    continue

            if not tables_found:
                return

            for db_name, table_name in tables_found:
                # Skip system tables (tables starting with __all_ or __all_virtual_)
                if table_name.startswith('__all_') or table_name.startswith('__all_virtual_'):
                    continue

                # Skip if database is oceanbase and table looks like a system table
                if db_name and db_name.lower() == 'oceanbase' and (table_name.startswith('__') or table_name.startswith('DBA_') or table_name.startswith('GV$') or table_name.startswith('V$')):
                    continue

                # Get partition distribution using __all_virtual_partition_table
                # SQL structure is similar for 3.x and 4.x, but field names might differ
                if is_ob4:
                    # OceanBase 4.x SQL
                    sql_partition_info = """
                    SELECT 
                        a.svr_ip,
                        c.database_name,
                        tb.table_name,
                        b.part_id,
                        b.part_name,
                        b.part_high_bound as part_high_value,
                        b.part_func_type,
                        b.subpart_id,
                        b.subpart_name,
                        b.subpart_high_bound as subpart_high_value,
                        b.subpart_func_type,
                        a.row_count
                    FROM oceanbase.__all_virtual_table tb
                    JOIN oceanbase.__all_virtual_partition_table a ON a.table_id = tb.table_id
                    JOIN oceanbase.__all_virtual_database c ON c.database_id = tb.database_id
                    LEFT JOIN oceanbase.__all_virtual_partition_item b ON a.partition_id = b.partition_id AND a.table_id = b.table_id
                    WHERE tb.table_name = UPPER('{0}')
                    AND c.database_name = UPPER('{1}')
                    AND a.role = 1
                    ORDER BY a.svr_ip, b.part_id, b.subpart_id
                    """.format(
                        table_name.replace("'", "''"), db_name.replace("'", "''")
                    )
                else:
                    # OceanBase 3.x SQL (field names may differ slightly)
                    sql_partition_info = """
                    SELECT 
                        a.svr_ip,
                        c.database_name,
                        tb.table_name,
                        b.part_id,
                        b.part_name,
                        b.part_high_bound as part_high_value,
                        b.part_func_type,
                        b.subpart_id,
                        b.subpart_name,
                        b.subpart_high_bound as subpart_high_value,
                        b.subpart_func_type,
                        a.row_count
                    FROM oceanbase.__all_virtual_table tb
                    JOIN oceanbase.__all_virtual_partition_table a ON a.table_id = tb.table_id
                    JOIN oceanbase.__all_virtual_database c ON c.database_id = tb.database_id
                    LEFT JOIN oceanbase.__all_virtual_partition_item b ON a.partition_id = b.partition_id AND a.table_id = b.table_id
                    WHERE tb.table_name = UPPER('{0}')
                    AND c.database_name = UPPER('{1}')
                    AND a.role = 1
                    ORDER BY a.svr_ip, b.part_id, b.subpart_id
                    """.format(
                        table_name.replace("'", "''"), db_name.replace("'", "''")
                    )

                try:
                    columns, partition_data = ob_connector.execute_sql_return_columns_and_data(sql_partition_info)
                    if partition_data and len(partition_data) > 0:
                        self.__report_sql_result(file_path, "Partition Distribution for {0}.{1} (sql_id={2})".format(db_name, table_name, sql_id), columns, partition_data)
                except Exception as e:
                    error_msg = str(e)
                    # Skip silently if the partition table view doesn't exist (version compatibility issue)
                    if "doesn't exist" in error_msg and "__all_virtual_partition_table" in error_msg:
                        continue
                    # Skip silently for system tables that don't have partition info
                    if "doesn't exist" in error_msg:
                        continue
                    # Only warn for other errors
                    self.stdio.warn("Failed to get partition info for {0}.{1}: {2}".format(db_name, table_name, e))

                # Check table structure for function-based partitions
                self.__check_table_structure(ob_connector, file_path, db_name, table_name)

        except Exception as e:
            self.stdio.error("gather partition distribution failed, error: {0}".format(e))

    def __check_table_structure(self, ob_connector, file_path, db_name, table_name):
        """Check table structure for function-based partitions"""
        try:
            # Get table creation statement
            sql_get_create_table = """
            SHOW CREATE TABLE `{0}`.`{1}`
            """.format(
                db_name.replace("`", "``"), table_name.replace("`", "``")
            )

            columns, data = ob_connector.execute_sql_return_columns_and_data(sql_get_create_table)
            if data and len(data) > 0:
                create_stmt = data[0][1] if len(data[0]) > 1 else ""
                self.__report_sql_result(file_path, "Table Structure for {0}.{1}".format(db_name, table_name), ["Create Table"], [[create_stmt]])

                # Check for function-based partitions (e.g., to_days)
                if create_stmt and ('to_days' in create_stmt.lower() or 'to_days(' in create_stmt.lower()):
                    self.stdio.warn("Function-based partition detected in {0}.{1}: to_days()".format(db_name, table_name))
                    with open(file_path, 'a', encoding='utf-8') as f:
                        f.write("\n\nWARNING: Function-based partition detected in {0}.{1} - this may cause routing issues!\n".format(db_name, table_name))
        except Exception as e:
            self.stdio.warn("Failed to check table structure for {0}.{1}: {2}".format(db_name, table_name, e))

    def __gather_sql_audit_full_info(self):
        """Gather full sql audit information for the collection time period"""
        try:
            self.stdio.print("gather sql audit full info start")
            from_timestamp, to_timestamp = self.__get_time_range()
            ob_connector = self.__get_ob_connector()
            sql_audit_view = self.__get_sql_audit_view()

            file_path = os.path.join(self.report_path, "sql_audit_full_info.txt")

            # Get all columns from sql audit view for the time period
            sql_full_audit = """
            SELECT * 
            FROM {2}
            WHERE REQUEST_TIME BETWEEN {0} AND {1}
            ORDER BY REQUEST_TIME DESC
            LIMIT 1000
            """.format(
                from_timestamp, to_timestamp, sql_audit_view
            )

            self.stdio.verbose("Executing SQL: {0}".format(sql_full_audit))
            columns, data = ob_connector.execute_sql_return_columns_and_data(sql_full_audit)
            self.__report_sql_result(file_path, sql_full_audit, columns, data)

            self.stdio.print("gather sql audit full info end")
        except Exception as e:
            self.stdio.error("gather sql audit full info failed, error: {0}".format(e))

    def __gather_qps_distribution(self):
        """Gather QPS request distribution and plan type analysis"""
        try:
            self.stdio.print("gather qps distribution start")
            from_timestamp, to_timestamp = self.__get_time_range()
            ob_connector = self.__get_ob_connector()
            sql_audit_view = self.__get_sql_audit_view()

            file_path = os.path.join(self.report_path, "qps_distribution.txt")

            # Get QPS distribution by zone and server
            sql_qps_distribution = """
            SELECT 
                t2.zone,
                t1.svr_ip,
                COUNT(*) as QPS
            FROM {2} t1
            JOIN oceanbase.__all_server t2 ON t1.svr_ip = t2.svr_ip
            WHERE t1.IS_EXECUTOR_RPC = 0 
            AND t1.request_time > {0}
            AND t1.request_time < {1}
            GROUP BY t1.svr_ip, t2.zone
            ORDER BY QPS DESC
            LIMIT 20
            """.format(
                from_timestamp, to_timestamp, sql_audit_view
            )

            self.stdio.verbose("Executing SQL: {0}".format(sql_qps_distribution))
            columns, data = ob_connector.execute_sql_return_columns_and_data(sql_qps_distribution)
            self.__report_sql_result(file_path, sql_qps_distribution, columns, data)

            # For top QPS nodes, get plan type distribution
            if data and len(data) > 0:
                top_nodes = [row[1] for row in data[:5] if row[1]]  # Top 5 nodes
                for svr_ip in top_nodes:
                    if not svr_ip:
                        continue
                    # Escape single quotes for SQL safety
                    safe_svr_ip = str(svr_ip).replace("'", "''")
                    sql_plan_type = """
                    SELECT 
                        plan_type,
                        COUNT(*) as QPS
                    FROM {3} t1
                    JOIN oceanbase.__all_server t2 ON t1.svr_ip = t2.svr_ip
                    WHERE t1.IS_EXECUTOR_RPC = 0 
                    AND t1.request_time > {0}
                    AND t1.request_time < {1}
                    AND t1.svr_ip = '{2}'
                    GROUP BY plan_type
                    ORDER BY QPS DESC
                    """.format(
                        from_timestamp, to_timestamp, safe_svr_ip, sql_audit_view
                    )

                    columns, plan_data = ob_connector.execute_sql_return_columns_and_data(sql_plan_type)
                    self.__report_sql_result(file_path, "Plan Type Distribution for svr_ip={0}".format(svr_ip), columns, plan_data)

            self.stdio.print("gather qps distribution end")
        except Exception as e:
            self.stdio.error("gather qps distribution failed, error: {0}".format(e))


cpu_high = CPUHigh()
