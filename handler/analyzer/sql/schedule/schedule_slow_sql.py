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
@time: 2024/11/04
@file: schedule_sql_audit.py
@desc:
"""
import sqlite3
import time
import threading

slowSqlSelect  = '''select 
    sql_id, 
    trace_id, 
    event, 
    client_ip, 
    table_scan as is_table_scan, 
    wait_time_micro as wait_time, 
    total_wait_time_micro as total_wait_time, 
    execute_time - total_wait_time_micro + get_plan_time cpu_time, 
    tenant_id, 
    user_id, 
    db_id, 
    request_id, 
    request_time, 
    plan_id, 
    request_id, 
    request_time, 
    client_port, 
    affected_rows, 
    return_rows, 
    partition_cnt, 
    ret_code, 
    total_waits, 
    rpc_count, 
    plan_type, 
    is_inner_sql, 
    is_executor_rpc, 
    is_hit_plan, 
    elapsed_time, 
    net_time, 
    net_wait_time, 
    queue_time, 
    decode_time, 
    get_plan_time, 
    execute_time, 
    application_wait_time, 
    concurrency_wait_time, 
    user_io_wait_time, 
    schedule_time, 
    row_cache_hit, 
    bloom_filter_cache_hit, 
    block_cache_hit, 
    block_index_cache_hit, 
    disk_reads, 
    retry_cnt, 
    consistency_level, 
    memstore_read_row_count, 
    ssstore_read_row_count 
    from `v$sql_audit` 
    where tenant_id = ? 
    and request_id >= ? 
    and request_id <= ? 
    and (elapsed_time > ? or (plan_type != 1 and elapsed_time > ? and length(query_sql) =0))'''


class SlowSQLAuditSchedule(object):
    def __init__(self, context):
        super(SlowSQLAuditSchedule, self).__init__()
        self.context = context
        self.stdio = context.stdio

    def get_last_request_time(tenant_id):
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT last_request_time FROM tenant_last_request_time WHERE tenant_id = ?", (tenant_id,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else None

    def update_last_request_time(tenant_id, last_request_time):
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO tenant_last_request_time (tenant_id, last_request_time) VALUES (?, ?)
            ON CONFLICT(tenant_id) DO UPDATE SET last_request_time=excluded.last_request_time;
        """, (tenant_id, last_request_time))
        conn.commit()
        conn.close()

    def fetch_and_store_data(tenant_id):
        last_request_time = self.get_last_request_time(tenant_id)
        
        # 连接OceanBase
        connection_ob = pymysql.connect(**OCEANBASE_CONFIG)
        cursor_ob = connection_ob.cursor()

        # 执行查询
        cursor_ob.execute(QUERY_GV_SQL_AUDIT, (tenant_id, last_request_time or '1970-01-01 00:00:00'))
        results = cursor_ob.fetchall()
        
        # 关闭OceanBase连接
        cursor_ob.close()
        connection_ob.close()

        # 如果没有新的数据，则直接返回
        if not results:
            return

        # 获取最新的request_time
        new_last_request_time = max(row['request_time'] for row in results)

        # 连接SQLite
        connection_sqlite = sqlite3.connect(SQLITE_DB_PATH)
        cursor_sqlite = connection_sqlite.cursor()

        # 创建表（如果不存在）
        cursor_sqlite.execute('''
            CREATE TABLE IF NOT EXISTS ob_sql_audit (
                -- 根据gv$ob_sql_audit的实际列定义表结构
                id INTEGER PRIMARY KEY,
                svr_ip TEXT,
                svr_port INTEGER,
                request_id INTEGER,
                tenant_id INTEGER,
                request_time DATETIME,
                ...
            )
        ''')

        # 创建用于跟踪最后请求时间的表（如果不存在）
        cursor_sqlite.execute('''
            CREATE TABLE IF NOT EXISTS tenant_last_request_time (
                tenant_id INTEGER PRIMARY KEY,
                last_request_time DATETIME
            )
        ''')

        # 插入数据
        for row in results:
            cursor_sqlite.execute('''
                INSERT INTO ob_sql_audit (id, svr_ip, svr_port, request_id, tenant_id, request_time, ...)
                VALUES (?, ?, ?, ?, ?, ?, ...);
            ''', (row['id'], row['svr_ip'], row['svr_port'], row['request_id'], row['tenant_id'], row['request_time'], ...))

        # 提交事务并关闭连接
        connection_sqlite.commit()
        cursor_sqlite.close()
        connection_sqlite.close()

        # 更新最后请求时间
        update_last_request_time(tenant_id, new_last_request_time)

    def scheduled_task():
        while True:
            print("Executing scheduled task...")
            # 执行你的定时任务
            time.sleep(60)  # 每60秒执行一次

    def job():
        # 假设我们有一个租户ID列表
        tenant_ids = [1001, 1002, 1003]  # 示例租户ID
        for tenant_id in tenant_ids:
            fetch_and_store_data(tenant_id)

    # 每小时运行一次job
    schedule.every().hour.do(job)

    # 主循环
    while True:
        schedule.run_pending()
        time.sleep(1)