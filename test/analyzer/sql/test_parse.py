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
@time: 2024/06/05
@file: test_parse.py
@desc:
"""

import unittest
from sqlgpt_parser.parser.oceanbase_parser import parser


class MyTestCase(unittest.TestCase):
    def test_create_table(self):
        sql = """
            CREATE TABLE tbl1 (c1 INT, c2 VARCHAR(50))
        """
        statment = "{'type': 'create_table', " "'table_name': 'tbl1', " "'element_list': [('c1', FieldType(), False), ('c2', FieldType(), False)]}"
        result = parser.parse(sql)
        self.assertEqual(str(result), statment)
        self.assertEqual(result["type"], "create_table")
        self.assertEqual(result["table_name"], "tbl1")
        self.assertEqual(str(result["element_list"][0][1]), "INT")

    def test_create_table_1(self):
        sql = """
CREATE TABLE `ob_hist_sql_audit_stat_0` (
  `ob_cluster_id` bigint(20) NOT NULL COMMENT 'OB的集群Id',
  `cluster_name` varchar(128) NOT NULL COMMENT 'OB的集群名称',
  `ob_tenant_id` bigint(20) NOT NULL COMMENT 'OB的租户Id',
  `ob_server_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'OB的服务Id',
  `ob_db_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'OB的数据库Id',
  `ob_user_id` bigint(20) NOT NULL DEFAULT '0' COMMENT 'OB的用户Id',
  `sql_id` varchar(32) NOT NULL DEFAULT '0' COMMENT 'SQL_ID',
  `begin_interval_time` bigint(20) NOT NULL COMMENT '统计指标的区间开始时间',
  `end_interval_time` bigint(20) NOT NULL COMMENT '统计指标的区间结束时间',
  `executions` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）执行次数',
  `affected_rows` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）更新行数',
  `return_rows` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）查询返回行数',
  `partition_cnt` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）访问分区数',
  `fail_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）结果码不为0的发生次数',
  `ret_code_4012_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）结果码-4012（OB_TIMEOUT）的发生次数',
  `ret_code_4013_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）结果码-4013（OB_ALLOCATE_MEMORY_FAILED）的发生次数',
  `ret_code_5001_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）结果码-5001（OB_ERR_PARSE_SQL）的发生次数',
  `ret_code_5024_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）结果码-5024（OB_ERR_PRIMARY_KEY_DUPLICATE）的发生次数',
  `ret_code_5167_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）结果码-5167（OB_ERR_DATA_TOO_LONG）的发生次数',
  `ret_code_5217_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）结果码-5217（OB_ERR_BAD_FIELD_ERROR）的发生次数',
  `ret_code_6002_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）结果码-6002（OB_TRANS_ROLLBACKED）的发生次数',
  `last_fail_info` bigint(20) DEFAULT NULL COMMENT '最后一次错误信息，前46存储错误时间信息，后18位存储错误码信息',
  `event_0_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）\"system internal wait\"等待事件的累计时间（微秒）（sum(case event when \"system internal wait\" then wait_time_micro else 0 end)）',
  `event_1_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）\"mysql response wait client\"等待事件的累计时间（微秒）（sum(case event when \"mysql response wait client\" then wait_time_micro else 0 end)）',
  `event_2_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）\"sync rpc\"等待事件的累计时间（微秒）（sum(case event when \"sync rpc\" then wait_time_micro else 0 end)）',
  `event_3_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）\"db file data read\"等待事件的累计时间（微秒）（sum(case event when \"db file data read\" then wait_time_micro else 0 end)）',
  `event_4_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）\"\"等待事件的累计时间（微秒）（sum(case event when \"\" then wait_time_micro else 0 end)）',
  `event_5_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）\"\"等待事件的累计时间（微秒）（sum(case event when \"\" then wait_time_micro else 0 end)）',
  `total_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）总等待时间（微秒）',
  `total_waits` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）总等待次数',
  `rpc_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）发送RPC次数',
  `plan_type_local_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）本地执行计划次数（sum(case plan_type when 1 then 1 else 0 end)）',
  `plan_type_remote_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）远程执行计划次数（sum(case plan_type when 2 then 1 else 0 end)）',
  `plan_type_dist_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）分布式执行计划次数（sum(case plan_type when 3 then 1 else 0 end)）',
  `inner_sql_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）内部SQL次数（count is_inner_sql=1）',
  `executor_rpc_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）执行RPC请求次数',
  `miss_plan_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）未命中计划缓存的次数',
  `elapsed_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）响应时间 —— 接受到请求到执行结束的总时间（微秒）',
  `max_elapsed_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间忙内的最大值）最大响应时间',
  `net_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）客户端请求传输到Observer的时间（微秒）',
  `net_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）请求在Observer从网络进入队列的时间（微秒）s',
  `queue_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）请求在队列中的等待时间（微秒）',
  `decode_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）语法解析时间（微秒）',
  `get_plan_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）生成计划的时间（微秒）',
  `execute_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）执行计划的时间（微秒）',
  `cpu_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）CPU时间（微秒）（execute_time+get_plan_time-total_wait_time_micro）',
  `max_cpu_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的最大值）最大CPU时间（微秒）',
  `application_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）所有Application类事件的总时间（微秒）',
  `concurrency_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）所有Concurrency类事件的总时间（微秒）',
  `user_io_wait_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）所有UserIO类事件的总时间（微秒）',
  `schedule_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）所有Schedule类事件的总时间（微秒）',
  `row_cache_hit` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）Row Cache命中次数',
  `bloom_filter_cache_hit` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）Bloom Filter Cache命中次数',
  `block_cache_hit` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）Block Cache命中次数',
  `block_index_cache_hit` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）Block Index Cache命中次数',
  `disk_reads` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）磁盘读次数',
  `retry_cnt` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）重试次数',
  `table_scan` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）表扫描次数',
  `consistency_level_strong` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）强一致性级别次数（sum(case consistency_level when 3 then 1 else 0 end)）',
  `consistency_level_weak` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）弱一致性级别次数（sum(case consistency_level when 2 then 1 else 0 end)）',
  `memstore_read_row_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）Memstore读行数',
  `ssstore_read_row_count` bigint(20) NOT NULL DEFAULT '0' COMMENT '（区间内的总计）Ssstore读行数',
  `min_request_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '区间内最小的执行时间',
  `max_request_time` bigint(20) NOT NULL DEFAULT '0' COMMENT '区间内最大的执行时间',
  `sql_type` bigint(20) NOT NULL DEFAULT '-1' COMMENT 'SQL的类型，1：select， 2:select for update, 3：insert，4：update, 5: delete, 6: replace',
  `max_affected_rows` bigint(20) NOT NULL DEFAULT '0' COMMENT '区间内最大的更新行数',
  `max_return_rows` bigint(20) NOT NULL DEFAULT '0' COMMENT '区间内最大的查询返回行数',
  `max_partition_cnt` bigint(20) NOT NULL DEFAULT '0' COMMENT '区间内最大的访问分区数',
  `user_client_ip_of_max_affected_rows` varchar(32) NOT NULL DEFAULT '' COMMENT '区间内最大的影响行数对应的user_client_ip',
  `user_client_ip_of_max_return_rows` varchar(32) NOT NULL DEFAULT '' COMMENT '区间内最大的返回行数对应的user_client_ip',
  `user_client_ip_of_max_partition_cnt` varchar(32) NOT NULL DEFAULT '' COMMENT '区间内最大的分区数对应的user_client_ip',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`ob_cluster_id`, `cluster_name`, `ob_tenant_id`, `begin_interval_time`, `ob_server_id`, `ob_db_id`, `ob_user_id`, `sql_id`),
  KEY `idx_ob_hist_sql_audit_stat_cluster_server_time` (`ob_cluster_id`, `cluster_name`, `ob_server_id`, `begin_interval_time`),
  KEY `idx_sql_id_begin_interval_time` (`sql_id`, `begin_interval_time`)
)
        """
        result = parser.parse(sql)
        self.assertEqual(len(result["index_list"]), 3)

    def test_select(self):
        sql = """
            SELECT * FROM T1
        """
        statment = '''{'line': 0, 'pos': 0, 'query_body': QuerySpecification(select=Select(distinct=False, select_items=[SingleColumn(expression=QualifiedNameReference(name=QualifiedName.of("*")))]), from_=Table(name=QualifiedName.of("T1"), for_update=False), order_by=[], limit=0, offset=0, for_update=False, nowait_or_wait=False), 'order_by': [], 'limit': 0, 'offset': 0}'''
        result = parser.parse(sql)
        self.assertEqual(str(result), statment)

        sql = """
                SELECT t1 FROM T1
            """
        statment = '''{'line': 0, 'pos': 0, 'query_body': QuerySpecification(select=Select(distinct=False, select_items=[SingleColumn(alias=[], expression=QualifiedNameReference(name=QualifiedName.of("t1")))]), from_=Table(name=QualifiedName.of("T1"), for_update=False), order_by=[], limit=0, offset=0, for_update=False, nowait_or_wait=False), 'order_by': [], 'limit': 0, 'offset': 0}'''
        result = parser.parse(sql)
        self.assertEqual(str(result), statment)

        sql = """
                SELECT t1 FROM T1 where t1 > 12
            """
        statment = '''{'line': 0, 'pos': 0, 'query_body': QuerySpecification(select=Select(distinct=False, select_items=[SingleColumn(alias=[], expression=QualifiedNameReference(name=QualifiedName.of("t1")))]), from_=Table(name=QualifiedName.of("T1"), for_update=False), where=ComparisonExpression(type='>', left=QualifiedNameReference(name=QualifiedName.of("t1")), right=LongLiteral(value=12)), order_by=[], limit=0, offset=0, for_update=False, nowait_or_wait=False), 'order_by': [], 'limit': 0, 'offset': 0}'''
        result = parser.parse(sql)
        print(result)
        self.assertEqual(str(result), statment)


if __name__ == '__main__':
    unittest.main()
