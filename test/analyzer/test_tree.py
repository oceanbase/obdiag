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
@time: 2023/12/09
@file: test_tree.py
@desc:
"""

from handler.analyzer.log_parser.tree import Tree

file_datas = [
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "1", "name": "open1", "id": "1", "parent_id": "00000000-0000-0000-0000-000000000000", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166233214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open2", "id": "2", "parent_id": "00000000-0000-0000-0000-000000000000", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166233214}},
    {
        'host_ip': '192.168.1.1',
        'host_type': 'OBSERVER',
        'trace_data': {"trace_id": "x", "name": "open3", "id": "3", "parent_id": "1", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662117166233214, "logs": "test log", "tags": "just a test"},
    },
    {
        'host_ip': '192.168.1.1',
        'host_type': 'OBSERVER',
        'trace_data': {
            "trace_id": "x",
            "name": "open6",
            "id": "6",
            "parent_id": "3",
            "is_follow": "false",
            "start_ts": 1662107166232204,
            "end_ts": 1662108166233214,
            "logs": "null",
            "tags": [{"sql_text": "select * from t where c1=1"}, {"hit_plan": "false"}, {"sql_id": "XXXXXXXXXXXXXXXXXXXXXX"}, {"database_id": 111111111}],
        },
    },
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open7", "id": "7", "parent_id": "6", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166433214, "logs": [{"end_ts": 1662107166433214}]}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBPROXY', 'trace_data': {"trace_id": "x", "name": "open11", "id": "11", "parent_id": "1", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107167233214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBPROXY', 'trace_data': {"trace_id": "x", "name": "open22", "id": "22", "parent_id": "2", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107173233214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBPROXY', 'trace_data': {"trace_id": "x", "name": "open12", "id": "12", "parent_id": "1", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662117166233214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBPROXY', 'trace_data': {"trace_id": "x", "name": "open13", "id": "13", "parent_id": "1", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166233314}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open23", "id": "23", "parent_id": "2", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166233314}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open32", "id": "32", "parent_id": "11", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166235214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open33", "id": "33", "parent_id": "11", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166283214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open41", "id": "41", "parent_id": "12", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166293214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open55", "id": "55", "parent_id": "32", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107166291214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open56", "id": "56", "parent_id": "32", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107167233214}},
    {'host_ip': '192.168.1.1', 'host_type': 'OBSERVER', 'trace_data': {"trace_id": "x", "name": "open66", "id": "66", "parent_id": "41", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107266233214}},
    {
        'host_ip': '192.168.1.1',
        'host_type': 'OBSERVER',
        'trace_data': {"trace_id": "x", "name": "open67", "id": "67", "parent_id": "999999", "is_follow": "false", "start_ts": 1662107166232204, "end_ts": 1662107966233214, "logs": "test log", "tags": "just a test"},
    },
]


def output(tree):
    if not tree.nodes:
        print("The analysis result is empty")
        return
    for line in tree.traverse(10, 5):
        print(line)


if __name__ == '__main__':
    tree = Tree()
    tree.build(file_datas)
    tree.traverse(5, 5)
    output(tree)
