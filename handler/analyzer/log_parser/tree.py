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
@time: 2023/11/07
@file: tree.py
@desc:
"""
import heapq
from typing import List, Dict, Union
from common.tool import TimeUtils
from prettytable import PrettyTable


class Node:
    __slots__ = ('id', 'value_list', 'p_node', 'c_nodes', 'tree_info', 'value', 'display_id', 'host_ip', 'host_type', 'host_info', 'index')

    def __init__(self, value: Union[Dict, str]):
        if isinstance(value, dict):
            self.id = value['trace_data']['id']
            self.value = value
            self.value_list = [value]
            self.host_ip = value["host_ip"]
            self.host_type = value["host_type"]
        else:
            self.id = value
            self.value = None
            self.value_list = []
            self.host_ip = ""
            self.host_type = ""
        if len(self.host_ip) > 0 and len(self.host_type) > 0:
            self.host_info = self.host_type + "(" + self.host_ip + ")"
        else:
            self.host_info = ""
        self.tree_info = ''
        self.p_node = None
        self.c_nodes = {}
        self.index = 0

        self.display_id = 0

    @property
    def name(self):
        return self.tree_info + self.value['trace_data']['name'] if self.value else 'TRACE'

    def _get_key_str(self, key):
        if not self.value:
            return ''
        v = self.value['trace_data'].get(key)
        return '{}: {}'.format(key, v) if v else ''

    @property
    def elapsed_time(self):
        if self.value:
            return TimeUtils.trans_time(self.value['trace_data']['end_ts'] - self.value['trace_data']['start_ts'])
        return '-'

    @property
    def elapsed_time_us(self):
        if self.value:
            return self.value['trace_data']['end_ts'] - self.value['trace_data']['start_ts']
        return 0

    @property
    def tags(self):
        return self._get_key_str('tags')

    @property
    def logs(self):
        return self._get_key_str('logs')

    def merge_key(self, key, value):
        info = self.value['trace_data'].get(key, None)
        info2 = value['trace_data'].get(key, None)
        if not info:
            self.value['trace_data'][key] = info2
            return
        if not info2:
            return
        if isinstance(info, int) and isinstance(info2, int):
            self.value['trace_data'][key] = max(info, info2)
        elif isinstance(info, str) and isinstance(info2, str):
            if len(info2) > len(info):
                self.value['trace_data'][key] = info2
        elif isinstance(info, list) and isinstance(info2, list):
            for v in info2:
                if v not in info:
                    info.append(v)
        elif isinstance(info, dict) and isinstance(info2, dict):
            info.update(info2)
        else:
            pass

    def _update_value(self, value):
        if self.value:
            for key in ['tags', 'logs']:
                self.merge_key(key, value)
            self.value['trace_data']['start_ts'] = max(self.value['trace_data']['start_ts'], value['trace_data']['start_ts'])
            self.value['trace_data']['end_ts'] = max(self.value['trace_data']['end_ts'], value['trace_data']['end_ts'])
        else:
            self.value = value

    def append(self, value: Dict) -> None:
        self._update_value(value)
        self.value_list.append(value)

    def set_parent(self, node) -> None:
        self.p_node = node

    def set_child(self, node) -> None:
        self.c_nodes[node.id] = node

    def set_index(self, index) -> None:
        self.index = index

    def merge(self, node):
        assert self.id == node.id

    def __repr__(self):
        return 'id:{} pid:{}'.format(self.id, self.p_node.id if self.p_node else None)


class TreeMeta:
    TAGS = 'Tags'
    LOGS = 'Logs'
    SPAN_NAME = 'Span Name'
    ELAPSED_TIME = 'Elapsed Time'
    ID = 'ID'
    HOSTS = 'HOSTS'

    detail_header = 'Tags & Logs:\n-------------------------------------'
    details_data = '\nDetails:\n'

    def __init__(self):
        self.max_name = len(self.SPAN_NAME)
        self.max_timestamp = len(self.ELAPSED_TIME)
        self.max_tags = len(self.TAGS)
        self.max_logs = len(self.LOGS)
        self.max_hosts = len(self.HOSTS)

        self.counter = 0

    @property
    def fmt_elements(self):
        col_id_len = max(len(str(self.counter)), len(self.ID))
        return [col_id_len, (self.max_name, 2), self.max_timestamp, self.max_hosts]

    def format(self, *args):
        def format_len(elem):
            if isinstance(elem, tuple):
                keyword, ex = elem
            else:
                keyword = elem
                ex = 0
            return '{: <' + str(keyword + ex) + 's}'

        return '| %s|' % '| '.join(format_len(elem) for elem in self.fmt_elements).format(*args)

    @property
    def sep_line(self):
        li = []
        for char in self.header:
            if char == '|':
                li.append('+')
            else:
                li.append('-')
        return ''.join(li)

    @property
    def header(self):
        return self.format(self.ID, self.SPAN_NAME, self.ELAPSED_TIME, self.HOSTS)

    @property
    def length(self):
        elem_len = 0
        for i in self.fmt_elements:
            if isinstance(i, int):
                elem_len += i
            else:
                for j in i:
                    elem_len += j
        return elem_len + len(self.fmt_elements) + 1

    def body(self, index, node: Node):
        return self.format(str(index), node.name, node.elapsed_time, node.host_info)

    def topN_body(self, index, node: Node):
        topN_name = str(node.name).replace('├', '').replace('│', '').replace('└', '').replace('─', '').replace(' ', '')
        return self.format(str(index), topN_name, node.elapsed_time, node.host_info)

    def detail(self, index, node: Node):
        tags = node.tags
        logs = node.logs
        hosts = node.host_info
        st = node.value['trace_data'].get('start_ts') if node.value else None
        et = node.value['trace_data'].get('end_ts') if node.value else None
        if st and et:
            time_str = 'Elapsed: {}'.format(TimeUtils.trans_time(et - st))
        else:
            time_str = ''
        return '{} - {}  {} {}' '{}{}{}{}{}'.format(
            index,
            node.value['trace_data']['name'] if node.value else '',
            time_str,
            ('\n' + ' ' * (3 + len(str(index)))) if hosts else '',
            hosts,
            ('\n' + ' ' * (3 + len(str(index)))) if tags else '',
            tags,
            ('\n' + ' ' * (3 + len(str(index)))) if logs else '',
            logs,
        )

    def record_node_info(self, node: Node):
        self.counter += 1
        self.max_name = max(self.max_name, len(node.name))
        self.max_timestamp = max(self.max_timestamp, len(node.elapsed_time))
        self.max_hosts = max(self.max_hosts, len(node.host_info))

    def __str__(self):
        return 'TreeMeta: counter {} max_name_len {}'.format(self.counter, self.max_name)


class Tree:
    root_id = '00000000-0000-0000-0000-000000000000'

    __slots__ = ('nodes', 'meta', 'leaf_childs')

    def __init__(self):
        self.nodes: Dict[str, Node] = {}
        self.meta: Dict[Node, TreeMeta] = {}
        self.leaf_childs: Dict[str, Node] = {}

    def __len__(self):
        return len(self.nodes)

    def insert(self, di: Dict) -> None:
        id = di['trace_data']['id']
        parent_id = di['trace_data']['parent_id']
        if id not in self.nodes:
            self.nodes[id] = Node(di)
        else:
            self.nodes[id].append(di)
        id_node = self.nodes[id]

        if parent_id not in self.nodes:
            self.nodes[parent_id] = Node(parent_id)
        parent_node = self.nodes[parent_id]
        id_node.set_parent(parent_node)
        parent_node.set_child(id_node)

    @property
    def root(self) -> Union[Node, None]:
        return self.nodes.get(self.root_id, None)

    def no_parent_node(self) -> List[Node]:
        for node in self.nodes.values():
            if node.p_node is None:
                yield node

    def record_meta(self, root_node, max_recursion, order_by='start_ts'):
        node_chars = '└─├─'
        child_chars = '  │ '
        meta_data = TreeMeta()

        def recurse(node: Node, parent_info):
            if max_recursion != -1 and len(parent_info) / 2 > max_recursion:
                return
            if parent_info:
                node.tree_info = parent_info[:-1].replace(node_chars[0:2], child_chars[0:2]).replace(node_chars[2:4], child_chars[2:4]) + parent_info[-1]
            else:
                node.tree_info = ''
            meta_data.record_node_info(node)
            node.set_index(meta_data.counter)
            if len(node.c_nodes) == 0:
                self.leaf_childs[node.id] = node
            yield node

            ordered_list = sorted(node.c_nodes.values(), key=lambda x: x.value['trace_data'][order_by])
            for index, c_node in enumerate(ordered_list):
                total = len(node.c_nodes)
                if total == 1 or index == total - 1:
                    char = node_chars[0:2]
                else:
                    char = node_chars[2:4]
                new_parent_info = parent_info + char
                yield from recurse(c_node, new_parent_info)

        yield from recurse(root_node, '')
        self.meta[root_node] = meta_data

    def _traverse(self, root_node: Node, max_recursion=3, top_n=5):
        li = []
        for node in self.record_meta(root_node, max_recursion):
            li.append(node)

        topN_li = []
        leaf_nodes = self.leaf_childs
        topN = heapq.nlargest(top_n, leaf_nodes.items(), lambda x: x[1].elapsed_time_us)
        topN_meta = TreeMeta()
        topN_counter = 0
        table = PrettyTable(['ID', 'Leaf Span Name', 'Elapsed Time', 'HOSTS'])
        table.align = 'l'
        while topN:
            topN_counter += 1
            element = heapq.heappop(topN)
            topN_name = str(element[1].name).replace('├', '').replace('│', '').replace('└', '').replace('─', '').replace(' ', '')
            table.add_row([element[1].index, topN_name, element[1].elapsed_time, element[1].host_info])
            topN_meta.record_node_info(element[1])
            topN_li.append(element[1])
        if topN_counter > 0:
            yield '\nTop time-consuming leaf span:\n'
            for line in str(table).splitlines():
                yield str(line)
            yield topN_meta.detail_header
            for node in topN_li:
                yield topN_meta.detail(node.index, node)

        meta = self.meta[root_node]
        yield meta.details_data
        yield meta.sep_line
        yield meta.header
        yield meta.sep_line
        for index, node in enumerate(li, start=1):
            yield meta.body(index, node)
        yield meta.sep_line
        yield meta.detail_header
        for index, node in enumerate(li, start=1):
            yield meta.detail(index, node)

    def traverse(self, max_recursion, topN):
        for node in self.no_parent_node():
            yield 'root node id: {}'.format(node.id)
            self.leaf_childs = {}
            for item in self._traverse(node, max_recursion, topN):
                yield item
            yield '\n\n'

    def build(self, data: list):
        for item in data:
            self.insert(item)

    def merge(self, tree):
        for node in tree.nodes.items():
            for data in node.value_list:
                self.insert(data)

    @staticmethod
    def find_parents(node: Node):
        li = [node]
        while True:
            p_node = node.p_node
            if p_node is not None:
                li.append(p_node)
                node = p_node
            else:
                break
        return li

    def search_name(self, name: str) -> list:
        nodes = set()
        for node in self.nodes.values():
            if node.value['trace_data']['name'] == name:
                parents = self.find_parents(node)
                nodes.update(parents)
        data = []
        for node in nodes:
            data.append(node.value)
        return data
