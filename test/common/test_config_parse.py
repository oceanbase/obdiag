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
@time: 2024/8/6
@file: test_config_parse.py
@desc: 
"""

import unittest
from common.tool import ConfigOptionsParserUtil


class TestConfigParser(unittest.TestCase):

    def setUp(self):
        self.parser = ConfigOptionsParserUtil()

    def test_valid_input_case1(self):
        input_array = [
            'ocp.login.url=http://xx.xx.xx.xx:xx',
            'ocp.login.user=admin',
            'obcluster.ob_cluster_name=test',
            'obcluster.db_host=192.168.1.1',
            'obcluster.db_port=2881',
            'obcluster.tenant_sys.user=root@sys',
            'obcluster.servers.nodes[0].ip=192.168.1.1',
            'obcluster.servers.nodes[1].ip=192.168.1.2',
            'obcluster.servers.nodes[2].ip=192.168.1.3',
            'obcluster.servers.global.ssh_username=test',
            'obcluster.servers.global.ssh_password=test',
            'obcluster.servers.global.home_path=/root/observer',
            'obproxy.obproxy_cluster_name=obproxy',
            'obproxy.servers.nodes[0].ip=192.168.1.4',
            'obproxy.servers.nodes[1].ip=192.168.1.5',
            'obproxy.servers.nodes[2].ip=192.168.1.6',
            'obproxy.servers.global.ssh_username=test',
            'obproxy.servers.global.ssh_password=test',
            'obproxy.servers.global.home_path=/root/obproxy',
        ]

        expected_output = {
            'ocp': {'login': {'url': 'http://xx.xx.xx.xx:xx', 'user': 'admin', 'password': ''}},
            'obcluster': {
                'ob_cluster_name': 'test',
                'db_host': '192.168.1.1',
                'db_port': '2881',
                'tenant_sys': {'user': 'root@sys', 'password': ''},
                'servers': {'global': {'ssh_username': 'test', 'ssh_password': 'test', 'home_path': '/root/observer'}, 'nodes': [{'ip': '192.168.1.1'}, {'ip': '192.168.1.2'}, {'ip': '192.168.1.3'}]},
            },
            'obproxy': {'obproxy_cluster_name': 'obproxy', 'servers': {'global': {'ssh_username': 'test', 'ssh_password': 'test', 'home_path': '/root/obproxy'}, 'nodes': [{'ip': '192.168.1.4'}, {'ip': '192.168.1.5'}, {'ip': '192.168.1.6'}]}},
        }

        parsed_config = self.parser.parse_config(input_array)
        self.assertEqual(parsed_config, expected_output)

    def test_valid_input_case2(self):
        input_array = [
            'ocp.login.url=http://xx.xx.xx.xx:xx',
            'ocp.login.user=admin',
            'obcluster.ob_cluster_name=test',
            'obcluster.db_host=192.168.1.1',
            'obcluster.db_port=2881',
            'obcluster.tenant_sys.user=root@sys',
            'obcluster.servers.nodes[0].ip=192.168.1.1',
            'obcluster.servers.nodes[0].ssh_username=test2',
            'obcluster.servers.nodes[0].ssh_password=test2',
            'obcluster.servers.nodes[0].home_path=/root/test/observer',
            'obcluster.servers.nodes[1].ip=192.168.1.2',
            'obcluster.servers.nodes[2].ip=192.168.1.3',
            'obcluster.servers.global.ssh_username=test',
            'obcluster.servers.global.ssh_password=test',
            'obcluster.servers.global.home_path=/root/observer',
            'obproxy.obproxy_cluster_name=obproxy',
            'obproxy.servers.nodes[0].ip=192.168.1.4',
            'obproxy.servers.nodes[1].ip=192.168.1.5',
            'obproxy.servers.nodes[2].ip=192.168.1.6',
            'obproxy.servers.global.ssh_username=test',
            'obproxy.servers.global.ssh_password=test',
            'obproxy.servers.global.home_path=/root/obproxy',
        ]

        expected_output = {
            'ocp': {'login': {'url': 'http://xx.xx.xx.xx:xx', 'user': 'admin', 'password': ''}},
            'obcluster': {
                'ob_cluster_name': 'test',
                'db_host': '192.168.1.1',
                'db_port': '2881',
                'tenant_sys': {'user': 'root@sys', 'password': ''},
                'servers': {
                    'global': {'ssh_username': 'test', 'ssh_password': 'test', 'home_path': '/root/observer'},
                    'nodes': [{'home_path': '/root/test/observer', 'ip': '192.168.1.1', 'ssh_username': 'test2', 'ssh_password': 'test2'}, {'ip': '192.168.1.2'}, {'ip': '192.168.1.3'}],
                },
            },
            'obproxy': {'obproxy_cluster_name': 'obproxy', 'servers': {'global': {'ssh_username': 'test', 'ssh_password': 'test', 'home_path': '/root/obproxy'}, 'nodes': [{'ip': '192.168.1.4'}, {'ip': '192.168.1.5'}, {'ip': '192.168.1.6'}]}},
        }

        parsed_config = self.parser.parse_config(input_array)
        self.assertEqual(parsed_config, expected_output)

    def test_invalid_format(self):
        input_array = ['ocp.login.url=http://xx.xx.xx.xx:xx', 'invalid_format_string']
        with self.assertRaises(ValueError):
            self.parser.parse_config(input_array)

    def test_invalid_node_index(self):
        input_array = ['ocp.login.url=http://xx.xx.xx.xx:xx', 'obcluster.servers.nodes[not_a_number].ip=192.168.1.1']
        with self.assertRaises(ValueError):
            self.parser.parse_config(input_array)


if __name__ == '__main__':
    unittest.main()
