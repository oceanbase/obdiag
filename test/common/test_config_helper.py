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


import unittest
from unittest.mock import patch, MagicMock
from common.config_helper import ConfigHelper


class TestConfigHelper(unittest.TestCase):
    def setUp(self):
        self.context = MagicMock()
        self.context.stdio = MagicMock()
        self.context.options = MagicMock()
        self.context.inner_config = MagicMock()
        self.config_helper = ConfigHelper(self.context)

    @patch('common.config_helper.get_observer_version')
    @patch('common.config_helper.OBConnector')
    def test_get_cluster_name(self, mock_connector, mock_get_observer_version):
        mock_get_observer_version.return_value = "3.0.0"
        mock_connector_instance = mock_connector.return_value
        mock_connector_instance.execute_sql.return_value = [("cluster_name",)]

        cluster_name = self.config_helper.get_cluster_name()

        mock_connector.assert_called_once()
        mock_connector_instance.execute_sql.assert_called_once_with("select cluster_name from oceanbase.v$ob_cluster")
        self.assertEqual(cluster_name, "cluster_name")

    @patch('common.config_helper.get_observer_version')
    @patch('common.config_helper.OBConnector')
    def test_get_host_info_list_by_cluster(self, mock_connector, mock_get_observer_version):
        mock_get_observer_version.return_value = "3.0.0"
        mock_connector_instance = mock_connector.return_value
        mock_connector_instance.execute_sql.return_value = [("192.168.1.1", 8080, "zone1", "build_version")]

        host_info_list = self.config_helper.get_host_info_list_by_cluster()

        mock_connector.assert_called_once()
        mock_connector_instance.execute_sql.assert_called_once_with("select SVR_IP, SVR_PORT, ZONE, BUILD_VERSION from oceanbase.v$ob_cluster")
        self.assertEqual(len(host_info_list), 1)
        self.assertEqual(host_info_list[0], {"ip": "192.168.1.1"})

    @patch('common.config_helper.get_observer_version')
    @patch('common.config_helper.OBConnector')
    def test_build_configuration(self, mock_connector, mock_get_observer_version):
        mock_get_observer_version.return_value = "3.0.0"
        self.config_helper.build_configuration()


if __name__ == '__main__':
    unittest.main()
