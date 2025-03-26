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
@time: 2025/03/26
@file: test.py
@desc:
"""


class Test:
    def execute(self, node):
        print("---> iP: ", node["ip"])
        print("---> ssh_username: ", node["ssh_username"])
        print("---> ssh_password: ", node["ssh_password"])
        print("---> ssh_port: ", node["ssh_port"])
        print("---> home_path: ", node["home_path"])
        print("---> data_dir: ", node["data_dir"])
        print("---> redo_dir: ", node["redo_dir"])
        print("---> ssh_key_file: ", node["ssh_key_file"])
        print("---> ssh_type: ", node["ssh_type"])
        print("---> container_name: ", node["container_name"])
        print("---> namespace: ", node["namespace"])
        print("---> pod_name: ", node["pod_name"])
        print("---> kubernetes_config_file: ", node["kubernetes_config_file"])
        print("---> host_type: ", node["host_type"])
        print("---> pod_name: ", node["pod_name"])
        return "this is a test."


test = Test()
