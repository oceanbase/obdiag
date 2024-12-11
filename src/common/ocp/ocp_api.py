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
@time: 2022/6/24
# File       : ocp_api.py
# Description：
"""
# login
login = "/api/v2/iam/login"

# host
host = "/api/v2/compute/hosts"
region = "/api/v2/compute/regions"
idc = "/api/v2/compute/idcs"
host_type = "/api/v2/compute/hostTypes"
host_agent = "/api/v2/compute/hosts/%s/agent"
host_remote_info = "/api/v2/compute/hosts/%s/remoteHostInfo"
host_by_idc = "/api/v2/compute/stats/hostByIdc"

# cluster
cluster = "/api/v2/ob/clusters"
cluster_resource_status = "/api/v2/ob/clusters/stats"
cluster_parameters = "/api/v2/ob/clusters/parameterInfo"

# task
task = "/api/v2/tasks/instances"
