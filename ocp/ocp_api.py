# !/usr/bin/env python3
# -*-coding:utf-8 -*-

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

