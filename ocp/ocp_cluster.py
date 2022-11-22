# !/usr/bin/env python3
# -*-coding:utf-8 -*-

"""
@time: 2022/6/24
# File       : ocp_cluster.py
# Description：
"""
import requests
from ocp import ocp_api


class ObCluster():
    def __init__(self, url, auth, id=None):
        self.url = url
        self.auth = auth

        self.architecture = ""
        self.compactionStatus = ""
        self.configUrl = ""
        self.createTime = ""
        self.creator = ""
        self.id = id
        self.dataDiskPath = ""
        self.installPath = ""
        self.logDiskPath = ""
        self.minObBuildVersion = ""
        self.name = ""
        self.obClusterId = ""
        self.obVersion = ""
        self.oraclePrivilegeManagementSupported = True
        self.partitionCount = 0
        self.performanceStats = {}
        self.protectionMode = ""
        self.redoTransportMode = ""
        self.redoTransportStatus = ""
        self.regionCount = 0
        self.rootServers = []
        self.serverCount = 0
        self.standbyClusters = []
        self.status = ""
        self.syncStatus = ""
        self.tenantCount = 0
        self.tenants = []
        self.type = ""
        self.updateTime = ""
        self.vpcId = 1
        self.zoneTopo = ""
        self.zones = []
        self.standbyClusterList = []

    def _seri_get(self, data):
        for k, v in data.items():
            setattr(self, k, v)

    def get(self):
        path = ocp_api.cluster + "/%s" % self.id
        response = requests.get(self.url + path, auth=self.auth)
        self._seri_get(response.json()["data"])
        # 备集群
        if len(self.standbyClusters) > 0:
            for info in self.standbyClusters:
                standby_cluster = ObCluster(self.url, self.auth, id=info["id"])
                standby_cluster.get()
                self.standbyClusterList.append(standby_cluster)

    def get_by_name(self, name):
        path = ocp_api.cluster
        response = requests.get(self.url + path, auth=self.auth)
        cluster_info = {}
        for content in response.json()["data"]["contents"]:
            if content["name"] == name[0]:
                cluster_info = content
                break
        if not cluster_info:
            raise Exception("can't find cluster by name:%s" % name)

        if cluster_info:
            self._seri_get(cluster_info)

    def get_cluster_id_by_name(self, name):
        cluster_info = {}
        path = ocp_api.cluster
        response = requests.get(self.url + path, auth=self.auth)
        for content in response.json()["data"]["contents"]:
            if content["name"] == name[0]:
                cluster_info = content
                break
        if not cluster_info:
            raise Exception("can't find cluster by name:%s" % name)

        if cluster_info:
            return cluster_info["id"]
        else:
            raise Exception("can't find clusterId by name:%s" % name)