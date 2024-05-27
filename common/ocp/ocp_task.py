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
# File       : ocp_task.py
# Description：
"""
import logging
import time
import requests

from common.ocp import ocp_api

logger = logging.getLogger("run")


class Task:
    """task状态见constant. subtask 顺序和接口返回顺序无关,要看seriesId"""

    STATUS_TASK_RUNNING = "RUNNING"
    STATUS_TASK_PENDING = "PENDING"
    STATUS_TASK_SUCCESS = "SUCCESSFUL"
    STATUS_TASK_FAILED = "FAILED"
    TASK_RETRY_PERMIT = {"Task retry": 3}

    def __init__(self, url, auth, task_id):
        self.url = url
        self.auth = auth
        self.id = task_id
        self.cluster = {}
        self.createTime = ""
        self.creator = {}
        self.executor = ""
        self.name = ""
        self.operation = ""
        self.prohibitRollback = ""
        self.startTime = ""
        self.status = ""
        self.subtasks = []
        self.taskDefinitionId = -1
        self.type = ""

        self.get()
        # 没有配置默认不重试
        self._retry_permit_time = Task.TASK_RETRY_PERMIT.get(self.name, 0)
        self._retry_times = 0

    def _seri_get(self, data):
        for k, v in data.items():
            setattr(self, k, v)

    def retry(self):
        path = ocp_api.task + "/%s/retry" % self.id
        response = requests.post(self.url + path, auth=self.auth)

    def get(self):
        path = ocp_api.task + "/%s" % self.id
        response = requests.get(self.url + path, auth=self.auth)
        self._seri_get(response.json()["data"])

    def wait_done(self, interval=20):
        """阻塞等待直到task出结果"""
        while True:
            try:
                self.get()
            except Exception as e:
                logger.warning("task(%s:%s) get statsu failed, error:%s" % (self.id, self.name, e))
                break
            if self.status == Task.STATUS_TASK_SUCCESS:
                logger.info("task(%s:%s) success" % (self.id, self.name))
                break
            elif self.status == Task.STATUS_TASK_FAILED:
                if self._retry_times >= self._retry_permit_time:
                    raise Exception("task failed! name:%s id:%s" % (self.name, self.id))
                else:
                    self.retry()
                    self._retry_times += 1
                    logger.warning('task %s failed,but allowed retry %s times, now retry %s time' % (self.name, self._retry_permit_time, self._retry_times))
                    time.sleep(interval)
                    continue
            else:
                logger.info("task(%s:%s) not finished yet, now status %s, waiting" % (self.id, self.name, self.status))
                time.sleep(interval)
        return self.status
