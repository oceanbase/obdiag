#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/11/29
@file: base_sql_handler.py
@desc:
"""

from common.ob_connector import OBConnector


# 通过sql连接到集群中去获取sql性能数据的都继承自该handler
class BaseSQLHandler(object):
    def __init__(self):
        pass
