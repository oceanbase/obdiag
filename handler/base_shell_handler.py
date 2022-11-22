#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/21
@file: base_handler.py
@desc:
"""


# 所有需要连接到各个节点执行ssh命令的继承自该handler，通过 paramiko 到目标机器上执行shell命令来来得到结果
class BaseShellHandler(object):
    def __init__(self, nodes):
        self.nodes = nodes
