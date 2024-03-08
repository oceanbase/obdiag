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
@time: 2023/12/26
@file: version_utils.py
@desc:
"""
import socket

import requests

from common.constant import const


def network_connectivity(url=""):
    try:
        socket.setdefaulttimeout(3)
        response = requests.get(url, timeout=(3))
        if response.status_code is not None:
            return True
        else:
            return False
    except Exception as e:
        return False


def download_file(url, local_filename):
    # 发送HTTP GET请求并获取响应内容
    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # 如果响应状态码不是200，则抛出异常
        # 打开本地文件并写入数据
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)  # 将内容写入文件
    return local_filename