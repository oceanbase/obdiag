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
@time: 2025/07/10
@file: crypto_config_handler.py
@desc:
"""
import os

from src.common.file_crypto.file_crypto import FileEncryptor
from src.common.tool import Util


class CryptoConfigHandler:
    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.options = self.context.options

    def handle(self):
        self.stdio.verbose("CryptoConfigHandler execute")
        file_path = Util.get_option(self.options, "file")
        pd = Util.get_option(self.options, "key")
        file_path = os.path.abspath(file_path)
        if not os.path.exists(file_path):
            self.stdio.error("file {} not exists".format(file_path))
            return
        try:
            fileEncryptor = FileEncryptor(context=self.context)
            fileEncryptor.encrypt_file(file_path, password=pd)
        except Exception as e:
            self.stdio.error("decrypt file failed, error: {}".format(e))
