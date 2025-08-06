#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
import getpass
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
        encrypted_file_path = Util.get_option(self.options, "encrypted_file") or ""
        pd = Util.get_option(self.options, "key") or ""
        if file_path:
            file_path = os.path.abspath(os.path.expanduser(file_path))
        if file_path and not pd and not encrypted_file_path:
            self.stdio.warn("file path is empty or key is empty. need input key")
            key_first = getpass.getpass("please input key: ")
            key_second = getpass.getpass("please input key again: ")
            if key_first != key_second:
                self.stdio.error("key is not same")
                return
            pd = key_first

        if file_path and pd and not encrypted_file_path:
            self.stdio.verbose("encrypt file {} ".format(file_path))
            self.encrypt_file(file_path, pd)
        elif file_path and pd and encrypted_file_path:
            self.stdio.verbose("check encrypt file {} and {}".format(file_path, encrypted_file_path))
            self.check_encrypt_file(file_path, pd, encrypted_file_path)
        elif file_path and encrypted_file_path and not pd:
            self.stdio.warn("file path is empty or key is empty. need input key")
            key_first = getpass.getpass("please input key: ")
            pd = key_first
            self.check_encrypt_file(file_path, pd, encrypted_file_path)
        elif encrypted_file_path and pd and not file_path:
            self.stdio.verbose("decrypt file {} ".format(encrypted_file_path))
            self.decrypt_file(encrypted_file_path, pd)
        elif not file_path and not encrypted_file_path:
            self.stdio.error("file path is empty or encrypted_file_path is empty")

    def encrypt_file(self, file_path, password):
        try:
            fileEncryptor = FileEncryptor(context=self.context)
            fileEncryptor.encrypt_file(file_path, password=password)
        except Exception as e:
            self.stdio.error("decrypt file failed, error: {}".format(e))

    def decrypt_file(self, encrypted_file_path, password):
        try:
            fileEncryptor = FileEncryptor(context=self.context)
            self.stdio.print(str(fileEncryptor.decrypt_file(encrypted_file_path, password=password, save=False).decode('utf-8', errors='ignore')))
        except Exception as e:
            self.stdio.error("decrypt file failed, error: {}".format(e))

    def check_encrypt_file(self, file_path, password, encrypted_file_path):
        try:
            fileEncryptor = FileEncryptor(context=self.context)
            fileEncryptor.check_encrypt_file(file_path, password=password, encrypted_file_path=encrypted_file_path)
        except Exception as e:
            self.stdio.error("decrypt file failed, error: {}".format(e))
