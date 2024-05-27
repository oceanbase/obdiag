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
@file: check_dependencies.py
@desc:
"""


def check_client_dependencies():
    install_flag = False
    try:
        import pprint
        import threading
        import tabulate
        import requests
        import json
        import contextlib
        import os
        import tarfile
        import zipfile
        import zstandard
        import base64
        import hashlib
        import argparse
        import paramiko
        import traceback

        install_flag = True
    except Exception as err:
        print("import  error!!!,cause:[{0}]".format(err))
        pass
    finally:
        return install_flag


if __name__ == "__main__":
    if not check_client_dependencies():
        print("\033[1;31m check dependencies failed, you need resolve dependencies \033[0m")
