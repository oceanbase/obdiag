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
@time: 2024/09/19
@file: import_modules.py
@desc:
"""
import os

from common.tool import DynamicLoading


class ImportModulesException(Exception):
    pass


# 实现模块导入，要求module_name为模块名和需要导入的对象名，module_file_path为模块文件路径


def import_modules(module_file_path, module_name, stdio):
    try:
        module_file_path = os.path.abspath(os.path.expanduser(module_file_path))
        # check module_name
        if module_file_path.endswith(".py"):
            if module_name == "":
                module_name = module_file_path[:-3]
        else:
            stdio.error("module file path should end with .py. Please check module_file_path: {0}".format(module_file_path))
            raise ImportModulesException("module_file_path should end with .py. Please check module_file_path: {0}".format(module_file_path))
        lib_path = os.path.dirname(module_file_path)
        DynamicLoading.add_lib_path(lib_path)
        module = DynamicLoading.import_module(module_name, None)
        if not hasattr(module, module_name):
            stdio.error("{0} import_module failed".format(module_name))
            raise ImportModulesException("{0} import_module failed".format(module_name))
        return getattr(module, module_name)
    except Exception as e:
        stdio.error("import_modules failed: {0}".format(e))
        raise ImportModulesException("import_modules failed: {0}".format(e))
