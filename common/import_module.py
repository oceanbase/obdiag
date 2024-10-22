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


def import_modules(module_file_dir, stdio):
    stdio.verbose("import_modules input: module_file_dir->{0}".format(module_file_dir))
    try:
        module_files = []
        module_list = {}
        for root, dirs, files in os.walk(module_file_dir):
            if root == module_file_dir:
                module_files = files
        for module_file in module_files:
            module_name = os.path.basename(module_file)[:-3]
            DynamicLoading.add_lib_path(module_file_dir)
            module = DynamicLoading.import_module(os.path.basename(module_file)[:-3], None)
            if not hasattr(module, module_name):
                stdio.error("{0} import_module failed".format(module_name))
                continue
            module_list[module_name] = getattr(module, module_name)
        return module_list
    except Exception as e:
        stdio.error("import_modules failed: {0}".format(e))
        raise ImportModulesException("import_modules failed: {0}".format(e))
