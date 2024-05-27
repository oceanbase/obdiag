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
@time: 2024/2/1
@file: check_list.py
@desc:
"""
import os

import yaml

from common.tool import Util


class CheckListHandler:
    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.work_path = os.path.expanduser(self.context.inner_config["check"]["work_path"] or "~/.obdiag/check")

    def handle(self):
        self.stdio.verbose("list check cases")
        entries = os.listdir(self.work_path)
        files = [f for f in entries if os.path.isfile(os.path.join(self.work_path, f))]
        for file in files:
            if "check_package" in file:
                cases_map = {"all": {"name": "all", "command": "obdiag check", "info_en": "default check all task without filter", "info_cn": "默认执行除filter组里的所有巡检项"}}
                # Obtain which files match and corresponding header files
                # Using string segmentation methods
                parts = file.split('_')
                if len(parts) < 1:
                    self.stdio.warn("invalid check package name :{0} , Please don't add file, which 'check_package' in the name".format(file))
                    continue
                target = parts[0]
                file = "{0}/{1}".format(self.work_path, file)
                package_file_data = None
                # read yaml file
                with open(file, 'r') as f:
                    package_file_data = yaml.safe_load(f)
                    if not package_file_data or len(package_file_data) == 0:
                        self.stdio.warn("No data check package data :{0} ".format(file))
                        continue
                    for package_data in package_file_data:
                        if package_data == "filter":
                            continue
                        package_target = target
                        if target == "observer":
                            package_target = "cases"
                        else:
                            package_target = "{0}_cases".format(target)

                        cases_map[package_data] = {
                            "name": package_data,
                            "command": "obdiag check --{0}={1}".format(package_target, package_data),
                            "info_en": package_file_data[package_data].get("info_en") or "",
                            "info_cn": package_file_data[package_data].get("info_cn") or "",
                        }
                Util.print_title("check cases about {0}".format(target))
                Util.print_scene(cases_map)
