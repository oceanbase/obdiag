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
@time: 2023/1/17
@file: yaml_utils.py
@desc:
"""

import oyaml as yaml
import os


def is_yaml_file(path):
    if not os.path.isfile(path):
        return False
    if path.endswith(('.yaml', '.yml')):
        return True
    else:
        return False

def read_yaml_data(file_path):
    if is_yaml_file(file_path):
        try:
            with open(file_path, 'r') as f:
                data = yaml.load(f, Loader=yaml.FullLoader)
            return data
        except yaml.YAMLError as exc:
            raise Exception("Error loading YAML from file, error: {0}".format(exc))


def write_yaml_data(data, file_path):
    with open(file_path, 'w') as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def write_yaml_data_append(data, file_path):
    with open(file_path, 'a+') as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


def write_yaml_data_sorted(data, file_path):
    with open(file_path, 'w') as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=True)


def write_yaml_data_append_sorted(data, file_path):
    with open(file_path, 'a+') as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=True)
