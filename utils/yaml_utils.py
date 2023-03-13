#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2023/1/17
@file: yaml_utils.py
@desc:
"""

import oyaml as yaml


def read_yaml_data(file_path):
    with open(file_path, 'r') as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
    return data


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
