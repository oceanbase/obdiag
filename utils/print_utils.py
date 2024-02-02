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
@time: 2024/01/23
@file: print_utils.py
@desc:
"""
from colorama import Fore, Style
from utils.utils import is_chinese

def print_scene(scene_dict):
    columns_to_print = ['command', 'info_en', 'info_cn']
    keys = columns_to_print
    table_data = [[value[key] for key in keys] for value in scene_dict.values()]
    column_widths = [max(len(str(item)) * (is_chinese(item) or 1) for item in column) for column in zip(*table_data)]
    table_data.insert(0, keys)
    print_line(length= sum(column_widths) + 5)
    for i in range(len(table_data)):
        print(Fore.GREEN + "   ".join(f"{item:<{width}}" for item, width in zip(table_data[i], column_widths)) + Style.RESET_ALL)
        if i == 0:
            print_line(length= sum(column_widths) + 5)
    print_line(length= sum(column_widths) + 5)

def print_line(char='-', length=50):
    print(char * length)

def print_title(name):
    print("\n[{0}]:".format(name))