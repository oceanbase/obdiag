#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/22
@file: version_utils.py
@desc:
"""


def compare_versions_greater(v1, v2):
    for i, j in zip(map(int, v1.split(".")), map(int, v2.split("."))):
        if i == j:
            continue
        return i > j
    return len(v1.split(".")) > len(v2.split("."))


def compare_versions_lower(v1, v2):
    for i, j in zip(map(int, v1.split(".")), map(int, v2.split("."))):
        if i == j:
            continue
        return i < j
    return len(v1.split(".")) < len(v2.split("."))
