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

"""Utilities for validating and handling configured remote paths."""

import re
import shlex


# 配置中的部署目录会被多个本地和远程命令复用，因此只接受无需 shell 解释的稳定路径字符。
_CONFIG_PATH_PATTERN = re.compile(r"^(?:/|~/)[\w@%+=:,./-]*$", re.UNICODE)
_SUPPORTED_PATH_DESCRIPTION = "an absolute path, '~', or a path starting with '~/' that contains only letters, numbers, '/', '.', '_', '-', '@', '%', '+', '=', ':', or ','"


def validate_config_path(value, field_name):
    """Validate a configured deployment path and return it unchanged."""
    if not isinstance(value, str):
        raise ValueError("Invalid configuration path for {0}: expected a string".format(field_name))
    if not value:
        raise ValueError("Invalid configuration path for {0}: path must not be empty".format(field_name))
    if value == "~":
        return value
    if not _CONFIG_PATH_PATTERN.fullmatch(value):
        raise ValueError("Invalid configuration path for {0}: expected {1}".format(field_name, _SUPPORTED_PATH_DESCRIPTION))
    return value


def quote_shell_path(path):
    """Quote a validated path for use as one POSIX shell argument."""
    if not isinstance(path, str) or not path:
        raise ValueError("Path must be a non-empty string")
    if path == "~":
        return '"$HOME"'
    if path.startswith("~/"):
        # 不能整体引用波浪号，否则远端 shell 不会展开用户主目录。
        return '"$HOME"' + shlex.quote(path[1:])
    return shlex.quote(path)
