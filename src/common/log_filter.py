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

"""本地与节点日志筛选共用的命令构造逻辑。"""

import shlex

from src.common.path_utils import quote_shell_path


def build_grep_command(source_path, target_path, patterns, append=False):
    # 动态参数分别引用，避免被 shell 解释；逐项筛选保留正则语义及多个条件的交集关系。
    commands = ["cat -- {0}".format(quote_shell_path(source_path))]
    commands.extend("grep -e {0}".format(shlex.quote(pattern)) for pattern in patterns)
    return "{0} {1} {2}".format(" | ".join(commands), ">>" if append else ">", quote_shell_path(target_path))
