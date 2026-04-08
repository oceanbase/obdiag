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

"""
@time: 2026/03/10
@file: __init__.py
@desc: obdiag agent toolsets — independent FunctionToolset instances that can be
       composed, tested, and reused by any pydantic-ai Agent.
"""

from src.handler.agent.toolsets.config_gen import config_gen_toolset
from src.handler.agent.toolsets.database import db_toolset
from src.handler.agent.toolsets.file_ops import file_toolset
from src.handler.agent.toolsets.knowledge_base import knowledge_toolset
from src.handler.agent.toolsets.obdiag import obdiag_toolset

__all__ = [
    "config_gen_toolset",
    "db_toolset",
    "file_toolset",
    "knowledge_toolset",
    "obdiag_toolset",
]
