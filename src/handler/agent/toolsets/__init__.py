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
@time: 2026/04/09
@file: __init__.py
@desc: obdiag agent toolsets — factory functions that return plain callables for Deep Agents SDK.
"""

from src.handler.agent.toolsets.config_gen import create_config_gen_tools
from src.handler.agent.toolsets.database import create_db_tools
from src.handler.agent.toolsets.knowledge_base import create_knowledge_tools
from src.handler.agent.toolsets.obdiag import create_obdiag_tools

__all__ = [
    "create_obdiag_tools",
    "create_db_tools",
    "create_config_gen_tools",
    "create_knowledge_tools",
]
