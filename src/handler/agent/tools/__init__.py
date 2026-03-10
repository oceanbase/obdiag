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
@time: 2026/03/09
@file: __init__.py
@desc: obdiag agent tools module

Lazy imports to avoid import errors when pydantic_ai is not installed.
"""

__all__ = [
    'register_database_tools',
    'register_file_tools',
    'register_obdiag_tools',
]


def __getattr__(name):
    """Lazy import for tool registration functions"""
    if name == 'register_database_tools':
        from src.handler.agent.tools.database import register_database_tools
        return register_database_tools
    elif name == 'register_file_tools':
        from src.handler.agent.tools.file_ops import register_file_tools
        return register_file_tools
    elif name == 'register_obdiag_tools':
        from src.handler.agent.tools.obdiag_commands import register_obdiag_tools
        return register_obdiag_tools
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
