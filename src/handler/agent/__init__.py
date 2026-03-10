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
@desc: obdiag agent module initialization

Lazy imports to avoid hard failures when pydantic-ai is not installed.
"""

__all__ = [
    # Handler
    'AiAgentHandler',
    'AiAssistantHandler',
    # Agent factory
    'create_agent',
    # Models
    'AgentConfig',
    'AgentDependencies',
    # Config
    'load_agent_config',
    'load_ai_config',
    'get_agent_config',
]


def __getattr__(name):
    if name in ('AiAgentHandler', 'AiAssistantHandler'):
        from src.handler.agent.handler import AiAgentHandler, AiAssistantHandler
        return AiAgentHandler if name == 'AiAgentHandler' else AiAssistantHandler
    elif name == 'create_agent':
        from src.handler.agent.agent import create_agent
        return create_agent
    elif name in ('AgentConfig', 'AgentDependencies'):
        from src.handler.agent import models as models_module
        return getattr(models_module, name)
    elif name in ('load_agent_config', 'load_ai_config', 'get_agent_config'):
        from src.handler.agent import config as config_module
        return getattr(config_module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
