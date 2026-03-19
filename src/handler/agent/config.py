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
@file: config.py
@desc: Configuration management for obdiag agent
"""

import json
import os
from typing import Any, Dict, Optional

import yaml

from src.handler.agent.cluster_resolve import OBDIAG_CONFIG_DIR, resolve_cluster_config_path
from src.handler.agent.models import AgentConfig

from src.common.constant import obdiag_path

AGENT_CONFIG_PATH = obdiag_path("config", "agent.yml")
OBDIAG_CONFIG_PATH = obdiag_path("config.yml")


def load_agent_config(config_path: Optional[str] = None, stdio: Any = None) -> Dict[str, Any]:
    """
    Load agent configuration from ~/.obdiag/config/agent.yml

    Args:
        config_path: Optional path to config file (defaults to ~/.obdiag/config/agent.yml)
        stdio: Optional stdio for logging

    Returns:
        Configuration dictionary with default values merged
    """
    default_config = {
        "llm": {
            "provider": "openai",
            "api_type": "openai",
            "api_key": os.getenv("OPENAI_API_KEY", ""),
            "base_url": os.getenv("OPENAI_BASE_URL", ""),
            "model": "gpt-4",
            "temperature": 0.7,
            "max_tokens": 2000,
        },
        "mcp": {
            "enabled": True,
            "servers": {},
        },
        "ui": {
            "show_welcome": True,
            "show_beta_warning": True,
            "clear_screen": True,
            "prompt": "obdiag agent> ",
            "tool_approval": True,
            "stream_output": False,
        },
    }

    path = config_path or AGENT_CONFIG_PATH
    agent_config = {}

    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                agent_config = yaml.safe_load(f) or {}
            if stdio:
                stdio.verbose(f"Loaded agent config from {path}")
        except Exception as e:
            if stdio:
                stdio.warn(f"Failed to load agent config from {path}: {e}")
    else:
        if stdio:
            stdio.verbose(f"Agent config file not found: {path}, using defaults")

    # Merge configurations
    llm_config = {**default_config["llm"], **agent_config.get("llm", {})}
    ui_config = {**default_config["ui"], **agent_config.get("ui", {})}

    # Handle MCP configuration
    mcp_config = {**default_config["mcp"]}
    user_mcp_config = agent_config.get("mcp", {})

    if "enabled" in user_mcp_config:
        mcp_config["enabled"] = user_mcp_config["enabled"]

    # Parse MCP servers - supports JSON string format
    if "servers" in user_mcp_config:
        servers_value = user_mcp_config["servers"]
        if isinstance(servers_value, str) and servers_value.strip():
            try:
                parsed = json.loads(servers_value)
                if parsed:
                    mcp_config["servers"] = parsed
            except json.JSONDecodeError as e:
                if stdio:
                    stdio.warn(f"Failed to parse MCP servers JSON: {e}, using built-in server")
                mcp_config["servers"] = {}
        elif isinstance(servers_value, dict) and servers_value:
            mcp_config["servers"] = servers_value

    return {
        "llm": llm_config,
        "mcp": mcp_config,
        "ui": ui_config,
    }


def get_agent_config(config_path: Optional[str] = None, stdio: Any = None) -> AgentConfig:
    """
    Load and return AgentConfig instance

    Args:
        config_path: Optional path to config file
        stdio: Optional stdio for logging

    Returns:
        AgentConfig instance
    """
    config_dict = load_agent_config(config_path, stdio)
    return AgentConfig.from_dict(config_dict)


# Backward compatibility alias
load_ai_config = load_agent_config


def get_model_string(config: AgentConfig) -> str:
    """
    Get the model string for Pydantic-AI based on provider and model name

    Args:
        config: AgentConfig instance

    Returns:
        Model string in format "provider:model" or custom base_url format
    """
    provider = config.provider.lower()
    model = config.model

    # Map provider names to pydantic-ai model prefixes
    provider_map = {
        "openai": "openai",
        "anthropic": "anthropic",
        "gemini": "gemini",
        "google": "gemini",
        "deepseek": "openai",  # DeepSeek uses OpenAI-compatible API
    }

    prefix = provider_map.get(provider, "openai")
    return f"{prefix}:{model}"
