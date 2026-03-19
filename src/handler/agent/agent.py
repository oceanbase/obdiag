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
@file: agent.py
@desc: Declarative pydantic-ai Agent definition for obdiag.

       The agent is configured once via ``create_agent()`` with all toolsets
       composed via the ``toolsets`` parameter. No wrapper class is needed —
       consumers use the returned ``Agent`` instance directly.
"""

import os
from typing import Any, List, Optional

from pydantic_ai import Agent, FunctionToolset

from src.handler.agent.config import get_model_string
from src.handler.agent.models import AgentConfig, AgentDependencies
from src.handler.agent.toolsets import (
    config_gen_toolset,
    db_toolset,
    file_toolset,
    obdiag_toolset,
)


SYSTEM_PROMPT = """You are obdiag agent, an intelligent diagnostic assistant for OceanBase database.

Your capabilities include:
1. Executing obdiag diagnostic commands (gather logs, analyze, check health, RCA)
2. Analyzing diagnostic results and providing insights
3. Recommending diagnostic steps based on user descriptions
4. Explaining OceanBase concepts and troubleshooting procedures
5. Querying the OceanBase database directly using SQL
6. Generating obdiag configuration files
7. Diagnosing multiple clusters in a single session

Multi-cluster support:
- When no cluster is specified, the default config ~/.obdiag/config.yml is used.
- Every diagnostic tool accepts an optional ``cluster_config_path`` parameter. It can be:
  - A short name (e.g., "obdiag_test") which resolves to ~/.obdiag/obdiag_test.yml
  - A full path (e.g., ~/.obdiag/obdiag_test.yml)
- The user may type "use obdiag_test" or "use <config_path>" to switch the active cluster.
- Use ``show_current_cluster`` to confirm which cluster is currently active.
- When the user asks to diagnose "obdiag_test cluster" or "cluster in obdiag_test.yml",
  pass cluster_config_path="obdiag_test" (or the config path) to the relevant tools.

When users describe problems or ask for diagnostics:
1. First understand what they need and which cluster they mean
2. Use the appropriate diagnostic tools (with cluster_config_path when needed)
3. Analyze the results
4. Provide clear explanations and recommendations

Tool selection for gather operations:
- gather_log: Use when the user wants to collect "日志" (logs), "observer 日志", "组件日志", or logs filtered by trace_id/keywords.
  Pass grep=[trace_id] or grep=[keyword] to filter logs. Use since/from_time/to_time for time range.
- gather_plan_monitor: Use ONLY when the user explicitly wants SQL plan monitor data (执行计划监控) for a trace_id.
  This is for SQL performance analysis, NOT for collecting observer/component logs.

User experience guidelines:
- Prefer action over clarification: When the user asks "这些文件有多大" (how big are these files) or "文件大小" without specifying a path, assume they mean the current working directory. Use file_list with directory_path="." immediately and show sizes, don't ask which directory.
- Respond in the same language as the user's question.
- Keep responses concise. Use bullet points or tables for lists. Avoid long explanatory preambles.
- Format output clearly: use tables for structured data (e.g., check tasks, file lists); use bold/emphasis for key points.
- When a tool fails: explain clearly in plain language, suggest concrete next steps (e.g., "请先配置 ~/.obdiag/config.yml 或运行 obdiag auto deploy 生成集群配置").
- When the user asks about something ambiguous (e.g., "这些" in context), infer from context: if they just listed a directory, "这些" refers to that; if no prior context, assume current directory for file-related questions.

Other guidelines:
- Always confirm before executing potentially long-running operations (SQL, bash, gather/analyze).
- Provide clear, actionable insights from diagnostic results.
- Only execute read-only SQL queries (SELECT, SHOW, DESCRIBE, EXPLAIN)."""


BUILTIN_TOOLSETS: List[FunctionToolset[AgentDependencies]] = [
    obdiag_toolset,
    db_toolset,
    file_toolset,
    config_gen_toolset,
]


def _build_mcp_toolsets(config: AgentConfig, stdio: Any = None) -> list:
    """Build native pydantic-ai MCP toolsets from config.mcp_servers."""
    mcp_toolsets: list = []
    if not config.mcp_enabled or not config.mcp_servers:
        return mcp_toolsets

    try:
        from pydantic_ai.mcp import MCPServerStdio, MCPServerHTTP
    except ImportError:
        if stdio:
            stdio.warn("pydantic-ai MCP support not available; skipping MCP servers")
        return mcp_toolsets

    for name, srv_cfg in config.mcp_servers.items():
        try:
            if "url" in srv_cfg:
                toolset = MCPServerHTTP(
                    url=srv_cfg["url"],
                    headers=srv_cfg.get("headers"),
                )
                mcp_toolsets.append(toolset)
                if stdio:
                    stdio.verbose(f"Registered MCP HTTP server: {name}")
            elif "command" in srv_cfg:
                toolset = MCPServerStdio(
                    command=srv_cfg["command"],
                    args=srv_cfg.get("args", []),
                    env=srv_cfg.get("env"),
                )
                mcp_toolsets.append(toolset)
                if stdio:
                    stdio.verbose(f"Registered MCP stdio server: {name}")
        except Exception as e:
            if stdio:
                stdio.warn(f"Failed to create MCP toolset '{name}': {e}")

    return mcp_toolsets


def create_agent(
    config: AgentConfig,
    stdio: Any = None,
) -> Agent[AgentDependencies, str]:
    """
    Create a fully-configured pydantic-ai Agent.

    The agent is assembled declaratively:
      - Built-in FunctionToolsets (obdiag commands, db, file, config generation)
      - External MCP servers (stdio / HTTP) from config, using pydantic-ai native support

    Args:
        config: AgentConfig loaded from ~/.obdiag/config/agent.yml
        stdio: Optional stdio for logging during setup

    Returns:
        A ready-to-use Agent[AgentDependencies, str]
    """
    model_string = get_model_string(config)
    system_prompt = config.system_prompt or SYSTEM_PROMPT

    if config.base_url:
        os.environ["OPENAI_BASE_URL"] = config.base_url
    if config.api_key:
        os.environ["OPENAI_API_KEY"] = config.api_key

    all_toolsets: list = list(BUILTIN_TOOLSETS)
    all_toolsets.extend(_build_mcp_toolsets(config, stdio))

    agent: Agent[AgentDependencies, str] = Agent(
        model=model_string,
        deps_type=AgentDependencies,
        output_type=str,
        system_prompt=system_prompt,
        toolsets=all_toolsets,
    )

    return agent
