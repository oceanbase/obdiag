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
from datetime import date
from typing import Any, List, Optional

from pydantic_ai import Agent, FunctionToolset, RunContext
from pydantic_ai.models import infer_model, parse_model_id

from src.handler.agent.config import DEFAULT_SKILLS_DIRECTORY, get_model_string
from src.handler.agent.models import AgentConfig, AgentDependencies
from src.handler.agent.toolsets import (
    config_gen_toolset,
    db_toolset,
    file_toolset,
    obdiag_toolset,
)
from src.handler.agent.toolsets.knowledge_base import knowledge_toolset


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
- Default cluster config is ~/.obdiag/config.yml (obdiag standard). Other configs are *.yml/*.yaml in the same directory.
- When the user asks which clusters exist, 有哪些集群, or to list configs, call ``list_obdiag_clusters`` first (then ``show_current_cluster`` if needed).
- Every diagnostic tool accepts an optional ``cluster_config_path`` parameter. It can be:
  - A short name (e.g., "obdiag_test") which resolves to ~/.obdiag/obdiag_test.yml
  - A full path (e.g., ~/.obdiag/obdiag_test.yml)
- The user may type ``/use obdiag_test`` or ``/use <config_path>`` in the agent REPL to switch the active cluster.
- Use ``show_current_cluster`` for the active session cluster; use ``list_obdiag_clusters`` for all discovered config files.
- When the user asks to diagnose "obdiag_test cluster" or "cluster in obdiag_test.yml",
  pass cluster_config_path="obdiag_test" (or the config path) to the relevant tools.

When users describe problems or ask for diagnostics:
1. First understand what they need and which cluster they mean
2. Use the appropriate diagnostic tools (with cluster_config_path when needed)
3. Analyze the results
4. Provide clear explanations and recommendations

Tool selection for gather operations:
- gather_log: Observer-side logs only (``obdiag gather log``): observer / election / rootservice, or generic 日志 when OBProxy and OMS are not mentioned.
- gather_obproxy_log: OBProxy / obproxy / 代理 日志 — never use gather_log for these.
- gather_oms_log: **OMS** 日志（Ghana、CM、supervisor、CDC/libobcdc、store 等，``obdiag gather oms_log``）— requires ``oms`` in obdiag config.yml; CDC 类 scope 常需 ``oms_component_id``. Never use ``gather_log`` or ``analyze_log`` for OMS log content.
- gather_ash: ASH report / 活跃会话历史 / ``gather ash``.
- gather_awr: AWR / ParalleSQL gather awr.
- gather_plan_monitor: ONLY for SQL plan monitor (执行计划监控) for a trace_id — not for ordinary log collection.
- gather_log details: Pass grep=[trace_id] or grep=[keyword]; use since/from_time/to_time for time range.
- Only a subset of ``obdiag gather`` subcommands are exposed as tools; for others the user should run ``obdiag gather <subcmd>`` in a shell or extend the toolset.

After a gather completes, if the user asks to **analyze / 分析 / 解读** logs (including OBProxy ``obproxy_diagnosis`` etc.):
- Prefer **file_list** (with a glob **pattern** if needed) and **file_read** under the **existing** gather output directory (e.g. ``obdiag_gather_pack_*``) to locate and read relevant diagnosis / log files, then summarize in natural language.
- Do **not** call ``gather_obproxy_log`` again unless the user explicitly wants a **new** collection (different time range, re-pull from nodes, or narrower scope because the previous pack is missing).
- ``analyze_log`` is **only** for OceanBase **cluster observer-node log analysis** (``obdiag analyze log`` scopes: observer, election, rootservice, all — all cluster-side, not OBProxy/**OMS**). For OBProxy use file tools after ``gather_obproxy_log``; for **OMS** use file tools after ``gather_oms_log`` — not ``analyze_log``.

**Multi-step tool runs (important):** One user turn can include **multiple** tool calls before the final text reply. If the user asked for **采集并分析**, **收集后分析**, or **分析** OBProxy or **OMS** logs in the **same** message (or right after a gather in context), **continue in the same run** after ``gather_obproxy_log`` or ``gather_oms_log`` succeeds: run **file_list** on the pack path from stdout; if you see **``.tar.gz`` / ``.zip``**, use **``run_shell``** to list/unpack (e.g. ``tar -xzf``) with user approval, then **file_read** on extracted logs, **then** answer. Do **not** end with only a conversational wrap-up after the gather tool when analysis was part of the request. Fulfilling the full request outweighs keeping the tool count minimal.

User experience guidelines:
- Prefer action over clarification: When the user asks "这些文件有多大" (how big are these files) or "文件大小" without specifying a path, assume they mean the current working directory. Use file_list with directory_path="." immediately and show sizes, don't ask which directory.
- Respond in the same language as the user's question.
- **You define the human-facing display:** The client may print lines like ``摘要(use tool_name) | args`` (Chinese summary + literal tool id). Still include your own **short sentence** in the user's language about what you are doing and why; do not assume the trace line alone is enough. Tables, headings, and emphasis should follow the user's language.
- Keep responses concise. Use bullet points or tables for lists. Avoid long explanatory preambles.
- Format output clearly: use tables for structured data (e.g., check tasks, file lists); use bold/emphasis for key points.
- When a tool fails: explain clearly in plain language, suggest concrete next steps (e.g., "请先配置 ~/.obdiag/config.yml 或运行 obdiag auto deploy 生成集群配置").
- When the user asks about something ambiguous (e.g., "这些" in context), infer from context: if they just listed a directory, "这些" refers to that; if no prior context, assume current directory for file-related questions.

Other guidelines:
- Always confirm before executing potentially long-running operations (SQL, bash, gather/analyze).
- Provide clear, actionable insights from diagnostic results.
- Only execute read-only SQL queries (SELECT, SHOW, DESCRIBE, EXPLAIN)."""

# Appended to SYSTEM_PROMPT when oceanbase_knowledge.enabled is true (see create_agent).
KNOWLEDGE_SYSTEM_SUFFIX_ENABLED = """
8. Querying the official OceanBase knowledge base via ``query_oceanbase_knowledge_base`` (gateway ``POST /retrieval`` with ``query``, ``component``, ``version``). Default ``component`` is ``oceanbase`` (general docs); use ``obdiag`` when the user asks specifically for obdiag-tool documentation. With ``oceanbase_knowledge.bearer_token`` set in ``agent.yml`` it calls the gateway; without token it returns setup instructions — still call the tool when the user wants official docs. Put Observer/OBProxy (and related) versions in the ``version`` argument or in ``query`` when known. **If the user explicitly asks to query the knowledge base** (e.g. 查知识库、查询知识库、官方文档怎么说、知识库里有没有), **you MUST call** ``query_oceanbase_knowledge_base`` **at least once in that turn before answering**, with a focused ``query`` (and optional ``context_text`` for long snippets). Do not skip the tool because you judge the topic "non-standard" or "comparison" — let the gateway return what docs cover, then summarize honestly (including gaps).
"""

# Appended when oceanbase_knowledge.enabled is false (tool not registered).
KNOWLEDGE_SYSTEM_SUFFIX_DISABLED = """
8. Official OceanBase knowledge base integration is **disabled** in this deployment (``oceanbase_knowledge.enabled: false`` in ``agent.yml``). The knowledge-base retrieval tool is **not** registered — do not attempt to call it. For documentation-style questions, answer from general knowledge and direct users to the official OceanBase documentation; say that the in-CLI knowledge gateway is not enabled when relevant.
"""


def _credential_provider_factory(config: AgentConfig):
    """Inject ``api_key`` / ``base_url`` / ``default_headers`` from agent.yml into OpenAI-compatible providers.

    Avoids mutating ``os.environ`` (which breaks multi-config or tests in one process).
    Other providers still use pydantic-ai ``infer_provider`` (typically env vars).
    """

    api_key = (config.api_key or "").strip() or None
    base_url = (config.base_url or "").strip() or None
    default_headers = config.default_headers or None

    def factory(provider_name: str):
        from pydantic_ai.providers import infer_provider
        from pydantic_ai.providers.openai import OpenAIProvider

        if provider_name in ("openai", "openai-chat", "openai-responses"):
            if default_headers:
                from openai import AsyncOpenAI

                openai_client = AsyncOpenAI(api_key=api_key, base_url=base_url, default_headers=default_headers)
                return OpenAIProvider(openai_client=openai_client)
            return OpenAIProvider(api_key=api_key, base_url=base_url)
        return infer_provider(provider_name)

    return factory


def _resolve_pydantic_ai_model(config: AgentConfig):
    """Build a concrete Model for OpenAI-compatible providers; else keep model id string."""
    model_string = get_model_string(config)
    provider_name, _ = parse_model_id(model_string)
    if provider_name in ("openai", "openai-chat", "openai-responses"):
        return infer_model(model_string, provider_factory=_credential_provider_factory(config))
    return model_string


BUILTIN_TOOLSETS: List[FunctionToolset[AgentDependencies]] = [
    obdiag_toolset,
    db_toolset,
    file_toolset,
    config_gen_toolset,
]


def _build_skills_toolset(config: AgentConfig, stdio: Any = None):
    """Build SkillsToolset from pydantic-ai-skills when skills are enabled."""
    if not config.skills_enabled:
        return None

    try:
        from pydantic_ai_skills import SkillsToolset
    except ImportError:
        if stdio:
            stdio.warn("pydantic-ai-skills not available; skills disabled")
        return None

    skills_dir = config.skills_directory or DEFAULT_SKILLS_DIRECTORY
    if not os.path.isdir(skills_dir):
        if stdio:
            stdio.verbose(f"Skills directory not found: {skills_dir}, skills disabled")
        return None

    try:
        exclude: list[str] = []
        if not getattr(config, "skills_run_script_tool", False):
            # Bundled skills are SKILL.md-only; run_skill_script breaks on providers that send args as JSON string.
            exclude.append("run_skill_script")
        toolset = SkillsToolset(
            directories=[skills_dir],
            validate=config.skills_validate,
            exclude_tools=exclude or None,
        )
        if stdio:
            stdio.verbose(f"Loaded skills from {skills_dir}")
        return toolset
    except Exception as e:
        if stdio:
            stdio.warn(f"Failed to create SkillsToolset: {e}")
        return None


def _skills_catalog_for_system_prompt(skills_toolset: Any) -> Optional[str]:
    """Build skills overview for the static system prompt.

    SkillsToolset.get_instructions() is normally injected via ModelRequest.instructions; pydantic-ai's
    OpenAI-compatible mapper inserts that as an extra system message after mapped parts. Combined
    with strict providers (e.g. Qwen) that require system before any user content, that ordering
    can trigger errors. Embedding the catalog in system_prompt keeps one primary system block.
    """
    skills = getattr(skills_toolset, "skills", None) or {}
    if not skills:
        return None
    lines: List[str] = []
    for skill in sorted(skills.values(), key=lambda s: s.name):
        lines.append("<skill>")
        lines.append(f"<name>{skill.name}</name>")
        lines.append(f"<description>{skill.description}</description>")
        if getattr(skill, "uri", None):
            lines.append(f"<uri>{skill.uri}</uri>")
        lines.append("</skill>")
    skills_list = "\n".join(lines)
    template = getattr(skills_toolset, "_instruction_template", None)
    if template:
        return template.format(skills_list=skills_list)
    try:
        from pydantic_ai_skills.toolset import _INSTRUCTION_SKILLS_HEADER

        return _INSTRUCTION_SKILLS_HEADER.format(skills_list=skills_list)
    except ImportError:
        return "You have access to skills. Use load_skill and related tools as needed.\n\n" "<available_skills>\n" f"{skills_list}\n" "</available_skills>"


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

    OpenAI-compatible model ids (``openai`` / ``deepseek`` mapping, etc.) use
    ``OpenAIProvider(api_key=..., base_url=...)`` so credentials from ``agent.yml``
    need not be copied into ``os.environ``.

    Args:
        config: AgentConfig loaded from ~/.obdiag/config/agent.yml
        stdio: Optional stdio for logging during setup

    Returns:
        A ready-to-use Agent[AgentDependencies, str]
    """
    agent_model = _resolve_pydantic_ai_model(config)
    if config.system_prompt:
        system_prompt = config.system_prompt
    else:
        system_prompt = SYSTEM_PROMPT.rstrip()
        if config.oceanbase_knowledge_enabled:
            system_prompt = f"{system_prompt}{KNOWLEDGE_SYSTEM_SUFFIX_ENABLED}"
        else:
            system_prompt = f"{system_prompt}{KNOWLEDGE_SYSTEM_SUFFIX_DISABLED}"

    skills_toolset = _build_skills_toolset(config, stdio)
    skills_catalog = _skills_catalog_for_system_prompt(skills_toolset) if skills_toolset else None
    if skills_catalog:
        system_prompt = f"{system_prompt.rstrip()}\n\n{skills_catalog}"

    # Wall-clock date is not in model weights or KB snapshot; inject so “latest” / 今年 align with the user’s session.
    _today = date.today().isoformat()
    system_prompt = (
        f"{system_prompt.rstrip()}\n\n"
        f"Session calendar date (this machine): {_today}. "
        "When answering what is “latest” or “current” (releases, docs), use this date. "
        "If knowledge-base text or training data only covers through an earlier year, say so and suggest verifying on the official OceanBase site rather than implying old material is still the newest."
    )

    all_toolsets: list = list(BUILTIN_TOOLSETS)

    if config.oceanbase_knowledge_enabled:
        all_toolsets.append(knowledge_toolset)

    if skills_toolset:
        all_toolsets.append(skills_toolset)

    all_toolsets.extend(_build_mcp_toolsets(config, stdio))

    agent: Agent[AgentDependencies, str] = Agent(
        model=agent_model,
        deps_type=AgentDependencies,
        output_type=str,
        system_prompt=system_prompt,
        toolsets=all_toolsets,
    )

    @agent.instructions
    async def _strict_provider_system_order(_ctx: RunContext[AgentDependencies]) -> str:
        """Minimal non-empty instructions for OpenAIChatModel._map_messages.

        When ``message_history`` is truncated so the mapped list would start with ``role=user``,
        pydantic-ai prepends this as system at index 0 only if it is truthy. Some gateways
        (Qwen/DashScope) reject the first message not being system.
        """
        return " "

    return agent
