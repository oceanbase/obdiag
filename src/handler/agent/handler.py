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
@file: handler.py
@desc: obdiag agent handler — deepagents-cli Textual TUI + obdiag tools.

Interactive mode  : create_cli_agent() → run_textual_app()  (full Textual TUI)
Single-shot (-m)  : create_cli_agent() → _run_agent_loop()  (async streaming, no TUI)

LLM configuration is read from [llm] in ~/.obdiag/config/agent.yml:
  llm:
    base_url: https://...
    api_key:  sk-...
    model:    Qwen3-235B-A22B
"""

import asyncio
import json
import logging
import os
import yaml
from typing import Any, Dict, Optional

from src.common.constant import expand_obdiag_path, obdiag_path
from src.common.result_type import ObdiagResult
from src.common.tool import Util
from src.handler.agent.models import AgentConfig, AgentDependencies, read_obcluster_config
from src.handler.agent.toolsets import (
    create_obdiag_tools,
    create_db_tools,
    create_config_gen_tools,
    create_knowledge_tools,
)

# ---------------------------------------------------------------------------
# Config constants (formerly config.py)
# ---------------------------------------------------------------------------

AGENT_CONFIG_PATH = obdiag_path("config", "agent.yml")
DEFAULT_SKILLS_DIRECTORY = obdiag_path("agent", "skills")
OBDIAG_CONFIG_PATH = obdiag_path("config.yml")


def load_agent_config(config_path: Optional[str] = None, stdio: Any = None) -> Dict[str, Any]:
    """
    Load agent configuration from ~/.obdiag/config/agent.yml.

    Parses [skills], [ui], [mcp], [oceanbase_knowledge], and [llm] sections.
    The [llm] section (base_url, api_key, model) is returned as-is for callers
    that need the model name (e.g. _get_default_model_spec).
    """
    default_config = {
        "mcp": {
            "enabled": True,
            "servers": {},
        },
        "skills": {
            "enabled": True,
            "directory": "",
            "validate": True,
            "script_timeout": 60,
            "run_script_tool": False,
        },
        "ui": {
            "show_welcome": True,
            "show_beta_warning": True,
            "clear_screen": True,
            "prompt": "obdiag agent> ",
            "tool_approval": True,
            "stream_output": False,
            "show_usage_after_turn": False,
            "show_tool_trace": True,
            "auto_compact": True,
            "context_window_tokens": None,
            "auto_compact_threshold_ratio": 0.85,
            "auto_compact_min_messages": 2,
            "show_usage_cost": False,
        },
        "oceanbase_knowledge": {
            "enabled": False,
            "bearer_token": "",
        },
    }

    path = config_path or AGENT_CONFIG_PATH
    agent_config = {}

    if os.path.exists(path):
        try:
            with open(path, encoding='utf-8') as f:
                agent_config = yaml.safe_load(f) or {}
            if stdio:
                stdio.verbose(f"Loaded agent config from {path}")
        except yaml.YAMLError as e:
            if stdio:
                stdio.warn(f"Failed to load agent config from {path}: {e}")
        except Exception as e:
            if stdio:
                stdio.warn(f"Failed to load agent config from {path}: {e}")
    else:
        if stdio:
            stdio.verbose(f"Agent config file not found: {path}, using defaults")

    ui_config = {**default_config["ui"], **agent_config.get("ui", {})}

    mcp_config = {**default_config["mcp"]}
    user_mcp_config = agent_config.get("mcp", {})
    if "enabled" in user_mcp_config:
        mcp_config["enabled"] = user_mcp_config["enabled"]
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

    skills_config = {**default_config["skills"], **agent_config.get("skills", {})}
    dir_path = (skills_config.get("directory") or "").strip()
    skills_config["directory"] = expand_obdiag_path(dir_path) if dir_path else DEFAULT_SKILLS_DIRECTORY

    ok_user = agent_config.get("oceanbase_knowledge") or {}
    if not isinstance(ok_user, dict):
        ok_user = {}
    oceanbase_knowledge_config = {**default_config["oceanbase_knowledge"], **ok_user}

    llm_config = agent_config.get("llm") or {}
    if not isinstance(llm_config, dict):
        llm_config = {}

    return {
        "mcp": mcp_config,
        "skills": skills_config,
        "ui": ui_config,
        "oceanbase_knowledge": oceanbase_knowledge_config,
        "llm": llm_config,
    }


def _inject_llm_env(config_path: Optional[str] = None, stdio: Any = None) -> None:
    """Read [llm] from agent.yml and inject credentials/config into os.environ.

    Injects:
      OPENAI_API_KEY   — from llm.api_key
      OPENAI_BASE_URL  — from llm.base_url
      _OBDIAG_LLM_HEADERS — from llm.headers (JSON, consumed by _get_provider_kwargs)

    Only sets each variable when the config value is non-empty and the env var is
    not already set.
    """
    path = config_path or AGENT_CONFIG_PATH
    if not os.path.exists(path):
        return
    try:
        with open(path, encoding='utf-8') as f:
            raw = yaml.safe_load(f) or {}
    except Exception:
        return

    llm = raw.get("llm") or {}
    if not isinstance(llm, dict):
        return

    api_key = (llm.get("api_key") or "").strip()
    if api_key and not os.environ.get("OPENAI_API_KEY"):
        os.environ["OPENAI_API_KEY"] = api_key
        if stdio:
            stdio.verbose("[agent] injected OPENAI_API_KEY from agent.yml [llm]")

    base_url = (llm.get("base_url") or "").strip()
    if base_url and not os.environ.get("OPENAI_BASE_URL"):
        os.environ["OPENAI_BASE_URL"] = base_url
        if stdio:
            stdio.verbose("[agent] injected OPENAI_BASE_URL from agent.yml [llm]")

    headers = llm.get("headers")
    if isinstance(headers, dict) and headers and not os.environ.get("_OBDIAG_LLM_HEADERS"):
        os.environ["_OBDIAG_LLM_HEADERS"] = json.dumps(headers)
        if stdio:
            stdio.verbose(f"[agent] injected {len(headers)} custom header(s) from agent.yml [llm]")


_LOG = logging.getLogger(__name__)

# Agent assistant_id — used as session namespace in deepagents-cli TUI
_ASSISTANT_ID = "obdiag"

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
- Default cluster config is ~/.obdiag/config.yml. Other configs are *.yml/*.yaml in the same directory.
- When the user asks which clusters exist, call list_obdiag_clusters first.
- Every diagnostic tool accepts an optional cluster_config_path parameter (short name or full path).

Tool selection for gather operations:
- gather_log: Observer-side logs only (observer/election/rootservice).
- gather_obproxy_log: OBProxy/obproxy/代理日志 — never use gather_log for these.
- gather_oms_log: OMS logs — requires oms in config.yml.
- gather_ash: ASH report / 活跃会话历史.
- gather_awr: AWR / ParallelSQL gather awr.
- gather_plan_monitor: ONLY for SQL plan monitor (执行计划监控) for a trace_id.

After a gather completes, if the user asks to analyze logs:
- Use ls and read_file on the gather output directory to locate and read relevant files, then summarize.
- analyze_log is ONLY for OceanBase cluster observer-node log analysis.

User experience guidelines:
- Respond in the same language as the user's question.
- Keep responses concise. Use bullet points or tables for lists.
- When a tool fails, suggest concrete next steps.
- Only execute read-only SQL queries (SELECT, SHOW, DESCRIBE, EXPLAIN).
- Always confirm before executing potentially long-running operations."""


class AiAgentHandler:
    """obdiag agent handler — deepagents-cli Textual TUI implementation."""

    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.options = context.options
        self._deps: Optional[AgentDependencies] = None
        self._config_path_ref: Dict[str, str] = {"v": ""}

    def handle(self) -> ObdiagResult:
        """Main entry point called by ObdiagHome."""
        config_dict = load_agent_config(stdio=self.stdio)
        config = AgentConfig.from_dict(config_dict)

        # Inject OPENAI_API_KEY / OPENAI_BASE_URL from [llm] so deepagents-cli
        # and the OpenAI SDK pick them up without requiring a manual `export`.
        _inject_llm_env(stdio=self.stdio)

        # Build obdiag toolset (closure injection pattern)
        cluster_config = read_obcluster_config(OBDIAG_CONFIG_PATH)
        self._deps = AgentDependencies(
            cluster_config=cluster_config,
            stdio=self.stdio,
            config_path=OBDIAG_CONFIG_PATH,
            oceanbase_knowledge_bearer_token=config.oceanbase_knowledge_bearer_token,
        )
        self._config_path_ref["v"] = self._deps.config_path

        def config_path_getter() -> str:
            return self._config_path_ref["v"]

        def deps_getter() -> AgentDependencies:
            return self._deps

        obdiag_tools = (
            create_obdiag_tools(config_path_getter, self.stdio) + create_db_tools(deps_getter) + create_config_gen_tools(self.stdio) + (create_knowledge_tools(lambda: config.oceanbase_knowledge_bearer_token) if config.oceanbase_knowledge_enabled else [])
        )

        msg = Util.get_option(self.options, "m")

        try:
            if msg:
                return self._run_single_shot(msg, obdiag_tools, config)
            return self._run_interactive(obdiag_tools, config)
        finally:
            if self._deps:
                self._deps.close()

    # ------------------------------------------------------------------
    # Interactive mode — full Textual TUI via vendored tui/
    # ------------------------------------------------------------------

    def _run_interactive(self, obdiag_tools: list, config: AgentConfig) -> ObdiagResult:
        from src.handler.agent.tui.agent import create_cli_agent
        from src.handler.agent.tui.app import run_textual_app
        from src.handler.agent.tui.config import create_model
        from src.handler.agent.tui.sessions import generate_thread_id, get_checkpointer

        async def _run() -> Any:
            # create_model() reads [models].default / [models].recent from
            # ~/.obdiag/config/agent.toml via deepagents-cli's ModelConfig.load()
            model_result = create_model()
            model_result.apply_to_settings()
            model = model_result.model

            async with get_checkpointer() as checkpointer:
                obdiag_interrupt_on = {fn.__name__: True for fn in obdiag_tools} if config.tool_approval else None
                agent, backend = create_cli_agent(
                    model=model,
                    assistant_id=_ASSISTANT_ID,
                    tools=obdiag_tools,
                    system_prompt=SYSTEM_PROMPT,
                    interactive=True,
                    auto_approve=not config.tool_approval,
                    extra_interrupt_on=obdiag_interrupt_on,
                    checkpointer=checkpointer,
                )
                thread_id = generate_thread_id()
                return await run_textual_app(
                    agent=agent,
                    backend=backend,
                    assistant_id=_ASSISTANT_ID,
                    thread_id=thread_id,
                )

        try:
            app_result = asyncio.run(_run())
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Agent session ended", "thread_id": app_result.thread_id or ""})
        except KeyboardInterrupt:
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Agent session interrupted"})
        except Exception as e:
            _LOG.exception("Interactive agent session error")
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=str(e))

    # ------------------------------------------------------------------
    # Single-shot mode — async streaming, no TUI
    # ------------------------------------------------------------------

    def _run_single_shot(self, msg: str, obdiag_tools: list, config: AgentConfig) -> ObdiagResult:
        from src.handler.agent.tui.agent import create_cli_agent
        from src.handler.agent.tui.config import build_stream_config, create_model
        from src.handler.agent.tui.file_ops import FileOpTracker
        from src.handler.agent.tui.non_interactive import _run_agent_loop
        from src.handler.agent.tui.sessions import generate_thread_id, get_checkpointer
        from rich.console import Console

        async def _run() -> None:
            model_result = create_model()
            model_result.apply_to_settings()
            model = model_result.model

            async with get_checkpointer() as checkpointer:
                agent, _backend = create_cli_agent(
                    model=model,
                    assistant_id=_ASSISTANT_ID,
                    tools=obdiag_tools,
                    system_prompt=SYSTEM_PROMPT,
                    interactive=False,
                    auto_approve=True,
                    enable_ask_user=False,
                    checkpointer=checkpointer,
                )
                thread_id = generate_thread_id()
                lc_config = build_stream_config(thread_id, _ASSISTANT_ID)
                console = Console()
                file_op_tracker = FileOpTracker(assistant_id=_ASSISTANT_ID, backend=None)
                await _run_agent_loop(agent, msg, lc_config, console, file_op_tracker)

        try:
            asyncio.run(_run())
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Task completed"})
        except KeyboardInterrupt:
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Interrupted"})
        except Exception as e:
            _LOG.exception("Single-shot agent error")
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=str(e))
