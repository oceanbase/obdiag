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
@file: handler.py
@desc: obdiag agent interactive handler using Pydantic-AI

       The handler creates the agent once via ``create_agent()`` and drives the
       interactive (or single-shot) conversation loop.  Conversation history is
       maintained as a plain list of pydantic-ai message objects — no wrapper
       class is needed.
"""

import dataclasses
import json
import os
import time
from collections.abc import AsyncIterable
from typing import Any, Dict, List, Optional

from pydantic_ai import Agent
from pydantic_ai.messages import (
    FunctionToolCallEvent,
    ModelMessagesTypeAdapter,
    ModelRequest,
)
from pydantic_ai.output import DeferredToolRequests
from pydantic_ai.tools import DeferredToolResults, RunContext, ToolDenied
from pydantic_ai.usage import RunUsage

from src.common.result_type import ObdiagResult
from src.common.tool import Util
from pydantic_ai.toolsets.function import FunctionToolset as _FunctionToolset

from src.handler.agent.agent import create_agent
from src.handler.agent.executor import OBDIAG_TOOL_SUMMARY_ZH
from src.handler.agent.config import (
    load_agent_config,
    OBDIAG_CONFIG_PATH,
    resolve_cluster_config_path,
)
from src.handler.agent.models import AgentConfig, AgentDependencies, read_obcluster_config

SESSIONS_DIR = os.path.expanduser("~/.obdiag/sessions")

try:
    from rich.console import Console
    from rich.markdown import Markdown

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    from prompt_toolkit import prompt as pt_prompt
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.history import InMemoryHistory
    from prompt_toolkit.shortcuts import CompleteStyle

    PROMPT_TOOLKIT_AVAILABLE = True

    # (slash command, short description) — shown in completion menu; keep in sync with /help and _parse_builtin_command.
    _BUILTIN_SLASH_COMMANDS_META: List[tuple[str, str]] = [
        ("/?", "Show help message"),
        ("/clear", "Clear conversation history"),
        ("/cluster", "Show current active cluster"),
        ("/compact", "Summarize history (save context)"),
        ("/exit", "Exit (auto-save session)"),
        ("/help", "Show help message"),
        ("/history", "Show conversation history"),
        ("/q", "Exit (auto-save session)"),
        ("/quit", "Exit (auto-save session)"),
        ("/save", "Save session to disk"),
        ("/sessions", "List saved sessions"),
        ("/tools", "List diagnostic tools"),
        ("/usage", "Token usage (turn + session)"),
        ("/use", "Switch cluster (/use <name|path>)"),
    ]

    class _BuiltinSlashCompleter(Completer):
        """Prefix-complete lines that start with ``/`` (no space yet) — e.g. ``/h`` → ``/help``, ``/history``."""

        def get_completions(self, document, complete_event):
            text = document.text_before_cursor
            if " " in text or not text.startswith("/"):
                return
            prefix = text.lower()
            matches = [(cmd, desc) for cmd, desc in _BUILTIN_SLASH_COMMANDS_META if cmd.lower().startswith(prefix)]
            matches.sort(key=lambda x: x[0].lower())
            n = len(text)
            for cmd, desc in matches:
                yield Completion(cmd, start_position=-n, display_meta=desc)

    _builtin_slash_completer = _BuiltinSlashCompleter()
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False
    _builtin_slash_completer = None  # type: ignore[assignment,misc]


MAX_HISTORY_MESSAGES = 20
# Limit for resumed sessions to reduce Qwen API "tool_call_id" errors with long tool chains
MAX_HISTORY_WHEN_RESUMED = 10

# Session file: v1 wraps messages + cumulative RunUsage; legacy file is a raw JSON array of messages.
_SESSION_FILE_VERSION = 1

# Max characters of tool arguments shown on the terminal (agent UI).
_TOOL_ARGS_PREVIEW_MAX_LEN = 220

# ``compact`` command: build a plain-text transcript for the summarization model call.
_COMPACT_TRANSCRIPT_TOTAL_MAX = 120_000
_COMPACT_USER_SNIPPET_MAX = 8_000
_COMPACT_ASSISTANT_TEXT_MAX = 6_000
_COMPACT_TOOL_RETURN_MAX = 2_500
_COMPACT_TOOL_CALL_ARGS_MAX = 400


def _preview_tool_args(args: Any, max_len: int = _TOOL_ARGS_PREVIEW_MAX_LEN) -> str:
    """Short single-line preview of tool arguments for the CLI."""
    if args in (None, "", {}):
        return ""
    try:
        if isinstance(args, dict):
            text = json.dumps(args, ensure_ascii=False, separators=(",", ":"))
        else:
            text = str(args).strip()
    except Exception:
        text = str(args)
    if not text:
        return ""
    if len(text) > max_len:
        text = text[: max_len - 3] + "..."
    return f" {text}"


# English-neutral spinners; tool trace line = 中文摘要 + (use tool_name) + 参数（便于对照文档与配置）。
_SPINNER_THINKING = "Thinking…"
_SPINNER_WORKING = "Working…"
_SPINNER_EXECUTING = "Executing…"
_SPINNER_COMPACTING = "Compacting…"
# Clear width for loading lines (label + margin for spinner / control chars). Single source — was 20 vs 24.
_SPINNER_CLEAR_WIDTH = (
    max(
        len(_SPINNER_THINKING),
        len(_SPINNER_WORKING),
        len(_SPINNER_EXECUTING),
        len(_SPINNER_COMPACTING),
    )
    + 12
)


def _clear_loading_line(stdio: Any) -> None:
    stdio.print("\r" + " " * _SPINNER_CLEAR_WIDTH + "\r", end="")


# _TOOL_TRACE_SUMMARY_ZH is now OBDIAG_TOOL_SUMMARY_ZH in executor.py (single source of truth).
_TOOL_TRACE_SUMMARY_ZH = OBDIAG_TOOL_SUMMARY_ZH


def _list_function_tools_from_agent(agent: Agent) -> List[tuple]:
    """Return sorted (tool_name, description_first_line) for static function tools."""
    rows: List[tuple] = []
    seen: set = set()
    for ts in agent.toolsets:
        if not isinstance(ts, _FunctionToolset):
            continue
        for _key, tool in ts.tools.items():
            name = getattr(tool, "name", None) or str(_key)
            if name in seen:
                continue
            seen.add(name)
            desc = getattr(tool, "description", None) or ""
            first = (desc.split("\n", 1)[0] if desc else "").strip()
            rows.append((name, first))
    rows.sort(key=lambda x: x[0].lower())
    return rows


def _non_function_toolset_count(agent: Agent) -> int:
    """Number of toolsets that are not FunctionToolset (e.g. MCP)."""
    return sum(1 for ts in agent.toolsets if not isinstance(ts, _FunctionToolset))


def _trace_arg_value(v: Any, max_len: int = 56) -> str:
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        if not v:
            return ""
        if len(v) <= 2:
            inner = ", ".join(_trace_arg_value(x, 24) for x in v)
            truncated = inner[:max_len]
            if truncated:
                return truncated
            return f"{len(v)} items"
        return f"{len(v)} items"
    if isinstance(v, dict):
        return f"{len(v)} fields"
    s = str(v).replace("\n", " ").strip()
    if len(s) > max_len:
        return s[: max_len - 1] + "…"
    return s


def _summarize_tool_args_for_trace(tool_name: str, raw_args: Any) -> str:
    """Build compact key=value summary for tool trace; empty if no meaningful args."""
    if raw_args in (None, "", {}):
        return ""
    d: Dict[str, Any]
    if isinstance(raw_args, dict):
        d = raw_args
    elif isinstance(raw_args, str):
        try:
            parsed = json.loads(raw_args)
            d = parsed if isinstance(parsed, dict) else {}
        except Exception:
            return f" | {_trace_arg_value(raw_args, 72)}"
    else:
        return ""
    if not d:
        return ""
    order = [
        "since",
        "from_time",
        "to_time",
        "scope",
        "grep",
        "store_dir",
        "recent_count",
        "cluster_config_path",
        "trace_id",
        "log_level",
        "sql",
        "file_path",
        "directory_path",
        "pattern",
        "start_line",
        "max_lines",
        "command",
        "cwd",
        "scene",
        "cases",
        "disk",
        "date",
        "query",
        "context_text",
        "component",
        "version",
        "oms_component_id",
        "ob_cluster_name",
        "db_host",
        "db_port",
        "nodes",
        "mode",
        "encoding",
    ]
    parts: List[str] = []
    seen = set()
    for key in order:
        if key not in d:
            continue
        val = d[key]
        if val is None or val == "" or val == [] or val == {}:
            continue
        seen.add(key)
        if key == "nodes" and isinstance(val, list):
            parts.append(f"nodes={len(val)}")
        elif key == "grep" and isinstance(val, list):
            parts.append(f"grep={len(val)} keywords")
        elif key == "sql":
            parts.append(f"sql={_trace_arg_value(val, 52)}")
        elif key == "context_text":
            parts.append(f"context={_trace_arg_value(val, 40)}")
        else:
            tv = _trace_arg_value(val, 48)
            if tv:
                parts.append(f"{key}={tv}")
    for key, val in d.items():
        if key in seen:
            continue
        if val is None or val == "" or val == [] or val == {}:
            continue
        tv = _trace_arg_value(val, 40)
        if tv:
            parts.append(f"{key}={tv}")
        if len(parts) >= 8:
            break
    if not parts:
        return ""
    return " | " + ", ".join(parts)


def _format_tool_trace_line(tool_name: str, raw_args: Any) -> str:
    """Human-readable summary + explicit tool id for docs/logs, e.g. ``采集…(use gather_log) | …``."""
    summary = _TOOL_TRACE_SUMMARY_ZH.get(tool_name) or f"调用工具（{tool_name}）"
    extra = _summarize_tool_args_for_trace(tool_name, raw_args)
    return f"{summary}(use {tool_name}){extra}"


def _agent_tool_trace_event_handler(enabled: bool):
    """Build pydantic-ai ``event_stream_handler`` so each tool call prints a visible line (not a blind spinner)."""
    if not enabled:
        return None

    async def _on_stream(ctx: RunContext[AgentDependencies], stream: AsyncIterable[Any]) -> None:
        stdio = ctx.deps.stdio
        async for event in stream:
            if isinstance(event, FunctionToolCallEvent):
                if getattr(stdio, "syncing", False):
                    stdio.stop_loading("succeed")
                    _clear_loading_line(stdio)
                part = event.part
                stdio.print(f"  → {_format_tool_trace_line(part.tool_name, part.args)}\n")
                stdio.start_loading(_SPINNER_WORKING)

    return _on_stream


def _user_prompt_part_as_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    return str(content)


def _tool_return_part_body(part: Any) -> str:
    """Plain text body of a tool-return / builtin-tool-return message part."""
    if isinstance(part, dict):
        return str(part.get("content", ""))
    try:
        return part.model_response_str()  # type: ignore[union-attr]
    except Exception:
        return str(getattr(part, "content", ""))


def _history_as_compact_transcript(history: List[Any], max_total_chars: int) -> str:
    """Flatten pydantic-ai message history into plain text for ``/compact`` summarization."""
    chunks: List[str] = []
    for msg in history:
        k = getattr(msg, "kind", None)
        parts = getattr(msg, "parts", ()) or ()
        if k == "request":
            for p in parts:
                pk = getattr(p, "part_kind", None)
                if pk == "user-prompt":
                    text = _user_prompt_part_as_text(getattr(p, "content", ""))[:_COMPACT_USER_SNIPPET_MAX]
                    chunks.append(f"User:\n{text}\n")
                elif pk in ("tool-return", "builtin-tool-return"):
                    name = getattr(p, "tool_name", "tool")
                    body = ""
                    try:
                        body = p.model_response_str()  # type: ignore[union-attr]
                    except Exception:
                        body = str(getattr(p, "content", ""))
                    if len(body) > _COMPACT_TOOL_RETURN_MAX:
                        body = body[: _COMPACT_TOOL_RETURN_MAX - 3] + "..."
                    chunks.append(f"Tool result ({name}):\n{body}\n")
        elif k == "response":
            for p in parts:
                pk = getattr(p, "part_kind", None)
                if pk == "text":
                    t = (getattr(p, "content", "") or "")[:_COMPACT_ASSISTANT_TEXT_MAX]
                    chunks.append(f"Assistant:\n{t}\n")
                elif pk in ("tool-call", "builtin-tool-call"):
                    name = getattr(p, "tool_name", "?")
                    args = getattr(p, "args", None)
                    ap = _preview_tool_args(args, max_len=_COMPACT_TOOL_CALL_ARGS_MAX) if args else ""
                    chunks.append(f"(assistant requested tool {name}{ap})\n")
    text = "\n---\n".join(chunks)
    if len(text) > max_total_chars:
        text = "...[earlier transcript truncated]...\n\n" + text[-max_total_chars:]
    return text


def _run_usage_from_dict(data: Optional[Dict[str, Any]]) -> RunUsage:
    if not data or not isinstance(data, dict):
        return RunUsage()
    names = {f.name for f in dataclasses.fields(RunUsage)}
    kwargs: Dict[str, Any] = {}
    for key, val in data.items():
        if key not in names:
            continue
        if key == "details" and isinstance(val, dict):
            det: Dict[str, int] = {}
            for k, v in val.items():
                try:
                    det[str(k)] = int(v)
                except (TypeError, ValueError):
                    continue
            kwargs[key] = det
            continue
        try:
            kwargs[key] = int(val)
        except (TypeError, ValueError):
            continue
    try:
        return RunUsage(**kwargs)
    except TypeError:
        return RunUsage()


def _run_usage_to_dict(u: RunUsage) -> Dict[str, Any]:
    return dataclasses.asdict(u)


def _accumulate_run_usage(dst: RunUsage, src: Any) -> None:
    try:
        dst.incr(src)
    except Exception:
        pass


def _load_session_messages_and_usage(raw: bytes) -> tuple[List[Any], RunUsage]:
    """Parse session file bytes: legacy JSON array or v1 envelope with messages + usage."""
    stripped = raw.lstrip()
    if stripped.startswith(b"["):
        loaded = list(ModelMessagesTypeAdapter.validate_json(raw))
        return loaded, RunUsage()
    root = json.loads(raw.decode("utf-8"))
    if not isinstance(root, dict) or "messages" not in root:
        raise ValueError("unrecognized session JSON shape")
    msg_bytes = json.dumps(root["messages"], ensure_ascii=False).encode("utf-8")
    loaded = list(ModelMessagesTypeAdapter.validate_json(msg_bytes))
    usage = _run_usage_from_dict(root.get("usage"))
    return loaded, usage


# Part kinds that make a ModelRequest a "tool" role message (must follow assistant with tool_calls)
_TOOL_ROLE_PART_KINDS = frozenset({"tool-return", "retry-prompt", "builtin-tool-return"})
# Part kinds that indicate tool calls (must be followed by tool responses)
_TOOL_CALL_PART_KINDS = frozenset({"tool-call", "builtin-tool-call"})


def _is_tool_only_message(msg: Any) -> bool:
    """True if message becomes role='tool' in API (must have preceding tool_calls)."""
    if getattr(msg, "kind", None) != "request":
        return False
    parts = getattr(msg, "parts", ())
    if not parts:
        return False
    return all(getattr(p, "part_kind", None) in _TOOL_ROLE_PART_KINDS for p in parts)


def _is_user_message(msg: Any) -> bool:
    """True if message is a user message (safe to start history with)."""
    if getattr(msg, "kind", None) != "request":
        return False
    parts = getattr(msg, "parts", ())
    return any(getattr(p, "part_kind", None) == "user-prompt" for p in parts)


def _has_tool_calls(msg: Any) -> bool:
    """True if message has tool_calls (must be followed by tool responses).
    Checks both msg.tool_calls and parts for tool-call part kinds.
    """
    # Direct tool_calls attribute (ModelResponse)
    tc = getattr(msg, "tool_calls", None)
    if callable(tc):
        if tc():
            return True
    elif tc:
        return True
    # Check parts for ToolCallPart (part_kind 'tool-call' or 'builtin-tool-call')
    parts = getattr(msg, "parts", ()) or ()
    for p in parts:
        if getattr(p, "part_kind", None) in _TOOL_CALL_PART_KINDS:
            return True
    # Dict-style (e.g. from JSON round-trip)
    if isinstance(msg, dict) and msg.get("tool_calls"):
        return True
    return False


def _message_has_any_tool_content(msg: Any) -> bool:
    """True if message or its parts contain any tool-related content (defensive check)."""
    if _has_tool_calls(msg):
        return True
    if _is_tool_only_message(msg):
        return True
    if isinstance(msg, dict):
        return bool(msg.get("tool_calls") or msg.get("tool_call_id"))
    # Check parts recursively for tool_call_id
    parts = getattr(msg, "parts", ()) or ()
    for p in parts:
        if getattr(p, "tool_call_id", None):
            return True
        if isinstance(p, dict) and p.get("tool_call_id"):
            return True
    return False


def _truncate_history_safe(history: List[Any], max_messages: int) -> List[Any]:
    """Truncate history without breaking tool_calls/tool response pairs.

    Strategy:
    1. Always keep all messages that have no tool content (user messages and
       text-only assistant messages).
    2. For tool-related messages (assistant-with-tool-calls + the corresponding
       tool-response messages), keep them as a unit or drop the unit as a unit
       so the history never has an orphaned tool-call or tool-response.
    3. After filtering, if still over max_messages, drop the oldest messages
       in pairs (user + assistant) from the front.  Never start with an
       assistant message.

    This preserves the last MAX_HISTORY_MESSAGES worth of meaningful context
    while avoiding the 'Tool call id not found' API error from providers like
    Qwen / DashScope.
    """
    if not history:
        return []

    # Build a list of "units": either a single non-tool message, or a
    # (tool-call assistant msg, *tool-response msgs) tuple that must travel
    # together.
    units: List[List[Any]] = []
    i = 0
    while i < len(history):
        msg = history[i]
        if _is_tool_only_message(msg):
            # Orphaned tool-response without a preceding tool-call — skip it.
            i += 1
            continue
        if _message_has_any_tool_content(msg):
            # Assistant message with tool_calls — collect it and all following
            # tool-response messages as one unit.
            unit: List[Any] = [msg]
            j = i + 1
            while j < len(history) and _is_tool_only_message(history[j]):
                unit.append(history[j])
                j += 1
            units.append(unit)
            i = j
        else:
            units.append([msg])
            i += 1

    # Flatten units and count messages
    flat: List[Any] = [m for unit in units for m in unit]
    if len(flat) <= max_messages:
        # Drop leading non-user messages before returning
        while flat and not _is_user_message(flat[0]):
            flat.pop(0)
        return flat

    # Drop oldest units from the front until under the budget
    while units and sum(len(u) for u in units) > max_messages:
        units.pop(0)

    flat = [m for unit in units for m in unit]
    # Drop leading until we hit a user message
    while flat and not _is_user_message(flat[0]):
        flat.pop(0)
    return flat


class AiAgentHandler:
    """obdiag agent interactive handler using Pydantic-AI"""

    BETA_WARNING = """
╔══════════════════════════════════════════════════════════════════════════╗
║                          ⚠️  BETA FEATURE WARNING  ⚠️                      ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  This is a BETA feature and may change in future versions.               ║
║  Compatibility with previous versions is not guaranteed.                 ║
║                                                                          ║
║  If you encounter any issues, please report them at:                     ║
║  https://github.com/oceanbase/obdiag/issues                              ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

    WELCOME_MESSAGE = """
╔══════════════════════════════════════════════════════════════════════════╗
║                    obdiag agent                                           ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  Welcome! I'm your obdiag agent for OceanBase diagnostics.                 ║
║  You can ask me to:                                                      ║
║    - Collect diagnostic information (logs, perf, sysstat)                ║
║    - Analyze logs and performance data                                   ║
║    - Run health checks                                                   ║
║    - Perform root cause analysis                                         ║
║    - Query the database directly                                         ║
║    - Generate obdiag configuration                                       ║
║    - And much more...                                                    ║
║                                                                          ║
║  Type /exit or /quit to end the session.                                 ║
║  Built-in commands start with / — try /help for the list.                  ║
║  Normal chat does not use a leading slash.                               ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

    def __init__(self, context: Any):
        self.context = context
        self.stdio = context.stdio
        self.options = context.options

        self._agent: Optional[Agent[AgentDependencies, str]] = None
        self._deps: Optional[AgentDependencies] = None
        self._history: List[Any] = []
        self._session_id: Optional[str] = None
        # Cumulative LLM usage for this session (pydantic-ai RunUsage); persisted in v1 session files.
        self._cumulative_usage = RunUsage()
        self._last_turn_usage = RunUsage()
        # Max run_sync input_tokens seen within the last completed user turn (for auto-compact).
        self._last_turn_peak_input_tokens = 0
        # After first failed disk save, suppress repeat warns until a save succeeds (debounce noise).
        self._session_save_warned = False

        self.console = Console() if RICH_AVAILABLE else None
        self.input_history = InMemoryHistory() if PROMPT_TOOLKIT_AVAILABLE else None

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _load_config(self) -> Dict:
        """Load agent configuration from disk."""
        return load_agent_config(stdio=self.stdio)

    # ------------------------------------------------------------------
    # Session persistence
    # ------------------------------------------------------------------

    @staticmethod
    def _ensure_sessions_dir():
        os.makedirs(SESSIONS_DIR, exist_ok=True)

    def _session_path(self, session_id: str) -> str:
        return os.path.join(SESSIONS_DIR, f"{session_id}.json")

    def _generate_session_id(self) -> str:
        return time.strftime("%Y%m%d_%H%M%S")

    def _save_session(self):
        """Save current history (and cumulative usage) to disk."""
        if not self._history:
            return
        self._ensure_sessions_dir()
        if not self._session_id:
            self._session_id = self._generate_session_id()
        path = self._session_path(self._session_id)
        try:
            messages_val = json.loads(ModelMessagesTypeAdapter.dump_json(self._history).decode("utf-8"))
            envelope = {
                "v": _SESSION_FILE_VERSION,
                "messages": messages_val,
                "usage": _run_usage_to_dict(self._cumulative_usage),
            }
            with open(path, "w", encoding="utf-8") as f:
                json.dump(envelope, f, ensure_ascii=False, separators=(",", ":"))
            self._session_save_warned = False
        except Exception as e:
            self.stdio.verbose(f"Failed to save session: {e}")
            if not self._session_save_warned:
                self.stdio.warn(f"Session could not be saved to disk: {e}")
                self._session_save_warned = True

    def _load_session(self, session_id: str) -> bool:
        """Load history from a saved session. Returns True on success."""
        path = self._session_path(session_id)
        if not os.path.exists(path):
            self.stdio.print(f"Session not found: {session_id}\n")
            return False
        try:
            with open(path, "rb") as f:
                data = f.read()
            loaded, usage = _load_session_messages_and_usage(data)
            self._history = _truncate_history_safe(loaded, MAX_HISTORY_WHEN_RESUMED)
            self._cumulative_usage = usage
            self._last_turn_usage = RunUsage()
            self._last_turn_peak_input_tokens = 0
            self._session_id = session_id
            return True
        except Exception as e:
            self.stdio.print(f"Failed to load session: {e}\n")
            return False

    def _list_sessions(self):
        """List saved sessions."""
        self._ensure_sessions_dir()
        files = sorted(
            [f for f in os.listdir(SESSIONS_DIR) if f.endswith(".json")],
            reverse=True,
        )
        if not files:
            self.stdio.print("No saved sessions.\n")
            return
        self.stdio.print("\nSaved sessions:\n")
        for f in files[:20]:
            sid = f.removesuffix(".json")
            path = os.path.join(SESSIONS_DIR, f)
            size = os.path.getsize(path)
            self.stdio.print(f"  {sid}  ({size} bytes)")
        self.stdio.print(f"\nResume with: obdiag agent --resume <session_id>\n")

    def _init_agent(self, config_dict: Dict):
        """Build the agent and dependencies from the config dictionary."""
        config = AgentConfig.from_dict(config_dict)

        api_key = config.api_key or os.getenv("OPENAI_API_KEY")
        if not api_key:
            provider = (config.provider or "openai").lower()
            env_hint = {
                "anthropic": "ANTHROPIC_API_KEY",
                "gemini": "GEMINI_API_KEY",
                "google": "GEMINI_API_KEY",
            }.get(provider, "OPENAI_API_KEY")
            raise ValueError(
                f"API key is required for provider '{provider}'. "
                f"Set {env_hint} or configure llm.api_key in ~/.obdiag/config/agent.yml"
            )
        config.api_key = api_key

        if not config.base_url:
            config.base_url = os.getenv("OPENAI_BASE_URL") or None

        obdiag_config_path = Util.get_option(self.options, "c") or OBDIAG_CONFIG_PATH
        resolved = resolve_cluster_config_path(obdiag_config_path)
        if resolved:
            obdiag_config_path = resolved
        else:
            obdiag_config_path = os.path.abspath(os.path.expanduser(obdiag_config_path))

        cluster_config = {}
        if hasattr(self.context, "cluster_config") and self.context.cluster_config:
            cluster_config = self.context.cluster_config
        if not cluster_config and os.path.isfile(obdiag_config_path):
            cluster_config = read_obcluster_config(obdiag_config_path)

        self._deps = AgentDependencies(
            cluster_config=cluster_config,
            stdio=self.stdio,
            config_path=obdiag_config_path,
            oceanbase_knowledge_bearer_token=((config.oceanbase_knowledge_bearer_token or "").strip()),
        )

        self._agent = create_agent(config, self.stdio)

    # ------------------------------------------------------------------
    # Chat
    # ------------------------------------------------------------------

    def _chat_sync(self, message: str, config_dict: Optional[Dict] = None) -> tuple[str, bool]:
        """Run a single synchronous turn, managing history. Handles tool approval and streaming."""
        ui = (config_dict or {}).get("ui", {})
        tool_approval = ui.get("tool_approval", True)
        if Util.get_option(self.options, "yolo"):
            tool_approval = False
        stream_output = ui.get("stream_output", False)
        show_tool_trace = ui.get("show_tool_trace", True)
        event_handler = _agent_tool_trace_event_handler(show_tool_trace)

        output_type = [str, DeferredToolRequests]
        history = _truncate_history_safe(list(self._history), MAX_HISTORY_MESSAGES) if self._history else None
        if history is not None and len(history) == 0:
            history = None
        deferred_results = None
        turn_total = RunUsage()
        peak_input = 0

        while True:
            if stream_output:
                run_result = self._agent.run_stream_sync(
                    message if deferred_results is None else "Continue",
                    output_type=output_type,
                    deps=self._deps,
                    message_history=history,
                    deferred_tool_results=deferred_results,
                    event_stream_handler=event_handler,
                )
                output = run_result.output
            else:
                run_result = self._agent.run_sync(
                    message if deferred_results is None else "Continue",
                    output_type=output_type,
                    deps=self._deps,
                    message_history=history,
                    deferred_tool_results=deferred_results,
                    event_stream_handler=event_handler,
                )
                output = run_result.output

            if isinstance(output, DeferredToolRequests):
                try:
                    u = run_result.usage()
                    _accumulate_run_usage(turn_total, u)
                    peak_input = max(peak_input, u.input_tokens)
                except Exception:
                    pass
                # Stop loading spinner so the approval prompt is visible
                self.stdio.stop_loading("succeed")
                _clear_loading_line(self.stdio)
                # Gather approvals
                deferred_results = DeferredToolResults()
                for call in output.approvals:
                    tool_name = call.tool_name
                    args = call.args
                    call_id = call.tool_call_id
                    if tool_approval:
                        self.stdio.print(f"\n[Tool] {_format_tool_trace_line(tool_name, args)}")
                        try:
                            if PROMPT_TOOLKIT_AVAILABLE:
                                ans = pt_prompt("Execute? [y/n/all/deny]: ", history=None).strip().lower()
                            else:
                                ans = input("Execute? [y/n/all/deny]: ").strip().lower()
                        except (EOFError, KeyboardInterrupt):
                            ans = "n"
                        if ans == "all":
                            deferred_results.approvals[call_id] = True
                            # Auto-approve remaining
                            for c in output.approvals:
                                if c.tool_call_id not in deferred_results.approvals:
                                    deferred_results.approvals[c.tool_call_id] = True
                            break
                        elif ans == "deny":
                            deferred_results.approvals[call_id] = ToolDenied("User denied")
                            for c in output.approvals:
                                if c.tool_call_id not in deferred_results.approvals:
                                    deferred_results.approvals[c.tool_call_id] = ToolDenied("User denied")
                            break
                        elif ans in ("y", "yes"):
                            deferred_results.approvals[call_id] = True
                        else:
                            deferred_results.approvals[call_id] = ToolDenied("User skipped")
                    else:
                        deferred_results.approvals[call_id] = True
                history = run_result.all_messages()
                message = "Continue"
                self.stdio.start_loading(_SPINNER_EXECUTING)
                continue
            else:
                was_streamed = False
                if stream_output and hasattr(run_result, "stream_text"):
                    self.stdio.print("")
                    for chunk in run_result.stream_text(delta=True):
                        self.stdio.print(chunk, end="")
                    self.stdio.print("\n")
                    was_streamed = True
                try:
                    u = run_result.usage()
                    _accumulate_run_usage(turn_total, u)
                    peak_input = max(peak_input, u.input_tokens)
                except Exception:
                    pass
                self._last_turn_usage = turn_total
                self._last_turn_peak_input_tokens = peak_input
                _accumulate_run_usage(self._cumulative_usage, turn_total)
                self._history = _truncate_history_safe(list(run_result.all_messages()), MAX_HISTORY_MESSAGES)
                return (output or "", was_streamed)

    # ------------------------------------------------------------------
    # UI helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clear_screen():
        os.system("clear" if os.name != "nt" else "cls")

    def _render_markdown(self, text: str):
        if RICH_AVAILABLE and self.console:
            try:
                self.console.print(Markdown(text))
            except Exception:
                self.stdio.print(text)
        else:
            self.stdio.print(text)

    @staticmethod
    def _usage_tokens_compact(u: RunUsage) -> str:
        """↑/↓ line segment (input / output tokens); compact ``/usage``-style token line."""
        return f"↑{u.input_tokens:,} ↓{u.output_tokens:,}"

    def _usage_context_peak_tokens(self, last_turn: RunUsage) -> int:
        """Best-effort prompt/context size for last user turn (API input_tokens peak, else turn sum)."""
        peak = self._last_turn_peak_input_tokens
        if peak > 0:
            return peak
        return max(0, last_turn.input_tokens)

    @staticmethod
    def _parse_context_window_tokens(ui: Dict[str, Any]) -> Optional[int]:
        raw = ui.get("context_window_tokens")
        if raw is None:
            return None
        try:
            n = int(raw)
            return n if n > 0 else None
        except (TypeError, ValueError):
            return None

    def _format_usage_line(self, config_dict: Optional[Dict] = None) -> str:
        """One-line footer after each reply; with ``context_window_tokens`` set, includes used % and counts."""
        t, s = self._last_turn_usage, self._cumulative_usage
        ui = (config_dict or {}).get("ui", {})
        ctx = self._parse_context_window_tokens(ui)

        turn_seg = self._usage_tokens_compact(t)
        n = t.requests
        req_lbl = "req" if n == 1 else "reqs"
        in_out_req = f"{turn_seg} · {n} {req_lbl}"

        if ctx is not None:
            used = self._usage_context_peak_tokens(t)
            pct = min(100.0, 100.0 * used / ctx)
            pct_s = f"{pct:.0f}%" if pct >= 9.95 or pct < 0.05 else f"{pct:.1f}%"
            head = f"{used:,}  {pct_s}"
            if ui.get("show_usage_cost"):
                head += "  ($0.00)"
            line = f"{head}  ·  {in_out_req}"
        else:
            line = in_out_req

        same_as_session = t.input_tokens == s.input_tokens and t.output_tokens == s.output_tokens and t.requests == s.requests
        if not same_as_session:
            m = s.requests
            cum_lbl = "req" if m == 1 else "reqs"
            line += f"  ·  session {self._usage_tokens_compact(s)} · {m} {cum_lbl}"
        return line

    def _show_usage_command(self, config_dict: Optional[Dict] = None):
        s = self._cumulative_usage
        t = self._last_turn_usage
        ui = (config_dict or {}).get("ui", {})
        ctx = self._parse_context_window_tokens(ui)
        peak = self._usage_context_peak_tokens(t)
        self.stdio.print("\nToken usage (API-reported; sums over requests, not exact context bytes).\n")
        self.stdio.print(f"  Last turn:  {self._usage_tokens_compact(t)}  (total {t.input_tokens + t.output_tokens:,})  ·  {t.requests} requests\n")
        if ctx is not None:
            pct = min(100.0, 100.0 * peak / ctx)
            self.stdio.print(f"  Context:    peak input ≈ {peak:,} / {ctx:,} window  ({pct:.1f}%)\n")
        self.stdio.print(f"  Session:    {self._usage_tokens_compact(s)}  (total {s.input_tokens + s.output_tokens:,})  ·  {s.requests} requests\n")
        if s.tool_calls or t.tool_calls:
            self.stdio.print(f"  tool_calls (runtime): turn={t.tool_calls}  session={s.tool_calls}\n")
        self.stdio.print(
            "Cumulative totals are saved in v1 session files and restored on --resume. "
            "Use ``compact`` to shorten history when prompts grow too large.\n"
            "Set ui.context_window_tokens in agent.yml to enable auto-compact when peak input_tokens "
            "exceeds auto_compact_threshold_ratio of that window.\n\n"
        )

    def _show_welcome(self, config_dict: Dict):
        ui = config_dict.get("ui", {})
        if ui.get("clear_screen", True):
            self._clear_screen()
        if ui.get("show_beta_warning", True):
            self.stdio.print(self.BETA_WARNING)
            self.stdio.print("")
        if ui.get("show_welcome", True):
            self.stdio.print(self.WELCOME_MESSAGE)
            self.stdio.print("")

    def _show_help(self):
        self.stdio.print(
            """
Built-in commands (must start with / — like common CLI coding assistants; plain chat has no leading /):
  /help, /?                - Show this help message
  /exit, /quit, /q         - Exit the agent (auto-saves session)
  /clear                   - Clear conversation history
  /compact                 - Summarize conversation into one message (reduces context)
  /usage                   - Show token usage for last turn and session (API-reported; v1 sessions)
  /history                 - Show conversation history
  /tools                   - List available diagnostic tools
  /use <name|path>         - Switch active cluster (e.g., /use obdiag_test or /use /path/to/config.yml)
  /cluster                 - Show current active cluster info
  /save                    - Save current session
  /sessions                - List saved sessions

Cluster: Default ~/.obdiag/config.yml. In chat, ask e.g. "目前有哪些集群" to list all config files
under ~/.obdiag; use ``/cluster`` for the active session only. Short names (e.g., obdiag_test for
~/.obdiag/obdiag_test.yml): ``/use obdiag_test``, or pass cluster_config_path="obdiag_test".

Session: conversations are auto-saved on exit. Resume with: obdiag agent --resume <session_id>

Configure tool_approval, stream_output, and show_tool_trace in ~/.obdiag/config/agent.yml (ui section):
  - tool_approval: prompt before SQL and bash execution; y/n/all/deny
  - stream_output: stream LLM response as it arrives
  - show_tool_trace: print each tool name and short args during a turn (default true; set false for minimal output)
  - compact_transcript_max_chars: max size of transcript sent to the model for ``compact`` (default 120000)
  - show_usage_after_turn: compact footer after each reply (default false); with context_window_tokens set, shows ``used  pct%  ·  ↑↓ …``
  - show_usage_cost: append ``($0.00)`` placeholder in footer (default false; real pricing not implemented)
  - context_window_tokens: model context size (e.g. 128000); enables % in footer, auto-compact, and ``usage`` context line
  - auto_compact: enable automatic compact when peak input_tokens exceed threshold (default true)
  - auto_compact_threshold_ratio: fraction of context_window_tokens (default 0.85)
  - auto_compact_min_messages: minimum history messages before auto-compact (default 2)

You can also ask me questions in natural language, such as:
  - "帮我检查数据库的健康状态"
  - "收集最近1小时的日志"
  - "分析日志中的错误信息"
  - "检查IO性能"
  - "执行根因分析"
  - "帮我生成obdiag配置文件"
  - "目前有哪些集群"
  - "对 obdiag_test 集群巡检"  (agent 会传递 cluster_config_path="obdiag_test")

Tip: Asking "what is /compact" in natural language is fine; only lines that *start* with / are commands.
With prompt_toolkit, type ``/`` then letters (e.g. ``/h``) for matching command hints; use Tab to complete.
"""
        )

    def _compact_conversation(self, config_dict: Dict) -> None:
        """Replace history with one user message containing an LLM summary (``/compact``)."""
        if not self._agent or not self._deps:
            self.stdio.print("Agent not initialized.\n")
            return
        if not self._history:
            self.stdio.print("No conversation to compact.\n")
            return
        ui = config_dict.get("ui", {})
        try:
            max_chars = int(ui.get("compact_transcript_max_chars", _COMPACT_TRANSCRIPT_TOTAL_MAX))
        except (TypeError, ValueError):
            max_chars = _COMPACT_TRANSCRIPT_TOTAL_MAX
        max_chars = max(10_000, min(max_chars, 500_000))
        transcript = _history_as_compact_transcript(self._history, max_total_chars=max_chars)
        prompt = (
            "Summarize the following obdiag-agent (OceanBase diagnostic) conversation into dense context "
            "for continuing the session in later turns.\n"
            "Preserve verbatim when critical: file paths, gather pack directories, cluster names and config paths, "
            "SQL and connection errors, obdiag command outcomes, OBProxy vs observer scope, and explicit user goals.\n"
            "Use concise bullets and short sections. Do not invent facts.\n\n"
            f"--- conversation transcript ---\n{transcript}"
        )
        self.stdio.print("")
        self.stdio.start_loading(_SPINNER_COMPACTING)
        try:
            with self._agent.override(
                toolsets=(),
                instructions="You only output the summary text. Do not call tools.",
            ):
                run_result = self._agent.run_sync(
                    prompt,
                    deps=self._deps,
                    message_history=None,
                )
            try:
                _accumulate_run_usage(self._cumulative_usage, run_result.usage())
            except Exception:
                pass
            summary = (run_result.output or "").strip()
        except Exception as e:
            self.stdio.stop_loading("failed")
            _clear_loading_line(self.stdio)
            self.stdio.print(f"Compact failed: {e}\n")
            self.stdio.error(f"compact: {e}")
            return
        self.stdio.stop_loading("succeed")
        _clear_loading_line(self.stdio)
        if not summary:
            self.stdio.print("Compact produced an empty summary; history unchanged.\n")
            return
        compact_msg = "[Compacted session — earlier turns were summarized to save context. " "Continue from this summary; re-run gathers or tools if you need fresh data.]\n\n" + summary
        self._history = [ModelRequest.user_text_prompt(compact_msg)]
        self._last_turn_peak_input_tokens = 0
        self.stdio.print(f"Compact done: history is now one summary message ({len(compact_msg)} characters). " "You can continue chatting; use ``save`` to persist the session.\n\n")

    def _maybe_auto_compact(self, config_dict: Dict) -> None:
        """If ui.context_window_tokens is set and last turn peak input_tokens exceeds threshold, run compact."""
        if not self._agent or not self._deps or not self._history:
            return
        ui = config_dict.get("ui", {})
        if ui.get("auto_compact", True) is False:
            return
        ctx = ui.get("context_window_tokens")
        try:
            ctx = int(ctx) if ctx is not None else 0
        except (TypeError, ValueError):
            return
        if ctx <= 0:
            return
        try:
            ratio = float(ui.get("auto_compact_threshold_ratio", 0.85))
        except (TypeError, ValueError):
            ratio = 0.85
        ratio = max(0.5, min(ratio, 0.99))
        try:
            min_msgs = int(ui.get("auto_compact_min_messages", 2))
        except (TypeError, ValueError):
            min_msgs = 2
        min_msgs = max(1, min_msgs)
        if len(self._history) < min_msgs:
            return
        threshold = int(ctx * ratio)
        peak = self._last_turn_peak_input_tokens
        if peak < threshold:
            return
        self.stdio.print(f"\n[auto-compact] Peak prompt input_tokens ({peak}) ≥ {ratio:.0%} of " f"context_window_tokens ({ctx}); summarizing session…\n")
        self._compact_conversation(config_dict)

    def _use_cluster(self, config_path: str):
        """Switch the active cluster for the current session."""
        if not self._deps:
            self.stdio.print("Agent not initialized.\n")
            return
        success, message = self._deps.switch_cluster(config_path)
        if success:
            self.stdio.print(f"✅ {message}\n")
            # Clear conversation history so the agent knows context changed.
            # Also reset session_id so the new empty history doesn't overwrite the
            # previous cluster's session file on the next auto-save.
            self._history.clear()
            self._session_id = None
            self._cumulative_usage = RunUsage()
            self._last_turn_usage = RunUsage()
            self._last_turn_peak_input_tokens = 0
            self.stdio.print("(Conversation history cleared for new cluster context)\n")
        else:
            self.stdio.print(f"❌ {message}\n")

    def _show_cluster(self):
        """Show the currently active cluster info."""
        if not self._deps:
            self.stdio.print("Agent not initialized.\n")
            return
        self.stdio.print(f"\n{self._deps.current_cluster_info()}\n")

    def _show_tools(self):
        """Dynamically list registered tools from the agent's toolsets."""
        if not self._agent:
            self.stdio.print("Agent not initialized.\n")
            return

        try:
            rows = _list_function_tools_from_agent(self._agent)
            extra = _non_function_toolset_count(self._agent)
            self.stdio.print(f"\nRegistered tools ({len(rows)}):\n")
            for name, first_line in rows:
                desc = f"  — {first_line}" if first_line else ""
                self.stdio.print(f"  • {name}{desc}")
            if extra:
                self.stdio.print(f"\n  ({extra} additional tool provider(s), e.g. MCP — " "tool names are resolved when the agent connects to the server.)\n")
            else:
                self.stdio.print("")
        except Exception:
            self.stdio.print("Unable to enumerate tools.\n")

    def _parse_builtin_command(self, user_input: str) -> Optional[tuple[str, Optional[str]]]:
        """Parse interactive built-ins.

        Only slash-prefixed lines (``/help``, ``/use obdiag_test``) are commands; everything else is chat.

        Returns:
            ``(command, arg)`` if this line is a built-in, else ``None`` (treat as chat).
            ``command`` is lowercased; ``arg`` preserves original casing when present.
        """
        s = user_input.strip()
        if not s or not s.startswith("/"):
            return None

        inner = s[1:].strip()
        if not inner:
            self.stdio.print("Commands start with /. Type /help for a list.\n")
            return ("_builtin_empty_slash", None)
        parts = inner.split(None, 1)
        cmd = parts[0].lower()
        arg = parts[1].strip() if len(parts) > 1 else None
        return (cmd, arg)

    def _show_history(self):
        if not self._history:
            self.stdio.print("No conversation history.\n")
            return
        self.stdio.print("\n=== Conversation History ===\n")
        idx = 0
        for msg in self._history:
            kind = getattr(msg, "kind", None)
            if kind is None and isinstance(msg, dict):
                kind = msg.get("kind")
            parts = getattr(msg, "parts", None)
            if parts is None and isinstance(msg, dict):
                parts = msg.get("parts") or ()

            if kind in ("request", "response") and parts:
                for p in parts:
                    pk = getattr(p, "part_kind", None)
                    if pk is None and isinstance(p, dict):
                        pk = p.get("part_kind")
                    if kind == "request" and pk == "user-prompt":
                        idx += 1
                        raw = getattr(p, "content", "") if not isinstance(p, dict) else p.get("content", "")
                        text = _user_prompt_part_as_text(raw)
                        display = text[:500] + "..." if len(text) > 500 else text
                        self.stdio.print(f"[{idx}] User: {display}\n")
                    elif kind == "request" and pk in ("tool-return", "builtin-tool-return"):
                        idx += 1
                        name = getattr(p, "tool_name", None) if not isinstance(p, dict) else p.get("tool_name")
                        name = name or "tool"
                        body = _tool_return_part_body(p)
                        if len(body) > 400:
                            body = body[:397] + "..."
                        self.stdio.print(f"[{idx}] Tool ({name}): {body}\n")
                    elif kind == "response" and pk == "text":
                        idx += 1
                        t = getattr(p, "content", "") if not isinstance(p, dict) else (p.get("content") or "")
                        t = str(t)
                        display = t[:400] + "..." if len(t) > 400 else t
                        self.stdio.print(f"[{idx}] Assistant: {display}\n")
                    elif kind == "response" and pk in ("tool-call", "builtin-tool-call"):
                        idx += 1
                        name = getattr(p, "tool_name", None) if not isinstance(p, dict) else p.get("tool_name")
                        name = name or "?"
                        args = getattr(p, "args", None) if not isinstance(p, dict) else p.get("args")
                        ap = _preview_tool_args(args) if args else ""
                        self.stdio.print(f"[{idx}] Assistant → tool {name}{ap}\n")
                continue

            # Legacy / unusual shapes
            if hasattr(msg, "role"):
                role = msg.role
                content = getattr(msg, "content", str(msg))
            elif isinstance(msg, dict) and msg.get("role"):
                role = msg.get("role", "unknown")
                content = msg.get("content", "")
            else:
                continue
            idx += 1
            if role == "user":
                self.stdio.print(f"[{idx}] User: {content}\n")
            elif role in ("assistant", "model-text-response"):
                display = str(content)[:200] + "..." if len(str(content)) > 200 else content
                self.stdio.print(f"[{idx}] Assistant: {display}\n")
        if idx == 0:
            self.stdio.print("(No displayable messages; history may use an unsupported shape.)\n")
        self.stdio.print("===========================\n")

    # ------------------------------------------------------------------
    # Main entry points
    # ------------------------------------------------------------------

    def handle(self):
        """Main handler — interactive or single-shot."""
        try:
            config_dict = self._load_config()

            single_shot_message = self._get_single_shot_message()
            if single_shot_message:
                return self._handle_single_shot(config_dict, single_shot_message)

            self._show_welcome(config_dict)
            self.stdio.verbose("Initializing agent...")
            self._init_agent(config_dict)
            self.stdio.verbose("Agent initialized successfully")

            # Resume session if requested
            resume_id = Util.get_option(self.options, "resume")
            if resume_id:
                if self._load_session(str(resume_id).strip()):
                    self.stdio.print(f"Resumed session: {self._session_id}  ({len(self._history)} messages)\n")
                else:
                    self.stdio.print("Starting new session.\n")

            ui = config_dict.get("ui", {})
            prompt = ui.get("prompt", "obdiag agent> ")

            while True:
                try:
                    if PROMPT_TOOLKIT_AVAILABLE:
                        pt_kw: Dict[str, Any] = {"history": self.input_history}
                        if _builtin_slash_completer is not None:
                            pt_kw["completer"] = _builtin_slash_completer
                            pt_kw["complete_style"] = CompleteStyle.COLUMN
                            pt_kw["complete_while_typing"] = True
                        user_input = pt_prompt(prompt, **pt_kw).strip()
                    else:
                        user_input = input(prompt).strip()

                    if not user_input:
                        continue

                    parsed = self._parse_builtin_command(user_input)
                    if parsed is not None:
                        cmd, cmd_arg = parsed
                        if cmd == "_builtin_empty_slash":
                            continue
                        if cmd in ("exit", "quit", "q"):
                            self._save_session()
                            if self._session_id:
                                self.stdio.print(f"\nSession saved: {self._session_id}")
                                self.stdio.print(f"Resume with: obdiag agent --resume {self._session_id}")
                            self.stdio.print("\nGoodbye! Have a nice day!\n")
                            break
                        if cmd in ("help", "?"):
                            self._show_help()
                            continue
                        if cmd == "clear":
                            self._history.clear()
                            self._session_id = None
                            self._cumulative_usage = RunUsage()
                            self._last_turn_usage = RunUsage()
                            self._last_turn_peak_input_tokens = 0
                            self.stdio.print("Conversation history cleared.\n")
                            continue
                        if cmd == "compact":
                            self._compact_conversation(config_dict)
                            continue
                        if cmd == "history":
                            self._show_history()
                            continue
                        if cmd == "tools":
                            self._show_tools()
                            continue
                        if cmd == "cluster":
                            self._show_cluster()
                            continue
                        if cmd == "use":
                            if not cmd_arg:
                                self.stdio.print("Usage: /use <short_name|path>  (e.g. /use obdiag_test)\n")
                                continue
                            self._use_cluster(cmd_arg)
                            continue
                        if cmd == "save":
                            self._save_session()
                            if self._session_id:
                                self.stdio.print(f"Session saved: {self._session_id}\n")
                            else:
                                self.stdio.print("No history to save.\n")
                            continue
                        if cmd == "sessions":
                            self._list_sessions()
                            continue
                        if cmd == "usage":
                            self._show_usage_command(config_dict)
                            continue
                        self.stdio.print(f"Unknown command: /{cmd}. Type /help for a list.\n")
                        continue

                    self.stdio.print("")
                    self.stdio.start_loading(_SPINNER_THINKING)
                    try:
                        response, was_streamed = self._chat_sync(user_input, config_dict)
                        self.stdio.stop_loading("succeed")
                        _clear_loading_line(self.stdio)
                        if not was_streamed:
                            self._render_markdown(response)
                        self.stdio.print("")
                        if config_dict.get("ui", {}).get("show_usage_after_turn", False):
                            self.stdio.print(self._format_usage_line(config_dict))
                            self.stdio.print("")
                        self._maybe_auto_compact(config_dict)
                        self._save_session()
                    except Exception as e:
                        self.stdio.stop_loading("failed")
                        err_msg = str(e)
                        if hasattr(e, "__cause__") and e.__cause__ is not None:
                            err_msg += f"\n  Cause: {e.__cause__}"
                        self.stdio.print(f"\rError: {err_msg}\n")
                        self.stdio.error(f"Failed to get agent response: {e}")

                except KeyboardInterrupt:
                    self.stdio.print("\n\nInterrupted. Type /exit to quit.\n")
                except EOFError:
                    self._save_session()
                    self.stdio.print("\n\nGoodbye!\n")
                    break

            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Agent session ended"})

        except Exception as e:
            self.stdio.error(f"Agent error: {e}")
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=f"Agent error: {e}")
        finally:
            if self._deps:
                try:
                    self._deps.close()
                except Exception:
                    pass

    def _handle_single_shot(self, config_dict: Dict, message: str):
        """Run agent once with the given message and exit."""
        try:
            self.stdio.verbose("Initializing agent...")
            self._init_agent(config_dict)
            self.stdio.verbose("Agent initialized successfully")

            resume_id = Util.get_option(self.options, "resume")
            if resume_id:
                if self._load_session(str(resume_id).strip()):
                    self.stdio.verbose(f"Resumed session: {self._session_id} ({len(self._history)} messages)")

            self.stdio.start_loading(_SPINNER_THINKING)
            try:
                response, was_streamed = self._chat_sync(message, config_dict)
                self.stdio.stop_loading("succeed")
                _clear_loading_line(self.stdio)
                if not was_streamed:
                    self._render_markdown(response)
                self.stdio.print("")
                if config_dict.get("ui", {}).get("show_usage_after_turn", False):
                    self.stdio.print(self._format_usage_line(config_dict))
                    self.stdio.print("")
                self._maybe_auto_compact(config_dict)
                self._save_session()
                if self._session_id:
                    self.stdio.print(f"\nSession saved: {self._session_id}")
                    self.stdio.print(f"Resume with: obdiag agent --resume {self._session_id} -m \"<message>\" --yolo\n")
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Agent completed"})
            except Exception as e:
                self.stdio.stop_loading("failed")
                err_msg = str(e)
                if hasattr(e, "__cause__") and e.__cause__ is not None:
                    err_msg += f"\n  Cause: {e.__cause__}"
                self.stdio.print(f"\rError: {err_msg}\n")
                self.stdio.error(f"Failed to get agent response: {e}")
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=err_msg)
        except Exception as e:
            self.stdio.error(f"Agent error: {e}")
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=f"Agent error: {e}")
        finally:
            if self._deps:
                try:
                    self._deps.close()
                except Exception:
                    pass

    def _get_single_shot_message(self) -> Optional[str]:
        message = Util.get_option(self.options, "m") or Util.get_option(self.options, "message")
        if message:
            return str(message).strip() or None
        return None


# Backward compatibility alias
AiAssistantHandler = AiAgentHandler
