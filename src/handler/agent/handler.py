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

import json
import os
import time
from typing import Any, Dict, List, Optional

from pydantic_ai import Agent
from pydantic_ai.messages import ModelMessagesTypeAdapter
from pydantic_ai.output import DeferredToolRequests
from pydantic_ai.tools import DeferredToolResults, ToolDenied

from src.common.result_type import ObdiagResult
from src.common.tool import Util
from src.handler.agent.agent import create_agent
from src.handler.agent.config import (
    load_agent_config,
    OBDIAG_CONFIG_PATH,
    resolve_cluster_config_path,
)
from src.handler.agent.models import AgentConfig, AgentDependencies

SESSIONS_DIR = os.path.expanduser("~/.obdiag/sessions")

try:
    from rich.console import Console
    from rich.markdown import Markdown

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    from prompt_toolkit import prompt as pt_prompt
    from prompt_toolkit.history import InMemoryHistory

    PROMPT_TOOLKIT_AVAILABLE = True
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False


MAX_HISTORY_MESSAGES = 20
# Limit for resumed sessions to reduce Qwen API "tool_call_id" errors with long tool chains
MAX_HISTORY_WHEN_RESUMED = 10

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
    return all(
        getattr(p, "part_kind", None) in _TOOL_ROLE_PART_KINDS
        for p in parts
    )


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
    Strips tool messages and assistant-with-tool_calls to avoid API errors
    (e.g. Qwen 'Tool call id not found in messages').
    """
    if not history:
        return []
    # Filter out any message with tool-related content
    filtered: List[Any] = []
    for msg in history:
        if _message_has_any_tool_content(msg):
            continue
        filtered.append(msg)
    if len(filtered) <= max_messages:
        return filtered
    truncated = list(filtered[-max_messages:])
    # Drop leading until we hit a user message
    while truncated and not _is_user_message(truncated[0]):
        truncated.pop(0)
    return truncated


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
║  Type 'exit' or 'quit' to end the session.                               ║
║  Type 'help' for more information.                                       ║
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
        """Save current history to disk."""
        if not self._history:
            return
        self._ensure_sessions_dir()
        if not self._session_id:
            self._session_id = self._generate_session_id()
        path = self._session_path(self._session_id)
        try:
            data = ModelMessagesTypeAdapter.dump_json(self._history)
            with open(path, "wb") as f:
                f.write(data)
        except Exception as e:
            self.stdio.verbose(f"Failed to save session: {e}")

    def _load_session(self, session_id: str) -> bool:
        """Load history from a saved session. Returns True on success."""
        path = self._session_path(session_id)
        if not os.path.exists(path):
            self.stdio.print(f"Session not found: {session_id}\n")
            return False
        try:
            with open(path, "rb") as f:
                data = f.read()
            loaded = list(ModelMessagesTypeAdapter.validate_json(data))
            self._history = _truncate_history_safe(loaded, MAX_HISTORY_WHEN_RESUMED)
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
            raise ValueError("OpenAI API key is required. " "Set OPENAI_API_KEY or configure it in ~/.obdiag/config/agent.yml")
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

        self._deps = AgentDependencies(
            cluster_config=cluster_config,
            stdio=self.stdio,
            config_path=obdiag_config_path,
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

        output_type = [str, DeferredToolRequests]
        history = _truncate_history_safe(list(self._history), MAX_HISTORY_MESSAGES) if self._history else None
        if history is not None and len(history) == 0:
            history = None
        deferred_results = None

        while True:
            if stream_output:
                run_result = self._agent.run_stream_sync(
                    message if deferred_results is None else "Continue",
                    output_type=output_type,
                    deps=self._deps,
                    message_history=history,
                    deferred_tool_results=deferred_results,
                )
                output = run_result.output
            else:
                run_result = self._agent.run_sync(
                    message if deferred_results is None else "Continue",
                    output_type=output_type,
                    deps=self._deps,
                    message_history=history,
                    deferred_tool_results=deferred_results,
                )
                output = run_result.output

            if isinstance(output, DeferredToolRequests):
                # Stop loading spinner so the approval prompt is visible
                self.stdio.stop_loading("succeed")
                self.stdio.print("\r" + " " * 20 + "\r", end="")
                # Gather approvals
                deferred_results = DeferredToolResults()
                for call in output.approvals:
                    tool_name = call.tool_name
                    args = call.args
                    call_id = call.tool_call_id
                    if tool_approval:
                        self.stdio.print(f"\n[Tool] {tool_name}({args})")
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
                self.stdio.start_loading("Executing...")
                continue
            else:
                was_streamed = False
                if stream_output and hasattr(run_result, "stream_text"):
                    self.stdio.print("")
                    for chunk in run_result.stream_text(delta=True):
                        self.stdio.print(chunk, end="")
                    self.stdio.print("\n")
                    was_streamed = True
                self._history = _truncate_history_safe(
                    list(run_result.all_messages()), MAX_HISTORY_MESSAGES
                )
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
Available commands:
  help, ?                  - Show this help message
  exit, quit, q            - Exit the agent (auto-saves session)
  clear                    - Clear conversation history
  history                  - Show conversation history
  tools                    - List available diagnostic tools
  use <name|path>          - Switch active cluster (e.g., use obdiag_test or use /path/to/config.yml)
  cluster                  - Show current active cluster info
  save                     - Save current session
  sessions                 - List saved sessions

Cluster: When not specified, ~/.obdiag/config.yml is used. Short names (e.g., obdiag_test for
~/.obdiag/obdiag_test.yml) are supported: use obdiag_test, or pass cluster_config_path="obdiag_test".

Session: conversations are auto-saved on exit. Resume with: obdiag agent --resume <session_id>

Configure tool_approval and stream_output in ~/.obdiag/config/agent.yml (ui section):
  - tool_approval: prompt before SQL and bash execution; y/n/all/deny
  - stream_output: stream LLM response as it arrives

You can also ask me questions in natural language, such as:
  - "帮我检查数据库的健康状态"
  - "收集最近1小时的日志"
  - "分析日志中的错误信息"
  - "检查IO性能"
  - "执行根因分析"
  - "帮我生成obdiag配置文件"
  - "对 obdiag_test 集群巡检"  (agent 会传递 cluster_config_path="obdiag_test")
"""
        )

    def _use_cluster(self, config_path: str):
        """Switch the active cluster for the current session."""
        if not self._deps:
            self.stdio.print("Agent not initialized.\n")
            return
        success, message = self._deps.switch_cluster(config_path)
        if success:
            self.stdio.print(f"✅ {message}\n")
            # Clear conversation history so the agent knows context changed
            self._history.clear()
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
            tool_names = sorted(self._agent._function_tools.keys())
            self.stdio.print(f"\nRegistered tools ({len(tool_names)}):\n")
            for name in tool_names:
                desc = ""
                tool_obj = self._agent._function_tools.get(name)
                if tool_obj and hasattr(tool_obj, "description"):
                    first_line = (tool_obj.description or "").split("\n")[0].strip()
                    desc = f"  — {first_line}" if first_line else ""
                self.stdio.print(f"  • {name}{desc}")
            self.stdio.print("")
        except Exception:
            self.stdio.print("Unable to enumerate tools.\n")

    def _show_history(self):
        if not self._history:
            self.stdio.print("No conversation history.\n")
            return
        self.stdio.print("\n=== Conversation History ===\n")
        for i, msg in enumerate(self._history, 1):
            if hasattr(msg, "role"):
                role = msg.role
                content = getattr(msg, "content", str(msg))
            elif isinstance(msg, dict):
                role = msg.get("role", "unknown")
                content = msg.get("content", "")
            else:
                continue
            if role == "user":
                self.stdio.print(f"[{i}] User: {content}\n")
            elif role in ("assistant", "model-text-response"):
                display = content[:200] + "..." if len(str(content)) > 200 else content
                self.stdio.print(f"[{i}] Assistant: {display}\n")
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

            self._show_loaded_tools()

            ui = config_dict.get("ui", {})
            prompt = ui.get("prompt", "obdiag agent> ")

            while True:
                try:
                    if PROMPT_TOOLKIT_AVAILABLE:
                        user_input = pt_prompt(prompt, history=self.input_history).strip()
                    else:
                        user_input = input(prompt).strip()

                    if not user_input:
                        continue

                    cmd = user_input.lower()
                    if cmd in ("exit", "quit", "q"):
                        self._save_session()
                        if self._session_id:
                            self.stdio.print(f"\nSession saved: {self._session_id}")
                            self.stdio.print(f"Resume with: obdiag agent --resume {self._session_id}")
                        self.stdio.print("\nGoodbye! Have a nice day!\n")
                        break
                    elif cmd in ("help", "?"):
                        self._show_help()
                        continue
                    elif cmd == "clear":
                        self._history.clear()
                        self._session_id = None
                        self.stdio.print("Conversation history cleared.\n")
                        continue
                    elif cmd == "history":
                        self._show_history()
                        continue
                    elif cmd == "tools":
                        self._show_tools()
                        continue
                    elif cmd == "cluster":
                        self._show_cluster()
                        continue
                    elif cmd.startswith("use "):
                        self._use_cluster(user_input[4:].strip())
                        continue
                    elif cmd == "save":
                        self._save_session()
                        if self._session_id:
                            self.stdio.print(f"Session saved: {self._session_id}\n")
                        else:
                            self.stdio.print("No history to save.\n")
                        continue
                    elif cmd == "sessions":
                        self._list_sessions()
                        continue

                    self.stdio.print("")
                    self.stdio.start_loading("Thinking...")
                    try:
                        response, was_streamed = self._chat_sync(user_input, config_dict)
                        self.stdio.stop_loading("succeed")
                        self.stdio.print("\r" + " " * 20 + "\r", end="")
                        if not was_streamed:
                            self._render_markdown(response)
                        self.stdio.print("")
                        self._save_session()
                    except Exception as e:
                        self.stdio.stop_loading("failed")
                        err_msg = str(e)
                        if hasattr(e, "__cause__") and e.__cause__ is not None:
                            err_msg += f"\n  Cause: {e.__cause__}"
                        self.stdio.print(f"\rError: {err_msg}\n")
                        self.stdio.error(f"Failed to get agent response: {e}")

                except KeyboardInterrupt:
                    self.stdio.print("\n\nInterrupted. Type 'exit' to quit.\n")
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

            self.stdio.start_loading("Thinking...")
            try:
                response, was_streamed = self._chat_sync(message, config_dict)
                self.stdio.stop_loading("succeed")
                self.stdio.print("\r" + " " * 20 + "\r", end="")
                if not was_streamed:
                    self._render_markdown(response)
                self.stdio.print("")
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

    def _show_loaded_tools(self):
        if not self._agent:
            return
        try:
            tool_names = sorted(self._agent._function_tools.keys())
            self.stdio.print(f"📦 Loaded {len(tool_names)} tools:")
            for name in tool_names:
                self.stdio.print(f"   • {name}")
            self.stdio.print("")
        except Exception:
            pass


# Backward compatibility alias
AiAssistantHandler = AiAgentHandler
