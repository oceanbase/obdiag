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

import os
from typing import Any, Dict, List, Optional

from pydantic_ai import Agent

from src.common.result_type import ObdiagResult
from src.common.tool import Util
from src.handler.agent.agent import create_agent
from src.handler.agent.config import (
    load_agent_config,
    OBDIAG_CONFIG_PATH,
    resolve_cluster_config_path,
)
from src.handler.agent.models import AgentConfig, AgentDependencies

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

        self.console = Console() if RICH_AVAILABLE else None
        self.input_history = InMemoryHistory() if PROMPT_TOOLKIT_AVAILABLE else None

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _load_config(self) -> Dict:
        """Load agent configuration from disk."""
        return load_agent_config(stdio=self.stdio)

    def _init_agent(self, config_dict: Dict):
        """Build the agent and dependencies from the config dictionary."""
        config = AgentConfig.from_dict(config_dict)

        api_key = config.api_key or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OpenAI API key is required. "
                "Set OPENAI_API_KEY or configure it in ~/.obdiag/ai.yml"
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

        self._deps = AgentDependencies(
            cluster_config=cluster_config,
            stdio=self.stdio,
            config_path=obdiag_config_path,
        )

        self._agent = create_agent(config, self.stdio)

    # ------------------------------------------------------------------
    # Chat
    # ------------------------------------------------------------------

    def _chat_sync(self, message: str) -> str:
        """Run a single synchronous turn, managing history."""
        result = self._agent.run_sync(
            message,
            deps=self._deps,
            message_history=self._history or None,
        )
        self._history = list(result.new_messages())
        if len(self._history) > MAX_HISTORY_MESSAGES:
            self._history = self._history[-MAX_HISTORY_MESSAGES:]
        return result.output

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
        self.stdio.print("""
Available commands:
  help, ?                  - Show this help message
  exit, quit, q            - Exit the agent
  clear                    - Clear conversation history
  history                  - Show conversation history
  tools                    - List available diagnostic tools
  use <name|path>          - Switch active cluster (e.g., use obdiag_test or use /path/to/config.yml)
  cluster                  - Show current active cluster info

Cluster: When not specified, ~/.obdiag/config.yml is used. Short names (e.g., obdiag_test for
~/.obdiag/obdiag_test.yml) are supported: use obdiag_test, or pass cluster_config_path="obdiag_test".

You can also ask me questions in natural language, such as:
  - "帮我检查数据库的健康状态"
  - "收集最近1小时的日志"
  - "分析日志中的错误信息"
  - "检查IO性能"
  - "执行根因分析"
  - "帮我生成obdiag配置文件"
  - "对 obdiag_test 集群巡检"  (agent 会传递 cluster_config_path="obdiag_test")
""")

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
                        self.stdio.print("\nGoodbye! Have a nice day!\n")
                        break
                    elif cmd in ("help", "?"):
                        self._show_help()
                        continue
                    elif cmd == "clear":
                        self._history.clear()
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

                    self.stdio.print("")
                    self.stdio.start_loading("Thinking...")
                    try:
                        response = self._chat_sync(user_input)
                        self.stdio.stop_loading("succeed")
                        self.stdio.print("\r" + " " * 20 + "\r", end="")
                        self._render_markdown(response)
                        self.stdio.print("")
                    except Exception as e:
                        self.stdio.stop_loading("failed")
                        self.stdio.print(f"\rError: {e}\n")
                        self.stdio.error(f"Failed to get agent response: {e}")

                except KeyboardInterrupt:
                    self.stdio.print("\n\nInterrupted. Type 'exit' to quit.\n")
                except EOFError:
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

            self.stdio.start_loading("Thinking...")
            try:
                response = self._chat_sync(message)
                self.stdio.stop_loading("succeed")
                self.stdio.print("\r" + " " * 20 + "\r", end="")
                self._render_markdown(response)
                self.stdio.print("")
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Agent completed"})
            except Exception as e:
                self.stdio.stop_loading("failed")
                self.stdio.error(f"Failed to get agent response: {e}")
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=str(e))
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
