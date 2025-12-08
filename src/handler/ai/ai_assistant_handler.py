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
@time: 2025/12/08
@file: ai_assistant_handler.py
@desc: AI Assistant interactive handler
"""

import os
import json
import yaml
from typing import Dict, List, Optional

from src.handler.ai.openai_client import ObdiagAIClient
from src.common.tool import Util
from src.common.result_type import ObdiagResult

# Rich library for Markdown rendering in terminal
try:
    from rich.console import Console
    from rich.markdown import Markdown
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class AiAssistantHandler:
    """AI Assistant interactive handler"""

    BETA_WARNING = """
╔══════════════════════════════════════════════════════════════════════════╗
║                          ⚠️  BETA FEATURE WARNING  ⚠️                      ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  This is a BETA feature and may change in future versions.              ║
║  Compatibility with previous versions is not guaranteed.                 ║
║                                                                          ║
║  If you encounter any issues, please report them at:                     ║
║  https://github.com/oceanbase/obdiag/issues                              ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

    WELCOME_MESSAGE = """
╔══════════════════════════════════════════════════════════════════════════╗
║                    obdiag AI Assistant                                   ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║  Welcome! I'm your AI assistant for OceanBase diagnostics.               ║
║  You can ask me to:                                                      ║
║    - Collect diagnostic information (logs, perf, sysstat)                ║
║    - Analyze logs and performance data                                   ║
║    - Run health checks                                                   ║
║    - Perform root cause analysis                                         ║
║    - And much more...                                                    ║
║                                                                          ║
║  Type 'exit' or 'quit' to end the session.                               ║
║  Type 'help' for more information.                                       ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

    # AI config file path
    AI_CONFIG_PATH = os.path.expanduser("~/.obdiag/ai.yml")

    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.options = context.options
        self.ai_client = None
        self.conversation_history: List[Dict] = []
        
        # Initialize Rich console for Markdown rendering
        if RICH_AVAILABLE:
            self.console = Console()
        else:
            self.console = None

    def _load_config(self) -> Dict:
        """
        Load AI assistant configuration from ~/.obdiag/ai.yml
        
        Config file path: ~/.obdiag/ai.yml
        """
        # Default MCP servers configuration
        default_mcp_servers = {
            "obdiag": {
                "command": "obdiag-mcp",
                "args": ["stdio"]
            }
        }
        
        # Default configuration
        default_config = {
            "llm": {
                "api_type": "openai",
                "api_key": os.getenv("OPENAI_API_KEY", ""),
                "base_url": os.getenv("OPENAI_BASE_URL", ""),
                "model": "gpt-4",
                "temperature": 0.7,
                "max_tokens": 2000,
            },
            "mcp": {
                "enabled": True,
                "servers": default_mcp_servers,
            },
            "ui": {
                "show_welcome": True,
                "show_beta_warning": True,
                "clear_screen": True,
                "prompt": "obdiag AI> ",
            },
        }

        # Try to load config from ~/.obdiag/ai.yml
        ai_config = {}
        config_path = self.AI_CONFIG_PATH
        
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    ai_config = yaml.safe_load(f) or {}
                self.stdio.verbose(f"Loaded AI config from {config_path}")
            except Exception as e:
                self.stdio.warn(f"Failed to load AI config from {config_path}: {e}")
        else:
            self.stdio.warn(f"AI config file not found: {config_path}")
            self.stdio.warn("Using default configuration. Please create ~/.obdiag/ai.yml for custom settings.")

        # Merge with user configuration
        llm_config = {**default_config["llm"], **ai_config.get("llm", {})}
        ui_config = {**default_config["ui"], **ai_config.get("ui", {})}
        
        # Handle MCP configuration
        mcp_config = {**default_config["mcp"]}
        user_mcp_config = ai_config.get("mcp", {})
        
        if "enabled" in user_mcp_config:
            mcp_config["enabled"] = user_mcp_config["enabled"]
        
        # Parse MCP servers - supports JSON string format
        if "servers" in user_mcp_config:
            servers_value = user_mcp_config["servers"]
            if isinstance(servers_value, str):
                # JSON string format
                try:
                    mcp_config["servers"] = json.loads(servers_value)
                except json.JSONDecodeError as e:
                    self.stdio.warn(f"Failed to parse MCP servers JSON: {e}")
                    mcp_config["servers"] = default_mcp_servers
            elif isinstance(servers_value, dict):
                # Direct dict format
                mcp_config["servers"] = servers_value
            else:
                self.stdio.warn("Invalid MCP servers format, using default")
                mcp_config["servers"] = default_mcp_servers

        return {
            "llm": llm_config,
            "mcp": mcp_config,
            "ui": ui_config,
        }

    def _init_ai_client(self, config: Dict):
        """Initialize AI client"""
        llm_config = config["llm"]
        mcp_config = config["mcp"]

        # Check API key
        api_key = llm_config.get("api_key") or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError(
                "OpenAI API key is required. "
                "Please set OPENAI_API_KEY environment variable or configure it in ~/.obdiag/ai.yml"
            )

        # Get base URL
        base_url = llm_config.get("base_url") or os.getenv("OPENAI_BASE_URL") or None

        # Get config path
        config_path = Util.get_option(self.options, "c") or os.path.expanduser("~/.obdiag/config.yml")

        # Get MCP settings
        use_mcp = mcp_config.get("enabled", True)
        mcp_servers = mcp_config.get("servers", {})

        # Get system prompt (use None for default)
        system_prompt = llm_config.get("system_prompt") or None

        # Initialize AI client
        self.ai_client = ObdiagAIClient(
            api_key=api_key,
            base_url=base_url,
            model=llm_config.get("model", "gpt-4"),
            config_path=config_path,
            use_mcp=use_mcp,
            mcp_servers=mcp_servers,
            temperature=llm_config.get("temperature", 0.7),
            max_tokens=llm_config.get("max_tokens", 2000),
            system_prompt=system_prompt,
        )

    def _clear_screen(self):
        """Clear screen"""
        os.system("clear" if os.name != "nt" else "cls")

    def _render_markdown(self, text: str):
        """
        Render text as Markdown in terminal
        
        Args:
            text: The text to render (may contain Markdown formatting)
        """
        if RICH_AVAILABLE and self.console:
            try:
                md = Markdown(text)
                self.console.print(md)
            except Exception as e:
                # Fallback to plain text if rendering fails
                self.stdio.verbose(f"Markdown rendering failed: {e}")
                self.stdio.print(text)
        else:
            # Fallback to plain text if rich is not available
            self.stdio.print(text)

    def _show_welcome(self, config: Dict):
        """Show welcome message and beta warning"""
        ui_config = config["ui"]

        if ui_config.get("clear_screen", True):
            self._clear_screen()

        if ui_config.get("show_beta_warning", True):
            self.stdio.print(self.BETA_WARNING)
            self.stdio.print("")

        if ui_config.get("show_welcome", True):
            self.stdio.print(self.WELCOME_MESSAGE)
            self.stdio.print("")

    def _show_help(self):
        """Show help information"""
        help_text = """
Available commands:
  help, ?          - Show this help message
  exit, quit, q    - Exit the AI assistant
  clear            - Clear conversation history
  history          - Show conversation history
  tools            - List available diagnostic tools

You can also ask me questions in natural language, such as:
  - "帮我检查数据库的健康状态"
  - "收集最近1小时的日志"
  - "分析日志中的错误信息"
  - "检查IO性能"
  - "执行根因分析"
"""
        self.stdio.print(help_text)

    def _show_tools(self):
        """Show available tools"""
        tools_text = """
Available diagnostic tools:

📦 Information Gathering:
  - gather_log         : Collect OceanBase observer logs
  - gather_sysstat     : Collect system statistics
  - gather_perf        : Collect performance data (flame graph, pstack)
  - gather_obproxy_log : Collect OBProxy logs
  - gather_ash         : Generate ASH (Active Session History) report
  - gather_awr         : Collect AWR data

🔍 Analysis:
  - analyze_log        : Analyze OceanBase logs for errors/warnings

✅ Health Check:
  - check              : Run health checks on OceanBase cluster
  - check_list         : List available check tasks

🔎 Root Cause Analysis:
  - rca_run            : Run root cause analysis for specific scenarios
  - rca_list           : List available RCA scenarios

🛠️ Tools:
  - tool_io_performance: Check disk IO performance
"""
        self.stdio.print(tools_text)

    def _show_loaded_tools(self):
        """Show loaded MCP tools information"""
        try:
            # Check if using MCP
            if self.ai_client and self.ai_client.mcp_client and self.ai_client.mcp_client.is_connected():
                # Get connected servers info
                connected_servers = self.ai_client.mcp_client.get_connected_servers()
                servers_info = self.ai_client.mcp_client.get_server_info()
                
                print(f"🔌 MCP Servers ({len(connected_servers)} connected):")
                for server_name in connected_servers:
                    info = servers_info.get(server_name, {})
                    version = info.get("version", "unknown")
                    print(f"   • {server_name} (v{version})")
                
                # List all tools
                tools = self.ai_client.mcp_client.list_tools()
                print(f"\n📦 Loaded {len(tools)} tools via MCP protocol:")
                for tool in tools:
                    tool_name = tool.get("name", "")
                    print(f"   • {tool_name}")
                print("")
            elif self.ai_client and self.ai_client.executor:
                tools = self.ai_client.executor.list_tools()
                print(f"📦 Loaded {len(tools)} tools via built-in executor:")
                for tool_name in tools:
                    print(f"   • {tool_name}")
                print("")
        except Exception as e:
            self.stdio.verbose(f"Failed to show loaded tools: {e}")

    def _show_history(self):
        """Show conversation history"""
        if not self.conversation_history:
            self.stdio.print("No conversation history.\n")
            return

        self.stdio.print("\n=== Conversation History ===\n")
        for i, msg in enumerate(self.conversation_history, 1):
            role = msg.get("role", "unknown")
            content = msg.get("content", "")
            if role == "user":
                self.stdio.print(f"[{i}] User: {content}\n")
            elif role == "assistant":
                display_content = content[:200] + "..." if len(content) > 200 else content
                self.stdio.print(f"[{i}] Assistant: {display_content}\n")
        self.stdio.print("===========================\n")

    def handle(self):
        """Main handler method"""
        try:
            # Load configuration
            config = self._load_config()

            # Show welcome and warning
            self._show_welcome(config)

            # Initialize AI client
            self.stdio.verbose("Initializing AI client...")
            self._init_ai_client(config)
            self.stdio.verbose("AI client initialized successfully")

            # Show loaded tools info
            self._show_loaded_tools()

            # Interactive loop
            ui_config = config["ui"]
            prompt = ui_config.get("prompt", "obdiag AI> ")

            while True:
                try:
                    # Get user input
                    user_input = input(prompt).strip()

                    if not user_input:
                        continue

                    # Handle special commands
                    if user_input.lower() in ["exit", "quit", "q"]:
                        print("\nGoodbye! Have a nice day!\n")
                        break
                    elif user_input.lower() in ["help", "?"]:
                        self._show_help()
                        continue
                    elif user_input.lower() == "clear":
                        self.conversation_history = []
                        print("Conversation history cleared.\n")
                        continue
                    elif user_input.lower() == "history":
                        self._show_history()
                        continue
                    elif user_input.lower() == "tools":
                        self._show_tools()
                        continue

                    # Process user input with AI
                    print("")  # New line
                    print("Thinking...", end="", flush=True)

                    try:
                        response = self.ai_client.chat(user_input, self.conversation_history)
                        print("\r" + " " * 20 + "\r", end="")  # Clear "Thinking..."
                        
                        # Render response as Markdown
                        self._render_markdown(response)
                        print("")  # New line after response

                        # Update conversation history
                        self.conversation_history.append({"role": "user", "content": user_input})
                        self.conversation_history.append({"role": "assistant", "content": response})

                        # Limit history size to prevent context overflow
                        if len(self.conversation_history) > 20:
                            self.conversation_history = self.conversation_history[-20:]

                    except Exception as e:
                        print(f"\rError: {str(e)}\n")
                        self.stdio.error(f"Failed to get AI response: {str(e)}")

                except KeyboardInterrupt:
                    print("\n\nInterrupted. Type 'exit' to quit.\n")
                except EOFError:
                    print("\n\nGoodbye!\n")
                    break

            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "AI assistant session ended"})

        except Exception as e:
            self.stdio.error(f"AI assistant error: {str(e)}")
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=f"AI assistant error: {str(e)}")
        finally:
            # Cleanup
            if self.ai_client:
                try:
                    self.ai_client.close()
                except Exception:
                    pass

