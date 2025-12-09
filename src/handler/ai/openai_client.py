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
@file: openai_client.py
@desc: OpenAI API client for AI Assistant with MCP support
"""

import json
from typing import Dict, List, Optional, Any, Generator
from openai import OpenAI


from src.handler.ai.obdiag_executor import ObdiagExecutor
from src.handler.ai.mcp_client import MCPClientManager


class ObdiagAIClient:
    """
    OpenAI API client for obdiag AI Assistant.
    Supports tool calling via MCP (multiple servers) or built-in executor.
    """

    SYSTEM_PROMPT = """You are obdiag AI Assistant, an intelligent diagnostic assistant for OceanBase database.

Your capabilities include:
1. Executing obdiag diagnostic commands (gather logs, analyze, check health, RCA)
2. Analyzing diagnostic results and providing insights
3. Recommending diagnostic steps based on user descriptions
4. Explaining OceanBase concepts and troubleshooting procedures

When users describe problems or ask for diagnostics:
1. First understand what they need
2. Use the appropriate diagnostic tools
3. Analyze the results
4. Provide clear explanations and recommendations

Important guidelines:
- Always confirm before executing potentially long-running operations
- Provide clear, actionable insights from diagnostic results
- Respond in the same language as the user's question
- Format output clearly with proper structure

When a tool execution fails, explain the error and suggest alternatives."""

    def __init__(
        self,
        context,
        api_key: str,
        base_url: Optional[str] = None,
        model: str = "gpt-4",
        config_path: Optional[str] = None,
        use_mcp: bool = True,
        mcp_servers: Optional[Dict[str, Dict]] = None,
        temperature: float = 0.7,
        max_tokens: int = 2000,
        system_prompt: Optional[str] = None,
    ):
        """
        Initialize the OpenAI client

        Args:
            api_key: OpenAI API key
            base_url: Optional custom API base URL
            model: Model name to use
            config_path: Path to obdiag config file (for built-in executor)
            use_mcp: Whether to use MCP client (requires obdiag_mcp package)
            mcp_servers: MCP servers configuration dict
                {
                    "server_name": {
                        "command": "...",  # for stdio
                        "args": [...],
                        "url": "...",      # for http
                        ...
                    }
                }
            temperature: Model temperature
            max_tokens: Maximum tokens in response
            system_prompt: Custom system prompt (uses default if not provided)
        """
        self.context = context
        self.stdio = context.stdio
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.use_mcp = use_mcp
        # Use custom system prompt if provided, otherwise use default
        self.system_prompt = system_prompt if system_prompt else self.SYSTEM_PROMPT

        # Initialize OpenAI client
        client_kwargs = {"api_key": api_key}
        if base_url:
            client_kwargs["base_url"] = base_url
        self.client = OpenAI(**client_kwargs)

        # Initialize tool executor
        self.mcp_client: Optional[MCPClientManager] = None
        self.executor: Optional[ObdiagExecutor] = None

        if use_mcp and mcp_servers:
            # Convert servers config to internal format
            servers_config = self._convert_servers_config(mcp_servers)
            if servers_config:
                self.mcp_client = MCPClientManager(self.context, servers_config)
                try:
                    self.mcp_client.start()
                    if not self.mcp_client.is_connected():
                        self.stdio.print("Warning: No MCP server connected, using built-in executor")
                        self.mcp_client = None
                        self.executor = ObdiagExecutor(config_path)
                except Exception as e:
                    self.stdio.print(f"Warning: MCP client failed to start ({e}), using built-in executor")
                    self.mcp_client = None
                    self.executor = ObdiagExecutor(config_path)
            else:
                self.executor = ObdiagExecutor(config_path)
        else:
            # Use built-in executor
            self.executor = ObdiagExecutor(config_path)

    def _convert_servers_config(self, mcp_servers: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Convert MCP servers config from JSON format to internal format

        Input format (compatible with Cursor's mcp.json):
            {
                "server_name": {
                    "command": "...",
                    "args": [...],
                    "env": {...},      # optional
                    "url": "...",      # for http
                    "headers": {...}   # optional, for http
                }
            }

        Output format:
            {
                "server_name": {
                    "transport": "stdio" | "http",
                    "command": "...",
                    "args": [...],
                    ...
                }
            }
        """
        result = {}
        for name, config in mcp_servers.items():
            server_config = {}

            if "url" in config:
                # HTTP transport
                server_config["transport"] = "http"
                server_config["url"] = config["url"]
                if "headers" in config:
                    server_config["headers"] = config["headers"]
            elif "command" in config:
                # Stdio transport
                server_config["transport"] = "stdio"
                server_config["command"] = config["command"]
                server_config["args"] = config.get("args", [])
                if "env" in config:
                    server_config["env"] = config["env"]
            else:
                # Skip invalid config
                continue

            result[name] = server_config

        return result

    def _get_tools(self) -> List[Dict]:
        """Get available tools for function calling"""
        if self.mcp_client and self.mcp_client.is_connected():
            try:
                return self.mcp_client.get_tools_for_openai()
            except Exception:
                pass

        if self.executor:
            return self.executor.get_available_tools()
        return []

    def _execute_tool(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """
        Execute a tool and return the result

        Args:
            tool_name: Name of the tool to execute
            arguments: Tool arguments

        Returns:
            Tool execution result as string
        """
        # Try MCP client first
        if self.mcp_client and self.mcp_client.is_connected():
            try:
                result = self.mcp_client.call_tool(tool_name, arguments)
                if result.get("success"):
                    content = result.get("content", [])
                    if content:
                        # Extract text content from MCP response
                        texts = []
                        for item in content:
                            if isinstance(item, dict) and item.get("type") == "text":
                                texts.append(item.get("text", ""))
                            elif isinstance(item, str):
                                texts.append(item)
                        return "\n".join(texts) if texts else str(content)
                    return "Tool executed successfully but returned no content."
                else:
                    return f"Tool execution failed: {result.get('error', 'Unknown error')}"
            except Exception as e:
                return f"MCP tool execution error: {str(e)}"

        # Fallback to built-in executor
        if self.executor:
            result = self.executor.execute(tool_name, arguments)
            if result.get("success"):
                output = result.get("stdout", "")
                if result.get("stderr"):
                    output += f"\nWarnings/Errors:\n{result['stderr']}"
                return output if output else "Command executed successfully."
            else:
                error_msg = result.get("stderr", "Unknown error")
                return f"Command execution failed: {error_msg}"

        return "No executor available"

    def chat(self, user_message: str, conversation_history: Optional[List[Dict]] = None) -> str:
        """
        Send a chat message and get a response

        Args:
            user_message: User's message
            conversation_history: Optional conversation history

        Returns:
            AI response as string
        """
        # Build messages
        messages = [{"role": "system", "content": self.system_prompt}]

        if conversation_history:
            messages.extend(conversation_history)

        messages.append({"role": "user", "content": user_message})

        # Get available tools
        tools = self._get_tools()

        # Make API call
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=tools if tools else None,
                tool_choice="auto" if tools else None,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )

            # Process response
            assistant_message = response.choices[0].message

            # Check if tool calls are needed
            if assistant_message.tool_calls:
                # Execute tools and collect results
                tool_results = []
                for tool_call in assistant_message.tool_calls:
                    tool_name = tool_call.function.name
                    try:
                        arguments = json.loads(tool_call.function.arguments)
                    except json.JSONDecodeError:
                        arguments = {}

                    result = self._execute_tool(tool_name, arguments)
                    tool_results.append(
                        {
                            "tool_call_id": tool_call.id,
                            "role": "tool",
                            "name": tool_name,
                            "content": result,
                        }
                    )

                # Add assistant message and tool results to messages
                messages.append(
                    {
                        "role": "assistant",
                        "content": assistant_message.content,
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                            }
                            for tc in assistant_message.tool_calls
                        ],
                    }
                )
                messages.extend(tool_results)

                # Get final response after tool execution
                final_response = self.client.chat.completions.create(
                    model=self.model,
                    messages=messages,
                    temperature=self.temperature,
                    max_tokens=self.max_tokens,
                )
                return final_response.choices[0].message.content or ""

            return assistant_message.content or ""

        except Exception as e:
            raise RuntimeError(f"API call failed: {str(e)}")

    def chat_stream(self, user_message: str, conversation_history: Optional[List[Dict]] = None) -> Generator[str, None, None]:
        """
        Send a chat message and get a streaming response

        Args:
            user_message: User's message
            conversation_history: Optional conversation history

        Yields:
            Response chunks
        """
        # Build messages
        messages = [{"role": "system", "content": self.system_prompt}]

        if conversation_history:
            messages.extend(conversation_history)

        messages.append({"role": "user", "content": user_message})

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
                stream=True,
            )

            for chunk in response:
                if chunk.choices[0].delta.content:
                    yield chunk.choices[0].delta.content

        except Exception as e:
            yield f"Error: {str(e)}"

    def close(self):
        """Clean up resources"""
        if self.mcp_client:
            self.mcp_client.stop()
            self.mcp_client = None

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close()
        return False
