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
@file: toolset_adapter.py
@desc: Adapters to integrate MCP servers as Pydantic-AI toolsets
"""

from typing import Any, Dict, List, Optional

from pydantic_ai import Agent, RunContext
from pydantic_ai.tools import ToolDefinition

from src.handler.agent.models import AgentDependencies


class MCPServerToolset:
    """
    Adapter to integrate the built-in MCPServer as a Pydantic-AI toolset.

    This allows the existing MCPServer tools to be used with the new
    Pydantic-AI agent architecture without major modifications.
    """

    def __init__(self, config_path: Optional[str] = None, stdio: Any = None, context: Any = None):
        """
        Initialize the MCP Server Toolset adapter

        Args:
            config_path: Path to obdiag config file
            stdio: Standard IO interface for logging
            context: obdiag context object
        """
        from src.handler.agent.mcp.server import MCPServer

        self.config_path = config_path
        self.stdio = stdio
        self.context = context
        self._server: Optional[MCPServer] = None

    @property
    def server(self) -> "MCPServer":
        """Get or create the MCPServer instance"""
        if self._server is None:
            from src.handler.agent.mcp.server import MCPServer

            self._server = MCPServer(
                config_path=self.config_path,
                stdio=self.stdio,
                context=self.context,
            )
        return self._server

    def get_tools(self) -> List[Dict[str, Any]]:
        """
        Get all available tools from the MCP server

        Returns:
            List of tool definitions
        """
        return self.server.tools

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        """
        Call a tool by name with the given arguments

        Args:
            name: Tool name
            arguments: Tool arguments

        Returns:
            Tool execution result as string
        """
        result = self.server._call_tool(name, arguments)
        return self._extract_result(result)

    def _extract_result(self, result: Dict[str, Any]) -> str:
        """
        Extract text content from MCP server result

        Args:
            result: Raw result from MCP server

        Returns:
            Extracted text content
        """
        content = result.get("content", [])
        is_error = result.get("isError", False)

        if content:
            texts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    texts.append(item.get("text", ""))
                elif isinstance(item, str):
                    texts.append(item)
            result_text = "\n".join(texts) if texts else str(content)
            if is_error:
                return f"Tool execution failed: {result_text}"
            return result_text

        return "Tool executed but returned no content."

    def register_tools(self, agent: Agent[AgentDependencies, str]):
        """
        Register all MCP server tools with the Pydantic-AI agent

        Args:
            agent: The agent to register tools with
        """
        for tool_def in self.get_tools():
            self._register_single_tool(agent, tool_def)

    def _register_single_tool(self, agent: Agent[AgentDependencies, str], tool_def: Dict[str, Any]):
        """
        Register a single tool with the agent

        Args:
            agent: The agent to register the tool with
            tool_def: Tool definition from MCP server
        """
        tool_name = tool_def.get("name", "")
        description = tool_def.get("description", "")
        input_schema = tool_def.get("inputSchema", {})

        # Create a closure to capture tool_name
        def make_tool_func(captured_name: str):
            def tool_func(ctx: RunContext[AgentDependencies], **kwargs) -> str:
                return self.call_tool(captured_name, kwargs)

            tool_func.__name__ = captured_name
            tool_func.__doc__ = description
            return tool_func

        tool_func = make_tool_func(tool_name)

        # Register with agent
        # Note: Pydantic-AI will infer parameters from the function signature
        # For MCP tools with dynamic schemas, we use **kwargs
        agent.tool(tool_func)


class MCPClientToolset:
    """
    Adapter to integrate external MCP servers via MCPClientManager as a Pydantic-AI toolset.

    This allows external MCP servers to be used with the new Pydantic-AI agent architecture.
    """

    def __init__(self, servers_config: Dict[str, Dict], context: Any = None):
        """
        Initialize the MCP Client Toolset adapter

        Args:
            servers_config: Dictionary of server configurations
            context: obdiag context object
        """
        from src.handler.agent.mcp.client import MCPClientManager

        self.servers_config = servers_config
        self.context = context
        self._client: Optional[MCPClientManager] = None
        self._started = False

    @property
    def client(self) -> "MCPClientManager":
        """Get or create the MCPClientManager instance"""
        if self._client is None:
            from src.handler.agent.mcp.client import MCPClientManager

            self._client = MCPClientManager(
                servers_config=self._convert_servers_config(self.servers_config),
                context=self.context,
            )
        return self._client

    def _convert_servers_config(self, mcp_servers: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Convert MCP servers config from JSON format to internal format

        Input format (compatible with Cursor's mcp.json):
            {
                "server_name": {
                    "command": "...",
                    "args": [...],
                    "env": {...},
                    "url": "...",
                    "headers": {...}
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
                server_config["transport"] = "http"
                server_config["url"] = config["url"]
                if "headers" in config:
                    server_config["headers"] = config["headers"]
            elif "command" in config:
                server_config["transport"] = "stdio"
                server_config["command"] = config["command"]
                server_config["args"] = config.get("args", [])
                if "env" in config:
                    server_config["env"] = config["env"]
            else:
                continue

            result[name] = server_config

        return result

    def start(self) -> bool:
        """
        Start the MCP client connections

        Returns:
            True if at least one server connected successfully
        """
        if self._started:
            return self.is_connected()

        try:
            self.client.start()
            self._started = True
            return self.is_connected()
        except Exception:
            return False

    def stop(self):
        """Stop all MCP client connections"""
        if self._client:
            self._client.stop()
        self._started = False

    def is_connected(self) -> bool:
        """Check if any MCP server is connected"""
        return self._client is not None and self._client.is_connected()

    def get_tools(self) -> List[Dict[str, Any]]:
        """
        Get all available tools from connected MCP servers

        Returns:
            List of tool definitions
        """
        if not self.is_connected():
            return []
        return self.client.list_tools()

    def call_tool(self, name: str, arguments: Dict[str, Any]) -> str:
        """
        Call a tool by name with the given arguments

        Args:
            name: Tool name
            arguments: Tool arguments

        Returns:
            Tool execution result as string
        """
        if not self.is_connected():
            return "Error: No MCP server connected"

        result = self.client.call_tool(name, arguments)
        return self._extract_result(result)

    def _extract_result(self, result: Dict[str, Any]) -> str:
        """
        Extract text content from MCP client result

        Args:
            result: Raw result from MCP client

        Returns:
            Extracted text content
        """
        if result.get("success"):
            content = result.get("content", [])
            if content:
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

    def get_connected_servers(self) -> List[str]:
        """Get list of connected server names"""
        if not self.is_connected():
            return []
        return self.client.get_connected_servers()

    def get_server_info(self) -> Dict[str, Dict]:
        """Get server info for all connected servers"""
        if not self.is_connected():
            return {}
        return self.client.get_server_info()

    def register_tools(self, agent: Agent[AgentDependencies, str]):
        """
        Register all MCP client tools with the Pydantic-AI agent

        Args:
            agent: The agent to register tools with
        """
        if not self.start():
            return

        for tool_def in self.get_tools():
            self._register_single_tool(agent, tool_def)

    def _register_single_tool(self, agent: Agent[AgentDependencies, str], tool_def: Dict[str, Any]):
        """
        Register a single tool with the agent

        Args:
            agent: The agent to register the tool with
            tool_def: Tool definition from MCP server
        """
        tool_name = tool_def.get("name", "")
        description = tool_def.get("description", "")

        def make_tool_func(captured_name: str):
            def tool_func(ctx: RunContext[AgentDependencies], **kwargs) -> str:
                return self.call_tool(captured_name, kwargs)

            tool_func.__name__ = captured_name
            tool_func.__doc__ = description
            return tool_func

        tool_func = make_tool_func(tool_name)
        agent.tool(tool_func)

    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()
        return False
