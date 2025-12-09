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
@file: mcp_client.py
@desc: MCP client for communicating with MCP servers
       Supports multiple servers and different transports (stdio, http)
       Reference: https://gofastmcp.com/clients/client
"""

import json
import subprocess
import threading
import queue
import shutil
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


class MCPTransport(ABC):
    """Abstract base class for MCP transports"""

    @abstractmethod
    def start(self):
        """Start the transport connection"""
        pass

    @abstractmethod
    def stop(self):
        """Stop the transport connection"""
        pass

    @abstractmethod
    def send_request(self, method: str, params: Optional[Dict] = None, timeout: float = 60.0) -> Dict:
        """Send a request and wait for response"""
        pass

    @abstractmethod
    def is_connected(self) -> bool:
        """Check if transport is connected"""
        pass


class StdioTransport(MCPTransport):
    """Stdio transport for MCP communication"""

    def __init__(self, command: str, args: Optional[List[str]] = None, env: Optional[Dict[str, str]] = None):
        self.command = command
        self.args = args or []
        self.env = env
        self.process: Optional[subprocess.Popen] = None
        self._request_id = 0
        self._pending_requests: Dict[int, queue.Queue] = {}
        self._reader_thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._running = False

    def start(self):
        if self._running:
            return

        # Build command
        cmd = [self.command] + self.args

        # Check if command exists
        if not shutil.which(self.command):
            raise RuntimeError(f"Command '{self.command}' not found")

        # Build environment
        import os

        process_env = os.environ.copy()
        if self.env:
            process_env.update(self.env)

        self.process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=process_env,
        )
        self._running = True

        # Start reader thread
        self._reader_thread = threading.Thread(target=self._read_responses, daemon=True)
        self._reader_thread.start()

    def stop(self):
        self._running = False
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
            except Exception:
                pass
            finally:
                self.process = None

    def _read_responses(self):
        while self._running and self.process and self.process.stdout:
            try:
                line = self.process.stdout.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    response = json.loads(line)
                    request_id = response.get("id")
                    if request_id is not None and request_id in self._pending_requests:
                        self._pending_requests[request_id].put(response)
                except json.JSONDecodeError:
                    pass
            except Exception:
                if self._running:
                    continue
                break

    def send_request(self, method: str, params: Optional[Dict] = None, timeout: float = 60.0) -> Dict:
        if not self._running or not self.process or not self.process.stdin:
            raise RuntimeError("Transport not running")

        with self._lock:
            self._request_id += 1
            request_id = self._request_id

        response_queue: queue.Queue = queue.Queue()
        self._pending_requests[request_id] = response_queue

        try:
            request = {"jsonrpc": "2.0", "id": request_id, "method": method}
            if params:
                request["params"] = params

            request_str = json.dumps(request) + "\n"
            self.process.stdin.write(request_str)
            self.process.stdin.flush()

            try:
                return response_queue.get(timeout=timeout)
            except queue.Empty:
                return {"error": {"code": -32000, "message": f"Request timeout after {timeout}s"}}
        finally:
            self._pending_requests.pop(request_id, None)

    def send_notification(self, method: str, params: Optional[Dict] = None):
        if not self._running or not self.process or not self.process.stdin:
            return
        notification = {"jsonrpc": "2.0", "method": method}
        if params:
            notification["params"] = params
        try:
            self.process.stdin.write(json.dumps(notification) + "\n")
            self.process.stdin.flush()
        except Exception:
            pass

    def is_connected(self) -> bool:
        return self._running and self.process is not None and self.process.poll() is None


class HttpTransport(MCPTransport):
    """HTTP transport for MCP communication"""

    def __init__(self, url: str, headers: Optional[Dict[str, str]] = None):
        if not HAS_REQUESTS:
            raise ImportError("requests package is required for HTTP transport. Install with: pip install requests")
        self.url = url
        self.headers = headers or {}
        self._connected = False
        self._request_id = 0
        self._lock = threading.Lock()

    def start(self):
        # Test connection
        try:
            response = requests.get(self.url, headers=self.headers, timeout=5)
            self._connected = True
        except Exception as e:
            raise RuntimeError(f"Failed to connect to {self.url}: {e}")

    def stop(self):
        self._connected = False

    def send_request(self, method: str, params: Optional[Dict] = None, timeout: float = 60.0) -> Dict:
        if not self._connected:
            raise RuntimeError("Transport not connected")

        with self._lock:
            self._request_id += 1
            request_id = self._request_id

        request = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params:
            request["params"] = params

        try:
            response = requests.post(
                self.url,
                json=request,
                headers={**self.headers, "Content-Type": "application/json"},
                timeout=timeout,
            )
            return response.json()
        except Exception as e:
            return {"error": {"code": -32000, "message": str(e)}}

    def is_connected(self) -> bool:
        return self._connected


class MCPServer:
    """Single MCP server connection"""

    def __init__(self, name: str, config: Dict):
        self.name = name
        self.config = config
        self.transport: Optional[MCPTransport] = None
        self._initialized = False
        self._tools_cache: Optional[List[Dict]] = None
        self._server_info: Optional[Dict] = None

    def start(self):
        transport_type = self.config.get("transport", "stdio")

        if transport_type == "stdio":
            command = self.config.get("command", "")
            args = self.config.get("args", [])
            env = self.config.get("env")
            self.transport = StdioTransport(command, args, env)
        elif transport_type == "http":
            url = self.config.get("url", "")
            headers = self.config.get("headers")
            self.transport = HttpTransport(url, headers)
        else:
            raise ValueError(f"Unsupported transport type: {transport_type}")

        self.transport.start()
        self._initialize()

    def stop(self):
        if self.transport:
            self.transport.stop()
        self._initialized = False
        self._tools_cache = None

    def _initialize(self):
        response = self.transport.send_request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "clientInfo": {"name": "obdiag-ai-assistant", "version": "1.0.0"},
            },
        )

        if "error" in response:
            raise RuntimeError(f"MCP initialization failed: {response['error'].get('message', 'Unknown error')}")

        if "result" in response:
            self._server_info = response["result"].get("serverInfo", {})

        # Send initialized notification for stdio transport
        if isinstance(self.transport, StdioTransport):
            self.transport.send_notification("notifications/initialized")

        self._initialized = True

    def list_tools(self) -> List[Dict]:
        if self._tools_cache is not None:
            return self._tools_cache

        response = self.transport.send_request("tools/list")

        if "result" in response and "tools" in response["result"]:
            self._tools_cache = response["result"]["tools"]
            return self._tools_cache
        return []

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 300.0) -> Dict:
        response = self.transport.send_request(
            "tools/call",
            {"name": name, "arguments": arguments or {}},
            timeout=timeout,
        )

        if "result" in response:
            return {
                "success": not response["result"].get("isError", False),
                "content": response["result"].get("content", []),
                "is_error": response["result"].get("isError", False),
            }
        elif "error" in response:
            return {"success": False, "content": [], "error": response["error"].get("message", "Unknown error")}
        return {"success": False, "content": [], "error": "Invalid response"}

    def is_connected(self) -> bool:
        return self.transport is not None and self.transport.is_connected() and self._initialized

    @property
    def server_info(self) -> Optional[Dict]:
        return self._server_info


class MCPClientManager:
    """
    Manager for multiple MCP server connections.
    Tools from all connected servers are merged and prefixed with server name.
    """

    def __init__(self, context, servers_config: Optional[Dict[str, Dict]] = None):
        """
        Initialize MCP client manager

        Args:
            servers_config: Dictionary of server configurations
                {
                    "server_name": {
                        "transport": "stdio" | "http",
                        "command": "...",  # for stdio
                        "args": [...],      # for stdio
                        "url": "...",       # for http
                        ...
                    }
                }
        """
        self.context = context
        self.stdio = context.stdio
        self.servers_config = servers_config or {}
        self.servers: Dict[str, MCPServer] = {}
        self._tools_map: Dict[str, str] = {}  # tool_name -> server_name

    def start(self) -> "MCPClientManager":
        """Start all configured MCP servers"""
        for name, config in self.servers_config.items():
            try:
                server = MCPServer(name, config)
                server.start()
                self.servers[name] = server
            except Exception as e:
                self.stdio.print(f"Warning: Failed to start MCP server '{name}': {e}")

        # Build tools map
        self._build_tools_map()
        return self

    def stop(self):
        """Stop all MCP servers"""
        for server in self.servers.values():
            try:
                server.stop()
            except Exception:
                pass
        self.servers.clear()
        self._tools_map.clear()

    def _build_tools_map(self):
        """Build mapping from tool name to server name"""
        self._tools_map.clear()
        for name, server in self.servers.items():
            if server.is_connected():
                try:
                    tools = server.list_tools()
                    for tool in tools:
                        tool_name = tool.get("name", "")
                        if tool_name:
                            # If multiple servers have same tool, prefix with server name
                            if tool_name in self._tools_map:
                                # Rename existing tool
                                existing_server = self._tools_map[tool_name]
                                self._tools_map[f"{existing_server}_{tool_name}"] = existing_server
                                del self._tools_map[tool_name]
                                self._tools_map[f"{name}_{tool_name}"] = name
                            else:
                                self._tools_map[tool_name] = name
                except Exception:
                    pass

    def list_tools(self) -> List[Dict]:
        """List all tools from all connected servers"""
        all_tools = []
        for name, server in self.servers.items():
            if server.is_connected():
                try:
                    tools = server.list_tools()
                    all_tools.extend(tools)
                except Exception:
                    pass
        return all_tools

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None, timeout: float = 300.0) -> Dict:
        """Call a tool by name"""
        # Find which server has this tool
        server_name = self._tools_map.get(name)

        if not server_name:
            # Try to find by checking all servers
            for sname, server in self.servers.items():
                if server.is_connected():
                    try:
                        tools = server.list_tools()
                        for tool in tools:
                            if tool.get("name") == name:
                                server_name = sname
                                break
                    except Exception:
                        pass
                if server_name:
                    break

        if not server_name or server_name not in self.servers:
            return {"success": False, "content": [], "error": f"Tool '{name}' not found in any connected server"}

        return self.servers[server_name].call_tool(name, arguments, timeout)

    def get_tools_for_openai(self) -> List[Dict]:
        """Get tools in OpenAI function calling format"""
        tools = self.list_tools()
        openai_tools = []

        for tool in tools:
            openai_tool = {
                "type": "function",
                "function": {
                    "name": tool.get("name", ""),
                    "description": tool.get("description", ""),
                    "parameters": tool.get("inputSchema", {"type": "object", "properties": {}}),
                },
            }
            openai_tools.append(openai_tool)

        return openai_tools

    def is_connected(self) -> bool:
        """Check if any server is connected"""
        return any(server.is_connected() for server in self.servers.values())

    def get_connected_servers(self) -> List[str]:
        """Get list of connected server names"""
        return [name for name, server in self.servers.items() if server.is_connected()]

    def get_server_info(self) -> Dict[str, Dict]:
        """Get server info for all connected servers"""
        info = {}
        for name, server in self.servers.items():
            if server.is_connected() and server.server_info:
                info[name] = server.server_info
        return info

    def __enter__(self) -> "MCPClientManager":
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
        return False


# Backward compatibility alias
MCPClient = MCPClientManager
