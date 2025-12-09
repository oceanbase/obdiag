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
@time: 2025/12/09
@file: mcp_server.py
@desc: Built-in MCP server for obdiag tools
"""

import json
import sys
from typing import Dict, List, Any, Optional
from src.handler.ai.obdiag_executor import ObdiagExecutor


class MCPServer:
    """Built-in MCP server for obdiag tools"""

    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path
        self.executor = ObdiagExecutor(config_path=config_path)
        self.tools = self._register_tools()
        self.initialized = False

    def _register_tools(self) -> List[Dict[str, Any]]:
        """Register available obdiag tools"""
        return [
            {
                "name": "gather_log",
                "description": "Gather OceanBase logs from specified time range",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "from": {"type": "string", "description": "Start time (format: yyyy-mm-dd hh:mm:ss)"},
                        "to": {"type": "string", "description": "End time (format: yyyy-mm-dd hh:mm:ss)"},
                        "since": {"type": "string", "description": "Time range (e.g., 1h, 30m)"},
                        "scope": {"type": "string", "description": "Log scope (observer, election, rootservice, all)", "default": "all"},
                        "grep": {"type": "array", "items": {"type": "string"}, "description": "Keywords to filter logs"},
                        "store_dir": {"type": "string", "description": "Directory to store results"},
                    },
                },
            },
            {
                "name": "gather_plan_monitor",
                "description": "Gather SQL plan monitor information",
                "inputSchema": {"type": "object", "properties": {"trace_id": {"type": "string", "description": "SQL trace ID"}, "store_dir": {"type": "string", "description": "Directory to store results"}}, "required": ["trace_id"]},
            },
            {
                "name": "check",
                "description": "Run health check on OceanBase cluster",
                "inputSchema": {"type": "object", "properties": {"cases": {"type": "string", "description": "Specific check cases to run"}, "store_dir": {"type": "string", "description": "Directory to store results"}}},
            },
            {
                "name": "analyze_log",
                "description": "Analyze OceanBase logs",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "files": {"type": "array", "items": {"type": "string"}, "description": "Log files to analyze"},
                        "from": {"type": "string", "description": "Start time"},
                        "to": {"type": "string", "description": "End time"},
                        "scope": {"type": "string", "description": "Log scope"},
                        "log_level": {"type": "string", "description": "Minimum log level"},
                    },
                },
            },
            {
                "name": "tool_io_performance",
                "description": "Check disk IO performance using tsar",
                "inputSchema": {"type": "object", "properties": {"disk": {"type": "string", "description": "Disk device name (e.g., sda, clog, data)"}, "date": {"type": "string", "description": "Date for historical data (format: YYYYMMDD)"}}},
            },
        ]

    def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle MCP request"""
        method = request.get("method")
        request_id = request.get("id")
        params = request.get("params", {})

        response = {"jsonrpc": "2.0", "id": request_id}

        try:
            if method == "initialize":
                response["result"] = {"protocolVersion": "2024-11-05", "capabilities": {"tools": {}}, "serverInfo": {"name": "obdiag-mcp-server", "version": "1.0.0"}}
                self.initialized = True

            elif method == "tools/list":
                response["result"] = {"tools": self.tools}

            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                result = self._call_tool(tool_name, arguments)
                response["result"] = result

            else:
                response["error"] = {"code": -32601, "message": "Method not found: {0}".format(method)}

        except Exception as e:
            response["error"] = {"code": -32603, "message": "Internal error: {0}".format(str(e))}

        return response

    def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call obdiag tool"""
        result = self.executor.execute_tool(tool_name, arguments)

        return {"content": [{"type": "text", "text": result.get("stdout", "")}], "isError": not result.get("success", False)}

    def run(self):
        """Run MCP server in stdio mode"""
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue

            try:
                request = json.loads(line)
                response = self.handle_request(request)
                print(json.dumps(response), flush=True)
            except json.JSONDecodeError:
                error_response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
                print(json.dumps(error_response), flush=True)
            except Exception as e:
                error_response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": "Internal error: {0}".format(str(e))}}
                print(json.dumps(error_response), flush=True)


if __name__ == "__main__":
    server = MCPServer()
    server.run()
