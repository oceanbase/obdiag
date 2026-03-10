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
@file: server.py
@desc: Built-in MCP server for obdiag tools.

       Uses the shared executor from ``src.common.obdiag_executor`` to avoid
       duplicating command mapping and subprocess logic.
"""

import json
import os
import shutil
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

from src.common.obdiag_executor import OBDIAG_COMMANDS, execute_obdiag_command


class MCPServer:
    """Built-in MCP server for obdiag tools — runs in stdio mode."""

    def __init__(self, config_path: Optional[str] = None, stdio: Any = None, context: Any = None):
        self.config_path = config_path or os.path.expanduser("~/.obdiag/config.yml")
        self.stdio = stdio
        self.context = context
        self.tools = self._register_tools()
        self.initialized = False

    # ------------------------------------------------------------------
    # Tool registry (JSON-Schema descriptions for MCP clients)
    # ------------------------------------------------------------------

    def _register_tools(self) -> List[Dict[str, Any]]:
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
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "trace_id": {"type": "string", "description": "SQL trace ID"},
                        "store_dir": {"type": "string", "description": "Directory to store results"},
                    },
                    "required": ["trace_id"],
                },
            },
            {
                "name": "check",
                "description": "Run health check on OceanBase cluster",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "cases": {"type": "string", "description": "Specific check cases to run"},
                        "store_dir": {"type": "string", "description": "Directory to store results"},
                    },
                },
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
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "disk": {"type": "string", "description": "Disk device name (e.g., sda, clog, data)"},
                        "date": {"type": "string", "description": "Date for historical data (format: YYYYMMDD)"},
                    },
                },
            },
            {
                "name": "generate_config",
                "description": (
                    "Generate obdiag configuration file for OceanBase cluster. "
                    "The config will be written to ~/.obdiag/config.yml. "
                    "Existing config is automatically backed up."
                ),
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "ob_cluster_name": {"type": "string", "description": "OceanBase cluster name (required)"},
                        "db_host": {"type": "string", "description": "Database host IP address (required)"},
                        "db_port": {"type": "integer", "description": "Database port, default 2881", "default": 2881},
                        "tenant_sys_user": {"type": "string", "description": "System tenant user, default root@sys", "default": "root@sys"},
                        "tenant_sys_password": {"type": "string", "description": "System tenant password (required)"},
                        "nodes": {
                            "type": "array",
                            "description": "List of OceanBase server nodes (required)",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "ip": {"type": "string"},
                                    "ssh_username": {"type": "string"},
                                    "ssh_password": {"type": "string"},
                                    "home_path": {"type": "string"},
                                    "data_dir": {"type": "string"},
                                    "redo_dir": {"type": "string"},
                                },
                                "required": ["ip"],
                            },
                        },
                        "global_ssh_username": {"type": "string"},
                        "global_ssh_password": {"type": "string"},
                        "global_ssh_port": {"type": "integer", "default": 22},
                        "global_ssh_key_file": {"type": "string"},
                        "global_home_path": {"type": "string"},
                        "global_data_dir": {"type": "string"},
                        "global_redo_dir": {"type": "string"},
                    },
                    "required": ["ob_cluster_name", "db_host", "tenant_sys_password", "nodes"],
                },
            },
        ]

    # ------------------------------------------------------------------
    # Request handling
    # ------------------------------------------------------------------

    def handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        method = request.get("method")
        request_id = request.get("id")
        params = request.get("params", {})
        response: Dict[str, Any] = {"jsonrpc": "2.0", "id": request_id}

        try:
            if method == "initialize":
                response["result"] = {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {"tools": {}},
                    "serverInfo": {"name": "obdiag-mcp-server", "version": "1.0.0"},
                }
                self.initialized = True
            elif method == "tools/list":
                response["result"] = {"tools": self.tools}
            elif method == "tools/call":
                tool_name = params.get("name")
                arguments = params.get("arguments", {})
                response["result"] = self._call_tool(tool_name, arguments)
            else:
                response["error"] = {"code": -32601, "message": f"Method not found: {method}"}
        except Exception as e:
            response["error"] = {"code": -32603, "message": f"Internal error: {e}"}

        return response

    def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        valid_tools = {t["name"] for t in self.tools}
        if tool_name not in valid_tools:
            return {"content": [{"type": "text", "text": f"Unknown tool: {tool_name}"}], "isError": True}

        if tool_name == "generate_config":
            return self._generate_config(arguments)

        tool_schema = next((t for t in self.tools if t["name"] == tool_name), None)
        valid_params: Optional[set] = None
        if tool_schema:
            valid_params = set(tool_schema.get("inputSchema", {}).get("properties", {}).keys())

        result = execute_obdiag_command(
            tool_name,
            arguments,
            config_path=self.config_path,
            stdio=self.stdio,
            valid_params=valid_params,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result["stderr"]

        return {"content": [{"type": "text", "text": output}], "isError": not result.get("success", False)}

    # ------------------------------------------------------------------
    # generate_config
    # ------------------------------------------------------------------

    def _generate_config(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        from src.handler.agent.toolsets.config_gen import _validate_config_args, _build_config

        err = _validate_config_args(arguments)
        if err:
            return {"content": [{"type": "text", "text": err}], "isError": True}

        config = _build_config(arguments)
        output_path = os.path.expanduser("~/.obdiag/config.yml")
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        backup_path = None
        if os.path.exists(output_path):
            try:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"{output_path}.backup_{ts}"
                shutil.copy2(output_path, backup_path)
            except Exception as e:
                return {"content": [{"type": "text", "text": f"Failed to backup: {e}"}], "isError": True}

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

            file_size = os.path.getsize(output_path)
            msg = f"Configuration generated at {output_path} ({file_size} bytes)\n"
            if backup_path:
                msg += f"Backup: {backup_path}\n"
            msg += "\n" + yaml.dump(config, default_flow_style=False, allow_unicode=True, sort_keys=False)
            return {"content": [{"type": "text", "text": msg}], "isError": False}
        except Exception as e:
            if backup_path and os.path.exists(backup_path):
                try:
                    shutil.copy2(backup_path, output_path)
                except Exception:
                    pass
            return {"content": [{"type": "text", "text": f"Failed to write config: {e}"}], "isError": True}

    # ------------------------------------------------------------------
    # Stdio loop
    # ------------------------------------------------------------------

    def run(self):
        """Run MCP server in stdio mode (for external clients)."""
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                request = json.loads(line)
                response = self.handle_request(request)
                print(json.dumps(response), flush=True)
            except json.JSONDecodeError:
                print(json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}), flush=True)
            except Exception as e:
                print(json.dumps({"jsonrpc": "2.0", "id": None, "error": {"code": -32603, "message": f"Internal error: {e}"}}), flush=True)


if __name__ == "__main__":
    server = MCPServer()
    server.run()
