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
import os
import shlex
import shutil
import subprocess
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional

import yaml


class MCPServer:
    """Built-in MCP server for obdiag tools"""

    # Supported obdiag commands mapping: tool_name -> obdiag command
    SUPPORTED_COMMANDS = {
        "gather_log": "obdiag gather log",
        "gather_plan_monitor": "obdiag gather plan_monitor",
        "gather_sysstat": "obdiag gather sysstat",
        "gather_perf": "obdiag gather perf",
        "gather_obproxy_log": "obdiag gather obproxy_log",
        "gather_ash": "obdiag gather ash",
        "gather_awr": "obdiag gather awr",
        "analyze_log": "obdiag analyze log",
        "check": "obdiag check run",
        "check_list": "obdiag check list",
        "rca_run": "obdiag rca run",
        "rca_list": "obdiag rca list",
        "tool_io_performance": "obdiag tool io_performance",
    }

    def __init__(self, config_path: Optional[str] = None, stdio=None, context=None):
        self.config_path = config_path or os.path.expanduser("~/.obdiag/config.yml")
        self.stdio = stdio
        self.context = context
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
            {
                "name": "generate_config",
                "description": "Generate obdiag configuration file for OceanBase cluster. The config will be written to ~/.obdiag/config.yml. If an existing config file exists, it will be automatically backed up with timestamp (e.g., config.yml.backup_20251209_120000). Required parameters must be provided, otherwise the tool will return error message listing missing required fields.",
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
                            "description": "List of OceanBase server nodes (required). Each node should have: ip (required), ssh_username, ssh_password, home_path, data_dir, redo_dir",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "ip": {"type": "string", "description": "Node IP address (required)"},
                                    "ssh_username": {"type": "string", "description": "SSH username for this node"},
                                    "ssh_password": {"type": "string", "description": "SSH password for this node"},
                                    "home_path": {"type": "string", "description": "OceanBase installation directory"},
                                    "data_dir": {"type": "string", "description": "Data storage directory"},
                                    "redo_dir": {"type": "string", "description": "Redo log directory"},
                                },
                                "required": ["ip"],
                            },
                        },
                        "global_ssh_username": {"type": "string", "description": "Global SSH username for all nodes (required if not specified in each node)"},
                        "global_ssh_password": {"type": "string", "description": "Global SSH password for all nodes (required if not specified in each node)"},
                        "global_ssh_port": {"type": "integer", "description": "Global SSH port, default 22", "default": 22},
                        "global_ssh_key_file": {"type": "string", "description": "Global SSH key file path (alternative to password)"},
                        "global_home_path": {"type": "string", "description": "Global OceanBase installation directory (required if not specified in each node)"},
                        "global_data_dir": {"type": "string", "description": "Global data storage directory"},
                        "global_redo_dir": {"type": "string", "description": "Global redo log directory"},
                        "obproxy_cluster_name": {"type": "string", "description": "OBProxy cluster name (optional, for obproxy config)"},
                        "obproxy_nodes": {
                            "type": "array",
                            "description": "List of OBProxy nodes (optional)",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "ip": {"type": "string", "description": "OBProxy node IP address"},
                                    "ssh_username": {"type": "string", "description": "SSH username"},
                                    "ssh_password": {"type": "string", "description": "SSH password"},
                                    "home_path": {"type": "string", "description": "OBProxy installation directory"},
                                },
                            },
                        },
                        "obproxy_global_ssh_username": {"type": "string", "description": "Global SSH username for OBProxy nodes"},
                        "obproxy_global_ssh_password": {"type": "string", "description": "Global SSH password for OBProxy nodes"},
                        "obproxy_global_ssh_port": {"type": "integer", "description": "Global SSH port for OBProxy, default 22"},
                        "obproxy_global_home_path": {"type": "string", "description": "Global OBProxy installation directory"},
                    },
                    "required": ["ob_cluster_name", "db_host", "tenant_sys_password", "nodes"],
                },
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
        # Validate tool name against registered tools
        valid_tools = {t["name"] for t in self.tools}
        if tool_name not in valid_tools:
            return {"content": [{"type": "text", "text": "Unknown tool: {0}".format(tool_name)}], "isError": True}

        # Handle special tools
        if tool_name == "generate_config":
            return self._generate_config(arguments)

        # Execute obdiag command
        result = self._execute_obdiag_command(tool_name, arguments)

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        return {"content": [{"type": "text", "text": output}], "isError": not result.get("success", False)}

    def _build_obdiag_command(self, tool_name: str, arguments: Dict[str, Any]) -> str:
        """
        Build obdiag command string from tool name and arguments

        Args:
            tool_name: Name of the tool to execute
            arguments: Dictionary of argument name-value pairs

        Returns:
            Complete command string
        """
        if tool_name not in self.SUPPORTED_COMMANDS:
            raise ValueError("Unsupported tool: {0}".format(tool_name))

        cmd_parts = [self.SUPPORTED_COMMANDS[tool_name]]

        # Add config path
        if self.config_path and os.path.exists(self.config_path):
            cmd_parts.append("-c {0}".format(shlex.quote(self.config_path)))

        # Enable silent mode for JSON output (integration mode)
        cmd_parts.append("--inner_config")
        cmd_parts.append("obdiag.logger.silent=True")

        # Get valid parameters for this tool from registered tools
        tool_schema = next((t for t in self.tools if t["name"] == tool_name), None)
        valid_params = set()
        if tool_schema:
            valid_params = set(tool_schema.get("inputSchema", {}).get("properties", {}).keys())

        # Add arguments
        for arg_name, arg_value in arguments.items():
            if arg_value is None:
                continue
            # Only add parameters that are defined in the tool schema
            if valid_params and arg_name not in valid_params:
                continue

            # Handle different argument types
            if isinstance(arg_value, list):
                # For array arguments like grep keywords
                for item in arg_value:
                    safe_value = shlex.quote(str(item))
                    cmd_parts.append("--{0} {1}".format(arg_name, safe_value))
            elif isinstance(arg_value, bool):
                if arg_value:
                    cmd_parts.append("--{0}".format(arg_name))
            else:
                safe_value = shlex.quote(str(arg_value))
                cmd_parts.append("--{0} {1}".format(arg_name, safe_value))

        return " ".join(cmd_parts)

    def _execute_obdiag_command(self, tool_name: str, arguments: Dict[str, Any], timeout: int = 300) -> Dict[str, Any]:
        """
        Execute obdiag command via subprocess

        Args:
            tool_name: Name of the tool to execute
            arguments: Dictionary of argument name-value pairs
            timeout: Command timeout in seconds

        Returns:
            Dictionary containing execution result
        """
        try:
            # Build command
            command = self._build_obdiag_command(tool_name, arguments)
            if self.stdio:
                self.stdio.verbose("Executing obdiag command: {0}".format(command))

            # Execute command
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout,
                env=os.environ.copy(),
            )

            return {
                "success": result.returncode == 0,
                "command": command,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "return_code": result.returncode,
            }

        except subprocess.TimeoutExpired:
            return {
                "success": False,
                "command": command if 'command' in locals() else tool_name,
                "stdout": "",
                "stderr": "Command timed out after {0} seconds".format(timeout),
                "return_code": -1,
            }
        except Exception as e:
            return {
                "success": False,
                "command": command if 'command' in locals() else tool_name,
                "stdout": "",
                "stderr": str(e),
                "return_code": -1,
            }

    def _generate_config(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Generate obdiag configuration file"""
        # Check required parameters
        missing_params = []
        required_params = ["ob_cluster_name", "db_host", "tenant_sys_password", "nodes"]

        for param in required_params:
            if param not in arguments or arguments[param] is None:
                missing_params.append(param)
            elif param == "nodes":
                nodes = arguments.get("nodes", [])
                if not nodes or len(nodes) == 0:
                    missing_params.append("nodes (at least one node with ip is required)")

        # Check if nodes have required ip field
        nodes = arguments.get("nodes", [])
        if nodes:
            for i, node in enumerate(nodes):
                if not node.get("ip"):
                    missing_params.append("nodes[{0}].ip".format(i))

        # Check global SSH credentials if not specified in nodes
        has_global_ssh = arguments.get("global_ssh_username") and (arguments.get("global_ssh_password") or arguments.get("global_ssh_key_file"))
        has_global_home_path = arguments.get("global_home_path")

        if nodes and not has_global_ssh:
            # Check if each node has SSH credentials
            for i, node in enumerate(nodes):
                if not node.get("ssh_username"):
                    if not arguments.get("global_ssh_username"):
                        missing_params.append("global_ssh_username or nodes[{0}].ssh_username".format(i))
                        break
                if not node.get("ssh_password") and not arguments.get("global_ssh_password") and not arguments.get("global_ssh_key_file"):
                    missing_params.append("global_ssh_password/global_ssh_key_file or nodes[{0}].ssh_password".format(i))
                    break

        if nodes and not has_global_home_path:
            # Check if each node has home_path
            for i, node in enumerate(nodes):
                if not node.get("home_path"):
                    if not arguments.get("global_home_path"):
                        missing_params.append("global_home_path or nodes[{0}].home_path".format(i))
                        break

        # If missing required parameters, return error with detailed message
        if missing_params:
            error_msg = "Cannot generate config file. Missing required parameters:\n"
            for param in missing_params:
                error_msg += "  - {0}\n".format(param)
            error_msg += "\nPlease provide the following information:\n"
            error_msg += "  1. ob_cluster_name: Name of your OceanBase cluster\n"
            error_msg += "  2. db_host: Database host IP address\n"
            error_msg += "  3. tenant_sys_password: Password for sys tenant (can be empty string '')\n"
            error_msg += "  4. nodes: List of server nodes, each with at least 'ip' field\n"
            error_msg += "  5. SSH credentials: Either global_ssh_username + global_ssh_password/global_ssh_key_file, or specify in each node\n"
            error_msg += "  6. home_path: OceanBase installation directory (global_home_path or in each node)\n"
            return {"content": [{"type": "text", "text": error_msg}], "isError": True}

        # Build configuration
        config = self._build_config(arguments)

        # Determine output path (always use ~/.obdiag/config.yml for local obdiag)
        output_path = os.path.expanduser("~/.obdiag/config.yml")

        # Ensure directory exists
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        # Backup existing config file if it exists
        backup_path = None
        if os.path.exists(output_path):
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = "{0}.backup_{1}".format(output_path, timestamp)
                shutil.copy2(output_path, backup_path)
            except Exception as e:
                return {"content": [{"type": "text", "text": "Failed to backup existing config file: {0}".format(str(e))}], "isError": True}

        # Write config file
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

            success_msg = "Configuration file generated successfully!\n"
            success_msg += "Output path: {0}\n".format(output_path)
            if backup_path:
                success_msg += "Backup of previous config: {0}\n".format(backup_path)
            success_msg += "\nGenerated configuration:\n"
            success_msg += yaml.dump(config, default_flow_style=False, allow_unicode=True, sort_keys=False)

            return {"content": [{"type": "text", "text": success_msg}], "isError": False}
        except Exception as e:
            # If write failed and we have backup, try to restore
            if backup_path and os.path.exists(backup_path):
                try:
                    shutil.copy2(backup_path, output_path)
                except Exception:
                    pass
            return {"content": [{"type": "text", "text": "Failed to write config file: {0}".format(str(e))}], "isError": True}

    def _build_config(self, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Build configuration dictionary from arguments"""
        config = {}

        # Build obcluster section
        obcluster = {
            "ob_cluster_name": arguments.get("ob_cluster_name"),
            "db_host": arguments.get("db_host"),
            "db_port": arguments.get("db_port", 2881),
            "tenant_sys": {
                "user": arguments.get("tenant_sys_user", "root@sys"),
                "password": arguments.get("tenant_sys_password", ""),
            },
            "servers": {"nodes": [], "global": {}},
        }

        # Build nodes
        nodes = arguments.get("nodes", [])
        for node in nodes:
            node_config = {"ip": node.get("ip")}
            if node.get("ssh_username"):
                node_config["ssh_username"] = node.get("ssh_username")
            if node.get("ssh_password"):
                node_config["ssh_password"] = node.get("ssh_password")
            if node.get("home_path"):
                node_config["home_path"] = node.get("home_path")
            if node.get("data_dir"):
                node_config["data_dir"] = node.get("data_dir")
            if node.get("redo_dir"):
                node_config["redo_dir"] = node.get("redo_dir")
            obcluster["servers"]["nodes"].append(node_config)

        # Build global section
        global_config = {}
        if arguments.get("global_ssh_username"):
            global_config["ssh_username"] = arguments.get("global_ssh_username")
        if arguments.get("global_ssh_password"):
            global_config["ssh_password"] = arguments.get("global_ssh_password")
        if arguments.get("global_ssh_port"):
            global_config["ssh_port"] = arguments.get("global_ssh_port", 22)
        if arguments.get("global_ssh_key_file"):
            global_config["ssh_key_file"] = arguments.get("global_ssh_key_file")
        if arguments.get("global_home_path"):
            global_config["home_path"] = arguments.get("global_home_path")
        if arguments.get("global_data_dir"):
            global_config["data_dir"] = arguments.get("global_data_dir")
        if arguments.get("global_redo_dir"):
            global_config["redo_dir"] = arguments.get("global_redo_dir")

        if global_config:
            obcluster["servers"]["global"] = global_config

        config["obcluster"] = obcluster

        # Build obproxy section if provided
        if arguments.get("obproxy_cluster_name") or arguments.get("obproxy_nodes"):
            obproxy = {"obproxy_cluster_name": arguments.get("obproxy_cluster_name", "obproxy"), "servers": {"nodes": [], "global": {}}}

            obproxy_nodes = arguments.get("obproxy_nodes", [])
            for node in obproxy_nodes:
                node_config = {}
                if node.get("ip"):
                    node_config["ip"] = node.get("ip")
                if node.get("ssh_username"):
                    node_config["ssh_username"] = node.get("ssh_username")
                if node.get("ssh_password"):
                    node_config["ssh_password"] = node.get("ssh_password")
                if node.get("home_path"):
                    node_config["home_path"] = node.get("home_path")
                if node_config:
                    obproxy["servers"]["nodes"].append(node_config)

            # Build obproxy global section
            obproxy_global = {}
            if arguments.get("obproxy_global_ssh_username"):
                obproxy_global["ssh_username"] = arguments.get("obproxy_global_ssh_username")
            if arguments.get("obproxy_global_ssh_password"):
                obproxy_global["ssh_password"] = arguments.get("obproxy_global_ssh_password")
            if arguments.get("obproxy_global_ssh_port"):
                obproxy_global["ssh_port"] = arguments.get("obproxy_global_ssh_port", 22)
            if arguments.get("obproxy_global_home_path"):
                obproxy_global["home_path"] = arguments.get("obproxy_global_home_path")

            if obproxy_global:
                obproxy["servers"]["global"] = obproxy_global

            config["obproxy"] = obproxy

        return config

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
