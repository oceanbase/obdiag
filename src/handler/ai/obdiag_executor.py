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
@file: obdiag_executor.py
@desc: obdiag command executor for AI Assistant
"""

import os
import subprocess
import shlex
from typing import Dict, List, Optional, Any


class ObdiagExecutor:
    """obdiag command executor for programmatic execution"""

    # Supported obdiag commands and their descriptions
    SUPPORTED_COMMANDS = {
        "gather_log": {
            "command": "obdiag gather log",
            "description": "Gather OceanBase logs from observer machines",
            "parameters": {
                "from": "Start time (format: 'yyyy-mm-dd hh:mm:ss')",
                "to": "End time (format: 'yyyy-mm-dd hh:mm:ss')",
                "since": "Time range before now (e.g., '1h', '30m', '1d')",
                "scope": "Log type (choices: observer, election, rootservice, all)",
                "grep": "Keywords to filter",
                "store_dir": "Directory to store results",
            },
        },
        "gather_sysstat": {
            "command": "obdiag gather sysstat",
            "description": "Gather system statistics from host machines",
            "parameters": {
                "store_dir": "Directory to store results",
            },
        },
        "gather_perf": {
            "command": "obdiag gather perf",
            "description": "Gather performance data",
            "parameters": {
                "store_dir": "Directory to store results",
                "scope": "Perf type (choices: sample, flame, pstack, all)",
            },
        },
        "gather_obproxy_log": {
            "command": "obdiag gather obproxy_log",
            "description": "Gather OBProxy logs",
            "parameters": {
                "from": "Start time (format: 'yyyy-mm-dd hh:mm:ss')",
                "to": "End time (format: 'yyyy-mm-dd hh:mm:ss')",
                "since": "Time range before now (e.g., '1h', '30m', '1d')",
                "scope": "Log type",
                "grep": "Keywords to filter",
                "store_dir": "Directory to store results",
            },
        },
        "analyze_log": {
            "command": "obdiag analyze log",
            "description": "Analyze OceanBase logs",
            "parameters": {
                "from": "Start time (format: 'yyyy-mm-dd hh:mm:ss')",
                "to": "End time (format: 'yyyy-mm-dd hh:mm:ss')",
                "since": "Time range before now (e.g., '1h', '30m', '1d')",
                "scope": "Log type (choices: observer, election, rootservice, all)",
                "grep": "Keywords to filter",
                "log_level": "Log level filter (choices: DEBUG, TRACE, INFO, WDIAG, WARN, EDIAG, ERROR)",
                "store_dir": "Directory to store results",
            },
        },
        "check": {
            "command": "obdiag check run",
            "description": "Run health check on OceanBase cluster",
            "parameters": {
                "cases": "Check cases for observer",
                "obproxy_cases": "Check cases for obproxy",
                "store_dir": "Directory to store results",
                "report_type": "Report format (choices: table, json, xml, yaml, html)",
            },
        },
        "rca_run": {
            "command": "obdiag rca run",
            "description": "Run root cause analysis",
            "parameters": {
                "scene": "RCA scene name",
                "store_dir": "Directory to store results",
                "report_type": "Report format (choices: table, json, xml, yaml, html)",
            },
        },
        "rca_list": {
            "command": "obdiag rca list",
            "description": "List available RCA scenes",
            "parameters": {},
        },
        "check_list": {
            "command": "obdiag check list",
            "description": "List available check tasks",
            "parameters": {},
        },
        "gather_ash": {
            "command": "obdiag gather ash",
            "description": "Gather ASH (Active Session History) report",
            "parameters": {
                "from": "Start time (format: 'yyyy-mm-dd hh:mm:ss')",
                "to": "End time (format: 'yyyy-mm-dd hh:mm:ss')",
                "sql_id": "SQL ID to filter",
                "trace_id": "Trace ID to filter",
                "report_type": "Report type (TEXT or HTML)",
                "store_dir": "Directory to store results",
            },
        },
        "gather_awr": {
            "command": "obdiag gather awr",
            "description": "Gather AWR (Automatic Workload Repository) data",
            "parameters": {
                "from": "Start time (format: 'yyyy-mm-dd hh:mm:ss')",
                "to": "End time (format: 'yyyy-mm-dd hh:mm:ss')",
                "since": "Time range before now (e.g., '1h', '30m', '1d')",
                "store_dir": "Directory to store results",
            },
        },
        "tool_io_performance": {
            "command": "obdiag tool io_performance",
            "description": "Check disk IO performance using tsar",
            "parameters": {
                "disk": "Disk device name (e.g., sda, sdb) or 'clog' or 'data'",
                "date": "Date for historical data (format: YYYYMMDD)",
            },
        },
    }

    def __init__(self, config_path: str = None):
        """
        Initialize obdiag executor

        Args:
            config_path: Path to obdiag configuration file
        """
        self.config_path = config_path or os.path.expanduser("~/.obdiag/config.yml")

    def get_available_tools(self) -> List[Dict[str, Any]]:
        """
        Get list of available tools for AI function calling

        Returns:
            List of tool definitions for OpenAI function calling format
        """
        tools = []
        for tool_name, tool_info in self.SUPPORTED_COMMANDS.items():
            parameters = {
                "type": "object",
                "properties": {},
                "required": [],
            }

            for param_name, param_desc in tool_info["parameters"].items():
                parameters["properties"][param_name] = {
                    "type": "string",
                    "description": param_desc,
                }

            tools.append(
                {
                    "type": "function",
                    "function": {
                        "name": tool_name,
                        "description": tool_info["description"],
                        "parameters": parameters,
                    },
                }
            )

        return tools

    def build_command(self, tool_name: str, arguments: Dict[str, str]) -> str:
        """
        Build obdiag command string from tool name and arguments

        Args:
            tool_name: Name of the tool to execute
            arguments: Dictionary of argument name-value pairs

        Returns:
            Complete command string
        """
        if tool_name not in self.SUPPORTED_COMMANDS:
            raise ValueError(f"Unsupported tool: {tool_name}")

        tool_info = self.SUPPORTED_COMMANDS[tool_name]
        cmd_parts = [tool_info["command"]]

        # Add config path
        cmd_parts.append(f"-c {shlex.quote(self.config_path)}")

        # Add arguments
        for arg_name, arg_value in arguments.items():
            if arg_value and arg_name in tool_info["parameters"]:
                # Handle special characters in values
                safe_value = shlex.quote(str(arg_value))
                cmd_parts.append(f"--{arg_name} {safe_value}")

        return " ".join(cmd_parts)

    def execute(self, tool_name: str, arguments: Dict[str, str], timeout: int = 300) -> Dict[str, Any]:
        """
        Execute an obdiag command

        Args:
            tool_name: Name of the tool to execute
            arguments: Dictionary of argument name-value pairs
            timeout: Command timeout in seconds

        Returns:
            Dictionary containing execution result
        """
        try:
            # Build command
            command = self.build_command(tool_name, arguments)

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
                "stderr": f"Command timed out after {timeout} seconds",
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

    def get_tool_description(self, tool_name: str) -> Optional[str]:
        """
        Get description for a specific tool

        Args:
            tool_name: Name of the tool

        Returns:
            Tool description or None if not found
        """
        if tool_name in self.SUPPORTED_COMMANDS:
            return self.SUPPORTED_COMMANDS[tool_name]["description"]
        return None

    def list_tools(self) -> List[str]:
        """
        List all available tool names

        Returns:
            List of tool names
        """
        return list(self.SUPPORTED_COMMANDS.keys())

