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
@file: obdiag_executor.py
@desc: Shared obdiag command execution engine used by both the agent toolsets
       and the MCP server. Single source of truth for command mappings and
       subprocess invocation.
"""

import os
import shlex
import subprocess
from typing import Any, Dict, Optional


OBDIAG_COMMANDS = {
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
    "tool_sql_syntax": "obdiag tool sql_syntax",
}


def build_obdiag_command(
    command_name: str,
    arguments: Dict[str, Any],
    config_path: Optional[str] = None,
    valid_params: Optional[set] = None,
) -> str:
    """
    Build an obdiag CLI command string.

    Args:
        command_name: Key in OBDIAG_COMMANDS
        arguments: Argument name-value pairs
        config_path: Path to obdiag config.yml
        valid_params: If given, only these parameter names are included

    Returns:
        Complete shell command string

    Raises:
        ValueError: If command_name is unknown
    """
    if command_name not in OBDIAG_COMMANDS:
        raise ValueError(f"Unknown obdiag command: {command_name}")

    parts = [OBDIAG_COMMANDS[command_name]]

    if config_path and os.path.exists(config_path):
        parts.append(f"-c {shlex.quote(config_path)}")

    parts += ["--inner_config", "obdiag.logger.silent=True"]

    for arg_name, arg_value in arguments.items():
        if arg_value is None:
            continue
        if valid_params and arg_name not in valid_params:
            continue

        if isinstance(arg_value, list):
            for item in arg_value:
                parts.append(f"--{arg_name} {shlex.quote(str(item))}")
        elif isinstance(arg_value, bool):
            if arg_value:
                parts.append(f"--{arg_name}")
        else:
            parts.append(f"--{arg_name} {shlex.quote(str(arg_value))}")

    return " ".join(parts)


def execute_obdiag_command(
    command_name: str,
    arguments: Dict[str, Any],
    config_path: Optional[str] = None,
    stdio: Any = None,
    timeout: int = 300,
    valid_params: Optional[set] = None,
) -> Dict[str, Any]:
    """
    Execute an obdiag command via subprocess.

    Args:
        command_name: Key in OBDIAG_COMMANDS
        arguments: Argument name-value pairs
        config_path: Path to obdiag config.yml
        stdio: Optional stdio for logging
        timeout: Subprocess timeout in seconds
        valid_params: If given, only these parameter names are forwarded

    Returns:
        Dict with keys: success, command, stdout, stderr, return_code
    """
    if command_name not in OBDIAG_COMMANDS:
        return {
            "success": False,
            "command": command_name,
            "stdout": "",
            "stderr": f"Unknown command: {command_name}",
            "return_code": -1,
        }

    command = ""
    try:
        command = build_obdiag_command(command_name, arguments, config_path, valid_params)

        if stdio:
            stdio.verbose(f"Executing obdiag command: {command}")

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
            "command": command or command_name,
            "stdout": "",
            "stderr": f"Command timed out after {timeout} seconds",
            "return_code": -1,
        }
    except Exception as e:
        return {
            "success": False,
            "command": command or command_name,
            "stdout": "",
            "stderr": str(e),
            "return_code": -1,
        }


def format_command_output(result: Dict[str, Any], success_label: str, failure_label: str) -> str:
    """Format execute_obdiag_command result into a human-readable string."""
    output = result.get("stdout", "")
    if result.get("stderr"):
        output += "\n" + result["stderr"]

    label = success_label if result.get("success") else failure_label
    return f"{label}\n\n{output}"
