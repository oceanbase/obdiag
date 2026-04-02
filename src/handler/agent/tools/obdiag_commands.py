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
@file: obdiag_commands.py
@desc: obdiag command tools for obdiag agent
"""

import os
import shlex
import subprocess
from typing import Any, Dict, List, Optional

from pydantic_ai import Agent, RunContext

from src.handler.agent.models import AgentDependencies


# Mapping of tool names to obdiag commands
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


def execute_obdiag_command(
    command_name: str,
    arguments: Dict[str, Any],
    config_path: str,
    stdio: Any = None,
    timeout: int = 300,
) -> Dict[str, Any]:
    """
    Execute an obdiag command via subprocess

    Args:
        command_name: Name of the obdiag command
        arguments: Dictionary of argument name-value pairs
        config_path: Path to obdiag config file
        stdio: Optional stdio for logging
        timeout: Command timeout in seconds

    Returns:
        Dictionary containing execution result
    """
    if command_name not in OBDIAG_COMMANDS:
        return {
            "success": False,
            "stdout": "",
            "stderr": f"Unknown command: {command_name}",
            "return_code": -1,
        }

    try:
        cmd_parts = [OBDIAG_COMMANDS[command_name]]

        # Add config path
        if config_path and os.path.exists(config_path):
            cmd_parts.append(f"-c {shlex.quote(config_path)}")

        # Enable silent mode
        cmd_parts.append("--inner_config")
        cmd_parts.append("obdiag.logger.silent=True")

        # Add arguments
        for arg_name, arg_value in arguments.items():
            if arg_value is None:
                continue

            if isinstance(arg_value, list):
                for item in arg_value:
                    safe_value = shlex.quote(str(item))
                    cmd_parts.append(f"--{arg_name} {safe_value}")
            elif isinstance(arg_value, bool):
                if arg_value:
                    cmd_parts.append(f"--{arg_name}")
            else:
                safe_value = shlex.quote(str(arg_value))
                cmd_parts.append(f"--{arg_name} {safe_value}")

        command = " ".join(cmd_parts)

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
            "command": command if 'command' in locals() else command_name,
            "stdout": "",
            "stderr": f"Command timed out after {timeout} seconds",
            "return_code": -1,
        }
    except Exception as e:
        return {
            "success": False,
            "command": command if 'command' in locals() else command_name,
            "stdout": "",
            "stderr": str(e),
            "return_code": -1,
        }


def register_obdiag_tools(agent: Agent[AgentDependencies, str]):
    """
    Register obdiag command tools with the agent

    Args:
        agent: The Pydantic-AI agent to register tools with
    """

    @agent.tool
    def gather_log(
        ctx: RunContext[AgentDependencies],
        since: Optional[str] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        scope: str = "all",
        grep: Optional[List[str]] = None,
        store_dir: Optional[str] = None,
    ) -> str:
        """
        Gather OceanBase logs from the cluster.

        Args:
            since: Time range from now (e.g., '1h', '30m', '2d')
            from_time: Start time (format: yyyy-mm-dd hh:mm:ss)
            to_time: End time (format: yyyy-mm-dd hh:mm:ss)
            scope: Log scope - 'observer', 'election', 'rootservice', or 'all'
            grep: Keywords to filter logs
            store_dir: Directory to store collected logs

        Returns:
            Command output or error message
        """
        deps = ctx.deps
        arguments = {}

        if since:
            arguments["since"] = since
        if from_time:
            arguments["from"] = from_time
        if to_time:
            arguments["to"] = to_time
        if scope:
            arguments["scope"] = scope
        if grep:
            arguments["grep"] = grep
        if store_dir:
            arguments["store_dir"] = store_dir

        result = execute_obdiag_command(
            "gather_log",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"Log gathering completed successfully.\n\n{output}"
        else:
            return f"Log gathering failed.\n\n{output}"

    @agent.tool
    def gather_sysstat(
        ctx: RunContext[AgentDependencies],
        store_dir: Optional[str] = None,
    ) -> str:
        """
        Gather system statistics from the OceanBase cluster nodes.

        Args:
            store_dir: Directory to store collected data

        Returns:
            Command output or error message
        """
        deps = ctx.deps
        arguments = {}

        if store_dir:
            arguments["store_dir"] = store_dir

        result = execute_obdiag_command(
            "gather_sysstat",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"System statistics gathering completed successfully.\n\n{output}"
        else:
            return f"System statistics gathering failed.\n\n{output}"

    @agent.tool
    def gather_perf(
        ctx: RunContext[AgentDependencies],
        store_dir: Optional[str] = None,
    ) -> str:
        """
        Gather performance data (flame graph, pstack) from the OceanBase cluster.

        Args:
            store_dir: Directory to store collected data

        Returns:
            Command output or error message
        """
        deps = ctx.deps
        arguments = {}

        if store_dir:
            arguments["store_dir"] = store_dir

        result = execute_obdiag_command(
            "gather_perf",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"Performance data gathering completed successfully.\n\n{output}"
        else:
            return f"Performance data gathering failed.\n\n{output}"

    @agent.tool
    def gather_plan_monitor(
        ctx: RunContext[AgentDependencies],
        trace_id: str,
        store_dir: Optional[str] = None,
    ) -> str:
        """
        Gather SQL plan monitor information for a specific trace ID.

        Args:
            trace_id: SQL trace ID to gather plan monitor data for
            store_dir: Directory to store collected data

        Returns:
            Command output or error message
        """
        deps = ctx.deps
        arguments = {"trace_id": trace_id}

        if store_dir:
            arguments["store_dir"] = store_dir

        result = execute_obdiag_command(
            "gather_plan_monitor",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"Plan monitor data gathering completed successfully.\n\n{output}"
        else:
            return f"Plan monitor data gathering failed.\n\n{output}"

    @agent.tool
    def analyze_log(
        ctx: RunContext[AgentDependencies],
        files: Optional[List[str]] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        scope: Optional[str] = None,
        log_level: Optional[str] = None,
    ) -> str:
        """
        Analyze OceanBase logs for errors and warnings.

        Args:
            files: Specific log files to analyze
            from_time: Start time for analysis
            to_time: End time for analysis
            scope: Log scope to analyze
            log_level: Minimum log level to include

        Returns:
            Analysis results or error message
        """
        deps = ctx.deps
        arguments = {}

        if files:
            arguments["files"] = files
        if from_time:
            arguments["from"] = from_time
        if to_time:
            arguments["to"] = to_time
        if scope:
            arguments["scope"] = scope
        if log_level:
            arguments["log_level"] = log_level

        result = execute_obdiag_command(
            "analyze_log",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"Log analysis completed successfully.\n\n{output}"
        else:
            return f"Log analysis failed.\n\n{output}"

    @agent.tool
    def check_cluster(
        ctx: RunContext[AgentDependencies],
        cases: Optional[str] = None,
        store_dir: Optional[str] = None,
    ) -> str:
        """
        Run health checks on the OceanBase cluster.

        Args:
            cases: Specific check cases to run (comma-separated)
            store_dir: Directory to store check results

        Returns:
            Check results or error message
        """
        deps = ctx.deps
        arguments = {}

        if cases:
            arguments["cases"] = cases
        if store_dir:
            arguments["store_dir"] = store_dir

        result = execute_obdiag_command(
            "check",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"Health check completed successfully.\n\n{output}"
        else:
            return f"Health check failed.\n\n{output}"

    @agent.tool
    def check_list(ctx: RunContext[AgentDependencies]) -> str:
        """
        List all available health check tasks.

        Returns:
            List of available check tasks
        """
        deps = ctx.deps

        result = execute_obdiag_command(
            "check_list",
            {},
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        return f"Available check tasks:\n\n{output}"

    @agent.tool
    def rca_run(
        ctx: RunContext[AgentDependencies],
        scene: str,
    ) -> str:
        """
        Run root cause analysis for a specific scenario.

        Args:
            scene: RCA scenario name to run

        Returns:
            RCA results or error message
        """
        deps = ctx.deps
        arguments = {"scene": scene}

        result = execute_obdiag_command(
            "rca_run",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"Root cause analysis completed successfully.\n\n{output}"
        else:
            return f"Root cause analysis failed.\n\n{output}"

    @agent.tool
    def rca_list(ctx: RunContext[AgentDependencies]) -> str:
        """
        List all available root cause analysis scenarios.

        Returns:
            List of available RCA scenarios
        """
        deps = ctx.deps

        result = execute_obdiag_command(
            "rca_list",
            {},
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        return f"Available RCA scenarios:\n\n{output}"

    @agent.tool
    def tool_io_performance(
        ctx: RunContext[AgentDependencies],
        disk: Optional[str] = None,
        date: Optional[str] = None,
    ) -> str:
        """
        Check disk IO performance on cluster nodes.

        Args:
            disk: Disk device name (e.g., 'sda', 'clog', 'data')
            date: Date for historical data (format: YYYYMMDD)

        Returns:
            IO performance data or error message
        """
        deps = ctx.deps
        arguments = {}

        if disk:
            arguments["disk"] = disk
        if date:
            arguments["date"] = date

        result = execute_obdiag_command(
            "tool_io_performance",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"IO performance check completed successfully.\n\n{output}"
        else:
            return f"IO performance check failed.\n\n{output}"

    @agent.tool
    def tool_sql_syntax(
        ctx: RunContext[AgentDependencies],
        sql: str,
        env: Optional[List[str]] = None,
    ) -> str:
        """
        Validate SQL on the cluster using EXPLAIN (does not execute the statement).

        Args:
            sql: Single SQL statement to check
            env: Optional connection overrides as key=value strings (host, port, user, password, database)

        Returns:
            Validation result (VALID, SYNTAX_ERROR, or SEMANTIC_ERROR) or error message
        """
        deps = ctx.deps
        arguments: Dict[str, Any] = {"sql": sql}
        if env:
            arguments["env"] = env

        result = execute_obdiag_command(
            "tool_sql_syntax",
            arguments,
            deps.config_path,
            deps.stdio,
        )

        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result.get("stderr", "")

        if result.get("success"):
            return f"SQL syntax check completed.\n\n{output}"
        else:
            return f"SQL syntax check failed.\n\n{output}"
