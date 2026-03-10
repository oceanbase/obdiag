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
@file: obdiag.py
@desc: obdiag command toolset — wraps CLI commands as pydantic-ai tools using the
       shared executor from src.common.obdiag_executor.

       Every tool accepts an optional ``cluster_config_path`` so the agent can
       target a non-default cluster without changing the session state.  When
       omitted, the active config_path from AgentDependencies is used.
"""

from typing import List, Optional

from pydantic_ai import FunctionToolset, RunContext

from src.common.obdiag_executor import execute_obdiag_command, format_command_output
from src.handler.agent.cluster_resolve import DEFAULT_CLUSTER_CONFIG, OBDIAG_CONFIG_DIR
from src.handler.agent.config import resolve_cluster_config_path
from src.handler.agent.models import AgentDependencies

obdiag_toolset: FunctionToolset[AgentDependencies] = FunctionToolset()

_CLUSTER_CONFIG_PARAM_DOC = (
    "cluster_config_path: Optional path or short name (e.g., 'obdiag_test' for "
    "~/.obdiag/obdiag_test.yml) for a non-default cluster. When omitted, the "
    "current active cluster config is used."
)


def _config(ctx: RunContext[AgentDependencies], override: Optional[str]) -> str:
    """Return the effective config_path: explicit override > active session path > default."""
    if not override:
        return ctx.deps.config_path or DEFAULT_CLUSTER_CONFIG
    resolved = resolve_cluster_config_path(override)
    if resolved:
        return resolved
    # Path-like or short name not found: return best-guess path for -c
    import os
    s = str(override).strip()
    if "/" in s or s.startswith("~"):
        return os.path.abspath(os.path.expanduser(s))
    base = s if s.endswith((".yml", ".yaml")) else s + ".yml"
    return os.path.abspath(os.path.join(OBDIAG_CONFIG_DIR, base))


def _run(
    ctx: RunContext[AgentDependencies],
    cmd: str,
    args: dict,
    ok: str,
    fail: str,
    cluster_config_path: Optional[str] = None,
) -> str:
    """Execute an obdiag command and return formatted output."""
    deps = ctx.deps
    cfg = _config(ctx, cluster_config_path)
    result = execute_obdiag_command(cmd, args, cfg, deps.stdio)
    return format_command_output(result, ok, fail)


# ---------------------------------------------------------------------------
# Gather tools
# ---------------------------------------------------------------------------

@obdiag_toolset.tool
def gather_log(
    ctx: RunContext[AgentDependencies],
    since: Optional[str] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    scope: str = "all",
    grep: Optional[List[str]] = None,
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather OceanBase logs from the cluster.

    Args:
        since: Time range from now (e.g., '1h', '30m', '2d')
        from_time: Start time (format: yyyy-mm-dd hh:mm:ss)
        to_time: End time (format: yyyy-mm-dd hh:mm:ss)
        scope: Log scope — 'observer', 'election', 'rootservice', or 'all'
        grep: Keywords to filter logs
        store_dir: Directory to store collected logs
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {}
    if since:
        args["since"] = since
    if from_time:
        args["from"] = from_time
    if to_time:
        args["to"] = to_time
    if scope:
        args["scope"] = scope
    if grep:
        args["grep"] = grep
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "gather_log", args,
                "Log gathering completed successfully.",
                "Log gathering failed.",
                cluster_config_path)


@obdiag_toolset.tool
def gather_sysstat(
    ctx: RunContext[AgentDependencies],
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather system statistics from the OceanBase cluster nodes.

    Args:
        store_dir: Directory to store collected data
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {}
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "gather_sysstat", args,
                "System statistics gathering completed successfully.",
                "System statistics gathering failed.",
                cluster_config_path)


@obdiag_toolset.tool
def gather_perf(
    ctx: RunContext[AgentDependencies],
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather performance data (flame graph, pstack) from the OceanBase cluster.

    Args:
        store_dir: Directory to store collected data
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {}
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "gather_perf", args,
                "Performance data gathering completed successfully.",
                "Performance data gathering failed.",
                cluster_config_path)


@obdiag_toolset.tool
def gather_plan_monitor(
    ctx: RunContext[AgentDependencies],
    trace_id: str,
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather SQL plan monitor information for a specific trace ID.

    Args:
        trace_id: SQL trace ID to gather plan monitor data for
        store_dir: Directory to store collected data
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {"trace_id": trace_id}
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "gather_plan_monitor", args,
                "Plan monitor data gathering completed successfully.",
                "Plan monitor data gathering failed.",
                cluster_config_path)


# ---------------------------------------------------------------------------
# Analyze tools
# ---------------------------------------------------------------------------

@obdiag_toolset.tool
def analyze_log(
    ctx: RunContext[AgentDependencies],
    files: Optional[List[str]] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    scope: Optional[str] = None,
    log_level: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Analyze OceanBase logs for errors and warnings.

    Args:
        files: Specific log files to analyze
        from_time: Start time for analysis
        to_time: End time for analysis
        scope: Log scope to analyze
        log_level: Minimum log level to include
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {}
    if files:
        args["files"] = files
    if from_time:
        args["from"] = from_time
    if to_time:
        args["to"] = to_time
    if scope:
        args["scope"] = scope
    if log_level:
        args["log_level"] = log_level
    return _run(ctx, "analyze_log", args,
                "Log analysis completed successfully.",
                "Log analysis failed.",
                cluster_config_path)


# ---------------------------------------------------------------------------
# Check tools
# ---------------------------------------------------------------------------

@obdiag_toolset.tool
def check_cluster(
    ctx: RunContext[AgentDependencies],
    cases: Optional[str] = None,
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Run health checks on the OceanBase cluster.

    Args:
        cases: Specific check cases to run (comma-separated)
        store_dir: Directory to store check results
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {}
    if cases:
        args["cases"] = cases
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "check", args,
                "Health check completed successfully.",
                "Health check failed.",
                cluster_config_path)


@obdiag_toolset.tool
def check_list(ctx: RunContext[AgentDependencies]) -> str:
    """List all available health check tasks."""
    result = execute_obdiag_command("check_list", {}, ctx.deps.config_path, ctx.deps.stdio)
    output = result.get("stdout", "")
    if result.get("stderr"):
        output += "\n" + result["stderr"]
    return f"Available check tasks:\n\n{output}"


# ---------------------------------------------------------------------------
# RCA tools
# ---------------------------------------------------------------------------

@obdiag_toolset.tool
def rca_run(
    ctx: RunContext[AgentDependencies],
    scene: str,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Run root cause analysis for a specific scenario.

    Args:
        scene: RCA scenario name to run
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    return _run(ctx, "rca_run", {"scene": scene},
                "Root cause analysis completed successfully.",
                "Root cause analysis failed.",
                cluster_config_path)


@obdiag_toolset.tool
def rca_list(ctx: RunContext[AgentDependencies]) -> str:
    """List all available root cause analysis scenarios."""
    result = execute_obdiag_command("rca_list", {}, ctx.deps.config_path, ctx.deps.stdio)
    output = result.get("stdout", "")
    if result.get("stderr"):
        output += "\n" + result["stderr"]
    return f"Available RCA scenarios:\n\n{output}"


# ---------------------------------------------------------------------------
# Utility tools
# ---------------------------------------------------------------------------

@obdiag_toolset.tool
def tool_io_performance(
    ctx: RunContext[AgentDependencies],
    disk: Optional[str] = None,
    date: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Check disk IO performance on cluster nodes.

    Args:
        disk: Disk device name (e.g., 'sda', 'clog', 'data')
        date: Date for historical data (format: YYYYMMDD)
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {}
    if disk:
        args["disk"] = disk
    if date:
        args["date"] = date
    return _run(ctx, "tool_io_performance", args,
                "IO performance check completed successfully.",
                "IO performance check failed.",
                cluster_config_path)


# ---------------------------------------------------------------------------
# Cluster info tool
# ---------------------------------------------------------------------------

@obdiag_toolset.tool
def show_current_cluster(ctx: RunContext[AgentDependencies]) -> str:
    """
    Show information about the currently active cluster.

    Returns the cluster name, host, port, and active config file path.
    """
    return ctx.deps.current_cluster_info()
