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
       shared executor from src.common.executor.

       Every tool accepts an optional ``cluster_config_path`` so the agent can
       target a non-default cluster without changing the session state.  When
       omitted, the active config_path from AgentDependencies is used.
"""

from typing import List, Optional, Union

from pydantic_ai import FunctionToolset, RunContext

from src.handler.agent.executor import execute_obdiag_command, format_command_output
from src.handler.agent.cluster_resolve import DEFAULT_CLUSTER_CONFIG, OBDIAG_CONFIG_DIR, resolve_cluster_config_path
from src.handler.agent.models import AgentDependencies, discover_obcluster_configs
from src.handler.agent.tool_output_limits import truncate_for_agent

obdiag_toolset: FunctionToolset[AgentDependencies] = FunctionToolset()

_CLUSTER_CONFIG_PARAM_DOC = "cluster_config_path: Optional path or short name (e.g., 'obdiag_test' for " "~/.obdiag/obdiag_test.yml) for a non-default cluster. When omitted, the " "current active cluster config is used."


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
    return truncate_for_agent(format_command_output(result, ok, fail), label="obdiag")


_OBPROXY_GATHER_ANALYSIS_FOLLOWUP = (
    "\n\n---\n"
    "**Next (same agent run):** If the user asked to 分析/解读/看看日志内容, or implied it after collection, "
    "do **not** stop here. Call **file_list** on the directory printed above (e.g. path containing "
    "`obdiag_gather_pack_`). If you see **.tar.gz / .zip**, use **run_shell** (user approval) to unpack or "
    "`tar -t` / `unzip -l`, then **file_read** plain log files; then summarize. "
    "OBProxy logs are not handled by `analyze_log`."
)

_OMS_GATHER_ANALYSIS_FOLLOWUP = (
    "\n\n---\n"
    "**Next (same agent run):** If the user asked to 分析/解读/看看 OMS or Ghana / CDC logs after collection, "
    "do **not** stop here. Call **file_list** on the pack directory from stdout (`obdiag_gather_pack_*`). "
    "For **.tar.gz / .zip**, use **run_shell** (user approval) to list/unpack, then **file_read** plain logs; then summarize. "
    "OMS / CDC logs are **not** handled by `analyze_log` (that is observer-side only)."
)


# ---------------------------------------------------------------------------
# Gather tools
# ---------------------------------------------------------------------------


@obdiag_toolset.tool(requires_approval=True, retries=2)
def gather_log(
    ctx: RunContext[AgentDependencies],
    since: Optional[str] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    scope: str = "all",
    grep: Optional[Union[str, List[str]]] = None,
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather **observer-side** OceanBase logs (observer, election, rootservice). This is ``obdiag gather log``.

    Do **not** use this for OBProxy / obproxy / 代理 — use ``gather_obproxy_log`` instead.
    Do **not** use this for **OMS** (Ghana, CDC, supervisor, cm, etc.) — use ``gather_oms_log`` instead.

    Use when the user asks to collect observer 日志, 收集日志 (without mentioning obproxy or OMS), or logs filtered by trace_id/keywords.
    Pass grep=[trace_id] to filter logs containing a specific trace_id.

    Args:
        since: Time range from now (e.g., '1h', '30m', '2d')
        from_time: Start time (format: yyyy-mm-dd hh:mm:ss)
        to_time: End time (format: yyyy-mm-dd hh:mm:ss)
        scope: Log scope — 'observer', 'election', 'rootservice', or 'all'
        grep: Keyword(s) to filter logs — string or list (e.g., trace_id for "收集 traceid XXX 日志")
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
        args["grep"] = [grep] if isinstance(grep, str) else grep
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "gather_log", args, "Log gathering completed successfully.", "Log gathering failed.", cluster_config_path)


@obdiag_toolset.tool(requires_approval=True, retries=2)
def gather_obproxy_log(
    ctx: RunContext[AgentDependencies],
    since: Optional[str] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    scope: str = "all",
    grep: Optional[Union[str, List[str]]] = None,
    store_dir: Optional[str] = None,
    recent_count: Optional[int] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather OBProxy logs from OBProxy nodes (obdiag ``gather obproxy_log``).

    Use this when the user asks for OBProxy / obproxy / 代理 日志 — NOT ``gather_log`` (that is observer-side).
    If the user already has a gather pack and asks to **analyze** OBProxy logs, use **file_list** / **file_read** on that directory instead of gathering again unless they ask for a fresh pull.

    Args:
        since: Relative window (e.g. '1h', '30m'); default on CLI is often 30m if omitted
        from_time: Start time yyyy-mm-dd hh:mm:ss
        to_time: End time yyyy-mm-dd hh:mm:ss
        scope: obproxy, obproxy_limit, obproxy_stat, obproxy_digest, obproxy_slow, obproxy_diagnosis, obproxy_error, or all
        grep: Keyword filter(s)
        store_dir: Output directory
        recent_count: If >0, only the N most recent log files (time filters ignored)
        cluster_config_path: Non-default obdiag config
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
        args["grep"] = [grep] if isinstance(grep, str) else grep
    if store_dir:
        args["store_dir"] = store_dir
    if recent_count is not None:
        args["recent_count"] = recent_count
    cfg = _config(ctx, cluster_config_path)
    result = execute_obdiag_command("gather_obproxy_log", args, cfg, ctx.deps.stdio)
    text = format_command_output(
        result,
        "OBProxy log gathering completed successfully.",
        "OBProxy log gathering failed.",
    )
    if result.get("success"):
        text += _OBPROXY_GATHER_ANALYSIS_FOLLOWUP
    return truncate_for_agent(text, label="obdiag")


@obdiag_toolset.tool(requires_approval=True, retries=2)
def gather_oms_log(
    ctx: RunContext[AgentDependencies],
    since: Optional[str] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    scope: str = "all",
    grep: Optional[Union[str, List[str]]] = None,
    store_dir: Optional[str] = None,
    temp_dir: Optional[str] = None,
    recent_count: Optional[int] = None,
    oms_component_id: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather **OMS** logs from OMS machines (``obdiag gather oms_log``): Ghana, CM, supervisor, CDC/libobcdc, nginx, etc.

    Use when the user asks for **OMS** logs, **同步到 Kafka** 排障, Ghana / **CDC** / store / libobcdc logs, or OMS platform diagnostics.
    Do **not** use ``gather_log`` (observer) or ``gather_obproxy_log`` (OBProxy) for OMS.

    Requires **oms** section in obdiag ``config.yml`` (see OMS deploy docs). For CDC scopes ``cdc`` or ``libobcdc``, pass **oms_component_id**
    (format e.g. ``x.x.x.x-123``) when gathering component-specific CDC logs.

    Args:
        since: Relative window (e.g. ``1h``, ``30m``); CLI default often ``30m`` if omitted
        from_time: Start time yyyy-mm-dd hh:mm:ss
        to_time: End time yyyy-mm-dd hh:mm:ss
        scope: e.g. ``all``, ``ghana``, ``supervisor``, ``cm``, ``cdc``, ``libobcdc``, ``store``, ``console``, ``nginx``, …
        grep: Keyword filter(s)
        store_dir: Output directory for the gather pack
        temp_dir: Temp dir on remote nodes (default ``/tmp`` on CLI)
        recent_count: If >0, only the N most recent log files (time filters ignored)
        oms_component_id: Required for many CDC-related gathers (see ``obdiag gather oms_log --help``)
        cluster_config_path: Non-default obdiag config (must include valid ``oms`` when gathering OMS logs)
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
        args["grep"] = [grep] if isinstance(grep, str) else grep
    if store_dir:
        args["store_dir"] = store_dir
    if temp_dir:
        args["temp_dir"] = temp_dir
    if recent_count is not None:
        args["recent_count"] = recent_count
    if oms_component_id:
        args["oms_component_id"] = oms_component_id
    cfg = _config(ctx, cluster_config_path)
    result = execute_obdiag_command("gather_oms_log", args, cfg, ctx.deps.stdio)
    text = format_command_output(
        result,
        "OMS log gathering completed successfully.",
        "OMS log gathering failed.",
    )
    if result.get("success"):
        text += _OMS_GATHER_ANALYSIS_FOLLOWUP
    return truncate_for_agent(text, label="obdiag")


@obdiag_toolset.tool(requires_approval=True, retries=2)
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
    return _run(ctx, "gather_sysstat", args, "System statistics gathering completed successfully.", "System statistics gathering failed.", cluster_config_path)


@obdiag_toolset.tool(requires_approval=True, retries=2)
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
    return _run(ctx, "gather_perf", args, "Performance data gathering completed successfully.", "Performance data gathering failed.", cluster_config_path)


@obdiag_toolset.tool(requires_approval=True, retries=2)
def gather_ash(
    ctx: RunContext[AgentDependencies],
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    trace_id: Optional[str] = None,
    sql_id: Optional[str] = None,
    wait_class: Optional[str] = None,
    report_type: str = "TEXT",
    store_dir: Optional[str] = None,
    svr_ip: Optional[str] = None,
    svr_port: Optional[int] = None,
    tenant_id: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather ASH report (``obdiag gather ash``). Use when the user asks for ASH / ASH 报告 / 活跃会话历史采样.
    """
    args: dict = {"report_type": report_type}
    if from_time:
        args["from"] = from_time
    if to_time:
        args["to"] = to_time
    if trace_id:
        args["trace_id"] = trace_id
    if sql_id:
        args["sql_id"] = sql_id
    if wait_class:
        args["wait_class"] = wait_class
    if store_dir:
        args["store_dir"] = store_dir
    if svr_ip:
        args["svr_ip"] = svr_ip
    if svr_port:
        args["svr_port"] = svr_port
    if tenant_id:
        args["tenant_id"] = tenant_id
    return _run(ctx, "gather_ash", args, "ASH report gathering completed successfully.", "ASH report gathering failed.", cluster_config_path)


@obdiag_toolset.tool(requires_approval=True, retries=2)
def gather_awr(
    ctx: RunContext[AgentDependencies],
    since: Optional[str] = None,
    from_time: Optional[str] = None,
    to_time: Optional[str] = None,
    cluster_name: Optional[str] = None,
    cluster_id: Optional[str] = None,
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather AWR / ParalleSQL-related package (``obdiag gather awr``). Use when the user asks for AWR / gather awr.
    """
    args: dict = {}
    if since:
        args["since"] = since
    if from_time:
        args["from"] = from_time
    if to_time:
        args["to"] = to_time
    if cluster_name:
        args["cluster_name"] = cluster_name
    if cluster_id:
        args["cluster_id"] = cluster_id
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "gather_awr", args, "AWR gathering completed successfully.", "AWR gathering failed.", cluster_config_path)


@obdiag_toolset.tool(requires_approval=True, retries=2)
def gather_plan_monitor(
    ctx: RunContext[AgentDependencies],
    trace_id: str,
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Gather SQL plan monitor (执行计划监控) data for a specific trace ID.
    Use ONLY when the user explicitly wants plan monitor/execution plan analysis, NOT for collecting logs.
    For "收集日志" or "收集 traceid XXX 日志", use gather_log with grep=[trace_id] instead.

    Args:
        trace_id: SQL trace ID for plan monitor data
        store_dir: Directory to store collected data
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {"trace_id": trace_id}
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "gather_plan_monitor", args, "Plan monitor data gathering completed successfully.", "Plan monitor data gathering failed.", cluster_config_path)


# ---------------------------------------------------------------------------
# Analyze tools
# ---------------------------------------------------------------------------


@obdiag_toolset.tool(requires_approval=True, retries=2)
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
    Run ``obdiag analyze log`` — **only** for OceanBase **cluster (observer node) log types**.

    Supported ``scope`` values match the CLI: ``observer``, ``election``, ``rootservice``, or ``all``
    (all three cluster-side components). This does **not** include OBProxy, **OMS** (Ghana/CDC), or other products.

    For OBProxy logs use **gather_obproxy_log** plus **file_list** / **file_read**. For **OMS** logs use **gather_oms_log**
    plus **file_list** / **file_read** — not this tool.

    Args:
        files: Offline: paths to **observer-side** log files (same family as above)
        from_time: Start time for analysis (online mode)
        to_time: End time for analysis (online mode)
        scope: ``observer`` | ``election`` | ``rootservice`` | ``all`` — cluster-side only
        log_level: Minimum log level (DEBUG, TRACE, INFO, WDIAG, WARN, EDIAG, ERROR)
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
    return _run(ctx, "analyze_log", args, "Log analysis completed successfully.", "Log analysis failed.", cluster_config_path)


# ---------------------------------------------------------------------------
# Check tools
# ---------------------------------------------------------------------------


@obdiag_toolset.tool(requires_approval=True, retries=2)
def check_cluster(
    ctx: RunContext[AgentDependencies],
    cases: Optional[str] = None,
    obproxy_cases: Optional[str] = None,
    observer_tasks: Optional[str] = None,
    obproxy_tasks: Optional[str] = None,
    store_dir: Optional[str] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Run health checks on the OceanBase cluster and/or OBProxy nodes (``obdiag check run``).

    Use ``cases`` / ``observer_tasks`` for observer-side checks; use ``obproxy_cases`` / ``obproxy_tasks``
    for OBProxy-side checks. ``*_tasks`` takes precedence over ``*_cases`` when both are given.
    Omit all four to run the full default check suite.

    Args:
        cases: Observer check cases to run (comma-separated); ignored when observer_tasks is set
        obproxy_cases: OBProxy check cases (comma-separated); ignored when obproxy_tasks is set
        observer_tasks: Specific observer task names (comma-separated); overrides cases
        obproxy_tasks: Specific OBProxy task names (comma-separated); overrides obproxy_cases
        store_dir: Directory to store check results
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {}
    if cases:
        args["cases"] = cases
    if obproxy_cases:
        args["obproxy_cases"] = obproxy_cases
    if observer_tasks:
        args["observer_tasks"] = observer_tasks
    if obproxy_tasks:
        args["obproxy_tasks"] = obproxy_tasks
    if store_dir:
        args["store_dir"] = store_dir
    return _run(ctx, "check", args, "Health check completed successfully.", "Health check failed.", cluster_config_path)


@obdiag_toolset.tool
def check_list(ctx: RunContext[AgentDependencies], cluster_config_path: Optional[str] = None) -> str:
    """List all available health check tasks.

    Args:
        cluster_config_path: Optional path or short name for a non-default cluster config.
    """
    cfg = _config(ctx, cluster_config_path)
    result = execute_obdiag_command("check_list", {}, cfg, ctx.deps.stdio)
    output = result.get("stdout", "")
    if result.get("stderr"):
        output += "\n" + result["stderr"]
    return truncate_for_agent(f"Available check tasks:\n\n{output}", label="obdiag")


# ---------------------------------------------------------------------------
# RCA tools
# ---------------------------------------------------------------------------


@obdiag_toolset.tool(requires_approval=True, retries=2)
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
    return _run(ctx, "rca_run", {"scene": scene}, "Root cause analysis completed successfully.", "Root cause analysis failed.", cluster_config_path)


@obdiag_toolset.tool
def rca_list(ctx: RunContext[AgentDependencies], cluster_config_path: Optional[str] = None) -> str:
    """List all available root cause analysis scenarios.

    Args:
        cluster_config_path: Optional path or short name for a non-default cluster config.
    """
    cfg = _config(ctx, cluster_config_path)
    result = execute_obdiag_command("rca_list", {}, cfg, ctx.deps.stdio)
    output = result.get("stdout", "")
    if result.get("stderr"):
        output += "\n" + result["stderr"]
    return truncate_for_agent(f"Available RCA scenarios:\n\n{output}", label="obdiag")


# ---------------------------------------------------------------------------
# Utility tools
# ---------------------------------------------------------------------------


@obdiag_toolset.tool(requires_approval=True, retries=2)
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
    return _run(ctx, "tool_io_performance", args, "IO performance check completed successfully.", "IO performance check failed.", cluster_config_path)


@obdiag_toolset.tool
def tool_sql_syntax(
    ctx: RunContext[AgentDependencies],
    sql: str,
    env: Optional[List[str]] = None,
    cluster_config_path: Optional[str] = None,
) -> str:
    """
    Validate SQL syntax/semantics on the cluster using EXPLAIN (does not execute the statement).

    Args:
        sql: Single SQL statement to check
        env: Optional connection overrides as key=value strings, e.g. host=127.0.0.1 port=2881 user=root@sys
        cluster_config_path: Path to obdiag config.yml for a non-default cluster
    """
    args: dict = {"sql": sql}
    if env:
        args["env"] = env
    valid = {"sql", "env"}
    result = execute_obdiag_command("tool_sql_syntax", args, _config(ctx, cluster_config_path), ctx.deps.stdio, valid_params=valid)
    return truncate_for_agent(format_command_output(result, "SQL syntax check completed.", "SQL syntax check failed."), label="obdiag")


# ---------------------------------------------------------------------------
# Cluster info tools
# ---------------------------------------------------------------------------


@obdiag_toolset.tool
def list_obdiag_clusters(ctx: RunContext[AgentDependencies]) -> str:
    """
    List OceanBase cluster configs under the obdiag workspace (~/.obdiag).

    Includes the default ``config.yml`` and any other ``*.yml`` / ``*.yaml`` in that directory.
    For each file, shows cluster name, db_host (if obcluster is present), short name for ``/use <name>`` in the agent REPL,
    and whether it is the default config file.

    Call this when the user asks which clusters exist, what configs are available, or 有哪些集群.
    """
    rows = discover_obcluster_configs()
    if not rows:
        return f"No *.yml / *.yaml files under {OBDIAG_CONFIG_DIR}. " "The default cluster file is usually ~/.obdiag/config.yml — create it with " "`obdiag config` or the generate_obdiag_config tool."
    lines: List[str] = [
        "Cluster config files (obdiag workspace):",
        "",
    ]
    for r in rows:
        default_tag = " [default]" if r["is_default"] else ""
        short = r["short_name"]
        if not r["has_obcluster"]:
            lines.append(f"- {r['file_name']}{default_tag}: (no obcluster section yet)  " f"→ switch with: /use {short}")
            continue
        cname = r["ob_cluster_name"] or "(ob_cluster_name not set)"
        host = r["db_host"] or "-"
        lines.append(f"- {cname}{default_tag}  db_host={host}  file={r['file_name']}  " f"→ switch with: /use {short}")
    lines.extend(
        [
            "",
            "Current session:",
            ctx.deps.current_cluster_info(),
        ]
    )
    return "\n".join(lines)


@obdiag_toolset.tool
def show_current_cluster(ctx: RunContext[AgentDependencies]) -> str:
    """
    Show information about the currently active cluster.

    Returns the cluster name, host, port, and active config file path.
    """
    return ctx.deps.current_cluster_info()
