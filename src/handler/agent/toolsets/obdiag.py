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
@time: 2026/04/09
@file: obdiag.py
@desc: obdiag command toolset for Deep Agents SDK.

       Includes the command execution engine (previously executor.py) and
       output-limit helpers (previously tool_output_limits.py).
       Returns plain Python functions via create_obdiag_tools().
       Dependencies injected via closure (config_path_getter), not RunContext.
"""

import os
import shlex
import subprocess
from collections.abc import Callable
from typing import Any, Dict, List, Optional, Union

from src.handler.agent.cluster_resolve import DEFAULT_CLUSTER_CONFIG, OBDIAG_CONFIG_DIR, resolve_cluster_config_path
from src.handler.agent.models import discover_obcluster_configs

# ---------------------------------------------------------------------------
# Output limits
# ---------------------------------------------------------------------------

# ~12k tokens UTF-8 upper bound; aligns with common coding-agent CLIs that cap
# tool output (bounded return + spill to file for huge shell/read streams).
DEFAULT_TOOL_OUTPUT_CHARS = 48 * 1024


def truncate_for_agent(
    text: str,
    limit: int = DEFAULT_TOOL_OUTPUT_CHARS,
    *,
    label: str = "output",
) -> str:
    if limit <= 0 or len(text) <= limit:
        return text
    head = text[:limit]
    return (
        f"{head}\n\n"
        f"... [{label} truncated: {len(text)} characters total, showing first {limit}] "
        "After gather_log / gather_obproxy_log / gather_oms_log, use file_list on the pack directory from tool output, "
        "then file_read(start_line=…, max_lines=…). Else use run_shell (head/tail/grep) with approval or read files on disk. "
        "中文：采集完成后先到打包输出目录执行 file_list，再用 file_read 分段读取；或经 run_shell 用 grep/head/tail。"
    )


# ---------------------------------------------------------------------------
# Command registry
# ---------------------------------------------------------------------------

OBDIAG_COMMANDS = {
    "gather_log": "obdiag gather log",
    "gather_scene_run": "obdiag gather scene run",
    "gather_scene_list": "obdiag gather scene list",
    "gather_plan_monitor": "obdiag gather plan_monitor",
    "gather_sysstat": "obdiag gather sysstat",
    "gather_perf": "obdiag gather perf",
    "gather_obproxy_log": "obdiag gather obproxy_log",
    "gather_oms_log": "obdiag gather oms_log",
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

# One-line Chinese summary for the terminal tool-trace line (not sent to LLM).
# Keys cover both obdiag CLI tools and non-CLI agent tools (db, file, skills, knowledge).
OBDIAG_TOOL_SUMMARY_ZH: Dict[str, str] = {
    "gather_log": "采集 OceanBase 集群侧日志（observer / election / rootservice）",
    "gather_obproxy_log": "采集 OBProxy 节点日志",
    "gather_oms_log": "采集 OMS 组件日志（Ghana/CDC 等）",
    "gather_sysstat": "采集各节点系统资源与指标",
    "gather_perf": "采集性能数据（火焰图、pstack 等）",
    "gather_ash": "采集 ASH 活跃会话历史",
    "gather_awr": "采集 AWR / 性能报告",
    "gather_plan_monitor": "按 trace_id 采集 SQL 计划监控",
    "gather_scene_run": "执行 gather scene 场景采集",
    "gather_scene_list": "列出可用 gather scene 场景",
    "analyze_log": "分析集群 observer 侧日志",
    "check_cluster": "执行集群健康巡检",
    "check_list": "列出可用巡检项",
    "rca_run": "执行根因分析（RCA）",
    "rca_list": "列出 RCA 场景",
    "tool_io_performance": "检查节点磁盘 IO",
    "tool_sql_syntax": "用 EXPLAIN/PREPARE 验证 SQL 语法/语义（不执行）",
    "list_obdiag_clusters": "列出 obdiag 集群配置",
    "show_current_cluster": "显示当前会话集群与配置路径",
    "db_query": "对集群执行只读 SQL",
    "file_read": "读取本地文本文件",
    "file_write": "写入或追加本地文件",
    "file_list": "列出目录内容",
    "run_shell": "执行本地 Shell（需审批）",
    "generate_config": "生成 obdiag 集群配置",
    "query_oceanbase_knowledge_base": "查询 OceanBase 知识库",
    "load_skill": "加载技能说明",
    "read_skill_uri": "读取技能资源",
    "run_skill_script": "运行技能脚本",
}


# ---------------------------------------------------------------------------
# Execution engine
# ---------------------------------------------------------------------------


def build_obdiag_command(
    command_name: str,
    arguments: Dict[str, Any],
    config_path: Optional[str] = None,
    valid_params: Optional[set] = None,
) -> str:
    """Build an obdiag CLI command string.

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
    """Execute an obdiag command via subprocess.

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


# ---------------------------------------------------------------------------
# Tool factory
# ---------------------------------------------------------------------------

_OBPROXY_GATHER_ANALYSIS_FOLLOWUP = (
    "\n\n---\n"
    "**Next (same agent run):** If the user asked to 分析/解读/看看日志内容, or implied it after collection, "
    "do **not** stop here. Call **read_file** / **ls** on the directory printed above (e.g. path containing "
    "`obdiag_gather_pack_`). If you see **.tar.gz / .zip**, use **execute** to unpack (e.g. `tar -xzf`), "
    "then read plain log files and summarize. OBProxy logs are not handled by `analyze_log`."
)

_OMS_GATHER_ANALYSIS_FOLLOWUP = (
    "\n\n---\n"
    "**Next (same agent run):** If the user asked to 分析/解读/看看 OMS or Ghana / CDC logs after collection, "
    "do **not** stop here. Call **ls** on the pack directory from stdout (`obdiag_gather_pack_*`). "
    "For **.tar.gz / .zip**, use **execute** to list/unpack, then read plain logs and summarize. "
    "OMS / CDC logs are **not** handled by `analyze_log` (that is observer-side only)."
)


def _resolve_config(override: Optional[str], config_path_getter: Callable[[], str]) -> str:
    """Return effective config_path: explicit override > session getter > default."""
    if not override:
        return config_path_getter() or DEFAULT_CLUSTER_CONFIG
    resolved = resolve_cluster_config_path(override)
    if resolved:
        return resolved
    s = str(override).strip()
    if "/" in s or s.startswith("~"):
        return os.path.abspath(os.path.expanduser(s))
    base = s if s.endswith((".yml", ".yaml")) else s + ".yml"
    return os.path.abspath(os.path.join(OBDIAG_CONFIG_DIR, base))


def _run(cmd: str, args: dict, cfg: str, ok: str, fail: str, stdio) -> str:
    if "store_dir" in args and args["store_dir"]:
        args["store_dir"] = os.path.abspath(os.path.expanduser(args["store_dir"]))
    result = execute_obdiag_command(cmd, args, cfg, stdio)
    return truncate_for_agent(format_command_output(result, ok, fail), label="obdiag")


def create_obdiag_tools(config_path_getter: Callable[[], str], stdio) -> list:
    """Return list of plain Python tool functions for the obdiag agent.

    Args:
        config_path_getter: Callable that returns the current active cluster config path.
                            Must be a callable (not a string) so /use cluster switching works.
        stdio: obdiag stdio object for logging (may be None in tests).
    """

    def gather_log(
        since: Optional[str] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        scope: str = "all",
        grep: Optional[Union[str, List[str]]] = None,
        store_dir: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Gather observer-side OceanBase logs (observer, election, rootservice). This is obdiag gather log.

        Do NOT use for OBProxy/obproxy — use gather_obproxy_log. Do NOT use for OMS (Ghana, CDC) — use gather_oms_log.

        Args:
            since: Time range from now, e.g. '1h', '30m', '2d'
            from_time: Start time yyyy-mm-dd hh:mm:ss
            to_time: End time yyyy-mm-dd hh:mm:ss
            scope: observer/election/rootservice/all
            grep: Keyword(s) to filter — string or list; pass trace_id here to filter by trace
            store_dir: Output directory
            cluster_config_path: Short name (e.g. 'obdiag_test') or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
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
        return _run("gather_log", args, cfg, "Log gathering completed successfully.", "Log gathering failed.", stdio)

    def gather_obproxy_log(
        since: Optional[str] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        scope: str = "all",
        grep: Optional[Union[str, List[str]]] = None,
        store_dir: Optional[str] = None,
        recent_count: Optional[int] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Gather OBProxy logs from OBProxy nodes (obdiag gather obproxy_log).

        Use for OBProxy/obproxy/代理日志 — NOT gather_log (that is observer-side).

        Args:
            since: Relative window e.g. '1h', '30m'
            from_time: Start time yyyy-mm-dd hh:mm:ss
            to_time: End time yyyy-mm-dd hh:mm:ss
            scope: obproxy/obproxy_limit/obproxy_stat/obproxy_digest/obproxy_slow/obproxy_diagnosis/obproxy_error/all
            grep: Keyword filter(s)
            store_dir: Output directory
            recent_count: If >0, only the N most recent log files
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
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
        result = execute_obdiag_command("gather_obproxy_log", args, cfg, stdio)
        text = format_command_output(result, "OBProxy log gathering completed.", "OBProxy log gathering failed.")
        if result.get("success"):
            text += _OBPROXY_GATHER_ANALYSIS_FOLLOWUP
        return truncate_for_agent(text, label="obdiag")

    def gather_oms_log(
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
        """Gather OMS logs: Ghana, CM, supervisor, CDC/libobcdc, nginx (obdiag gather oms_log).

        Use for OMS logs, 同步到Kafka排障, Ghana/CDC/store/libobcdc logs. NOT for observer or OBProxy.
        Requires oms section in obdiag config.yml.

        Args:
            since: Relative window e.g. '1h'
            from_time: Start time yyyy-mm-dd hh:mm:ss
            to_time: End time yyyy-mm-dd hh:mm:ss
            scope: all/ghana/supervisor/cm/cdc/libobcdc/store/console/nginx
            grep: Keyword filter(s)
            store_dir: Output directory
            temp_dir: Temp dir on remote nodes
            recent_count: Only N most recent log files
            oms_component_id: Required for CDC-related gathers (format: x.x.x.x-123)
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
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
        result = execute_obdiag_command("gather_oms_log", args, cfg, stdio)
        text = format_command_output(result, "OMS log gathering completed.", "OMS log gathering failed.")
        if result.get("success"):
            text += _OMS_GATHER_ANALYSIS_FOLLOWUP
        return truncate_for_agent(text, label="obdiag")

    def gather_sysstat(
        store_dir: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Gather system statistics from OceanBase cluster nodes (obdiag gather sysstat).

        Args:
            store_dir: Output directory
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        args: dict = {}
        if store_dir:
            args["store_dir"] = store_dir
        return _run("gather_sysstat", args, cfg, "System statistics gathering completed.", "System statistics gathering failed.", stdio)

    def gather_perf(
        store_dir: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Gather performance data — flame graph, pstack — from the OceanBase cluster (obdiag gather perf).

        Args:
            store_dir: Output directory
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        args: dict = {}
        if store_dir:
            args["store_dir"] = store_dir
        return _run("gather_perf", args, cfg, "Performance data gathering completed.", "Performance data gathering failed.", stdio)

    def gather_ash(
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
        """Gather ASH report / 活跃会话历史采样 (obdiag gather ash).

        Args:
            from_time: Start time yyyy-mm-dd hh:mm:ss
            to_time: End time yyyy-mm-dd hh:mm:ss
            trace_id: Filter by trace ID
            sql_id: Filter by SQL ID
            wait_class: Filter by wait class
            report_type: TEXT or HTML
            store_dir: Output directory
            svr_ip: Server IP to query
            svr_port: Server port
            tenant_id: Tenant ID
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
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
        return _run("gather_ash", args, cfg, "ASH report gathering completed.", "ASH report gathering failed.", stdio)

    def gather_awr(
        since: Optional[str] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        cluster_name: Optional[str] = None,
        cluster_id: Optional[str] = None,
        store_dir: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Gather AWR / ParalleSQL report (obdiag gather awr).

        Args:
            since: Relative window e.g. '1h'
            from_time: Start time yyyy-mm-dd hh:mm:ss
            to_time: End time yyyy-mm-dd hh:mm:ss
            cluster_name: Cluster name override
            cluster_id: Cluster ID override
            store_dir: Output directory
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
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
        return _run("gather_awr", args, cfg, "AWR gathering completed.", "AWR gathering failed.", stdio)

    def gather_plan_monitor(
        trace_id: str,
        store_dir: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Gather SQL plan monitor / 执行计划监控 for a specific trace ID (obdiag gather plan_monitor).

        Use ONLY when the user wants plan monitor/execution plan analysis, NOT for collecting logs.
        For '收集日志' or '收集traceid XXX日志', use gather_log with grep=[trace_id] instead.

        Args:
            trace_id: SQL trace ID
            store_dir: Output directory
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        args: dict = {"trace_id": trace_id}
        if store_dir:
            args["store_dir"] = store_dir
        return _run("gather_plan_monitor", args, cfg, "Plan monitor gathering completed.", "Plan monitor gathering failed.", stdio)

    def gather_scene_run(
        scene: str,
        since: Optional[str] = "30m",
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        env: Optional[List[str]] = None,
        store_dir: Optional[str] = None,
        temp_dir: Optional[str] = None,
        skip_type: Optional[str] = None,
        redact: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Run a gather scene (obdiag gather scene run).

        Use for YAML/Python-defined scene tasks under plugins/gather/tasks, e.g. observer.cluster_down,
        observer.memory, observer.io, obproxy.restart, or other.application_error.

        Args:
            scene: Gather scene name, e.g. "observer.cluster_down"; multiple scenes can be separated by ";" or ","
            since: Relative window e.g. '30m', '1h', '2d'. Ignored by obdiag when both from_time and to_time are provided.
            from_time: Start time yyyy-mm-dd hh:mm:ss
            to_time: End time yyyy-mm-dd hh:mm:ss
            env: Scene env overrides as key=value strings, e.g. host=127.0.0.1 port=2881 user=root@sys
            store_dir: Output directory
            temp_dir: Temp dir on remote nodes
            skip_type: Skip gather type, choices include ssh or sql
            redact: Desensitization options
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        args: dict = {"scene": scene}
        if from_time:
            args["from"] = from_time
        if to_time:
            args["to"] = to_time
        if since and not (from_time and to_time):
            args["since"] = since
        if env:
            args["env"] = env
        if store_dir:
            args["store_dir"] = store_dir
        if temp_dir:
            args["temp_dir"] = temp_dir
        if skip_type:
            args["skip_type"] = skip_type
        if redact:
            args["redact"] = redact
        return _run("gather_scene_run", args, cfg, "Gather scene completed.", "Gather scene failed.", stdio)

    def gather_scene_list() -> str:
        """List all available gather scenes (obdiag gather scene list).

        Call this when the user asks which gather scenes exist, or before choosing a scene name.
        """
        result = execute_obdiag_command("gather_scene_list", {}, None, stdio)
        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result["stderr"]
        return truncate_for_agent(f"Available gather scenes:\n\n{output}", label="obdiag")

    def analyze_log(
        files: Optional[List[str]] = None,
        from_time: Optional[str] = None,
        to_time: Optional[str] = None,
        scope: Optional[str] = None,
        log_level: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Run obdiag analyze log — ONLY for OceanBase cluster observer-node log types.

        Supported scope: observer/election/rootservice/all (cluster-side only).
        For OBProxy logs use gather_obproxy_log + read_file. For OMS logs use gather_oms_log + read_file.

        Args:
            files: Offline paths to observer-side log files
            from_time: Start time for analysis
            to_time: End time for analysis
            scope: observer/election/rootservice/all
            log_level: Min log level: DEBUG/TRACE/INFO/WDIAG/WARN/EDIAG/ERROR
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
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
        return _run("analyze_log", args, cfg, "Log analysis completed.", "Log analysis failed.", stdio)

    def check_cluster(
        cases: Optional[str] = None,
        obproxy_cases: Optional[str] = None,
        observer_tasks: Optional[str] = None,
        obproxy_tasks: Optional[str] = None,
        store_dir: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Run health checks on the OceanBase cluster and/or OBProxy nodes (obdiag check run).

        Use cases/observer_tasks for observer checks; obproxy_cases/obproxy_tasks for OBProxy checks.
        Omit all four to run the full default check suite (all tasks minus the filter package).

        Args:
            cases: Observer check package name (single name from observer_check_package.yaml, e.g. "ad", "k8s_basic", "deep"); mutually exclusive with observer_tasks, observer_tasks takes priority
            obproxy_cases: OBProxy check package name (single name from obproxy_check_package.yaml, e.g. "proxy"); mutually exclusive with obproxy_tasks
            observer_tasks: Specific observer task names or regex patterns (semicolon-separated), e.g. "cluster.*" or "disk.data_disk_full;cluster.no_leader"
            obproxy_tasks: Specific OBProxy task names or regex patterns (semicolon-separated)
            store_dir: Output directory
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
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
        return _run("check", args, cfg, "Health check completed.", "Health check failed.", stdio)

    def check_list() -> str:
        """List all available health check tasks.

        Args:
            cluster_config_path: Optional short name or full path for non-default cluster.
        """
        result = execute_obdiag_command("check_list", {}, None, stdio)
        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result["stderr"]
        return truncate_for_agent(f"Available check tasks:\n\n{output}", label="obdiag")

    def rca_run(
        scene: str,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Run root cause analysis for a specific scenario (obdiag rca run).

        Args:
            scene: RCA scenario name
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        return _run("rca_run", {"scene": scene}, cfg, "Root cause analysis completed.", "Root cause analysis failed.", stdio)

    def rca_list() -> str:
        """List all available root cause analysis scenarios (obdiag rca list).

        Args:
            cluster_config_path: Optional short name or full path for non-default cluster.
        """
        result = execute_obdiag_command("rca_list", {}, None, stdio)
        output = result.get("stdout", "")
        if result.get("stderr"):
            output += "\n" + result["stderr"]
        return truncate_for_agent(f"Available RCA scenarios:\n\n{output}", label="obdiag")

    def tool_io_performance(
        disk: Optional[str] = None,
        date: Optional[str] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Check disk IO performance on cluster nodes (obdiag tool io_performance).

        Args:
            disk: Disk device name e.g. 'sda', 'clog', 'data'
            date: Date for historical data YYYYMMDD
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        args: dict = {}
        if disk:
            args["disk"] = disk
        if date:
            args["date"] = date
        return _run("tool_io_performance", args, cfg, "IO performance check completed.", "IO performance check failed.", stdio)

    def tool_sql_syntax(
        sql: str,
        env: Optional[List[str]] = None,
        cluster_config_path: Optional[str] = None,
    ) -> str:
        """Validate SQL syntax/semantics using EXPLAIN/PREPARE — does not execute the statement (obdiag tool sql_syntax).

        Args:
            sql: Single SQL statement to check
            env: Optional connection overrides as key=value strings e.g. host=127.0.0.1 port=2881 user=root@sys
            cluster_config_path: Short name or full path for non-default cluster
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        args: dict = {"sql": sql}
        if env:
            args["env"] = env
        result = execute_obdiag_command("tool_sql_syntax", args, cfg, stdio, valid_params={"sql", "env"})
        return truncate_for_agent(format_command_output(result, "SQL syntax check completed.", "SQL syntax check failed."), label="obdiag")

    def list_obdiag_clusters() -> str:
        """List OceanBase cluster configs under ~/.obdiag (all *.yml/*.yaml files).

        Shows cluster name, db_host, short name for /use <name>, and whether it is the default.
        Call when the user asks which clusters exist, what configs are available, or 有哪些集群.
        """
        rows = discover_obcluster_configs()
        if not rows:
            return f"No *.yml / *.yaml files under {OBDIAG_CONFIG_DIR}. " "The default cluster file is usually ~/.obdiag/config.yml — " "create it with `obdiag config` or the generate_obdiag_config tool."
        lines: List[str] = ["Cluster config files (obdiag workspace):", ""]
        for r in rows:
            default_tag = " [default]" if r["is_default"] else ""
            short = r["short_name"]
            if not r["has_obcluster"]:
                lines.append(f"- {r['file_name']}{default_tag}: (no obcluster section yet)  → switch with: /use {short}")
                continue
            cname = r["ob_cluster_name"] or "(ob_cluster_name not set)"
            host = r["db_host"] or "-"
            lines.append(f"- {cname}{default_tag}  db_host={host}  file={r['file_name']}  → switch with: /use {short}")
        lines.extend(["", "Active config:", config_path_getter()])
        return "\n".join(lines)

    def show_current_cluster() -> str:
        """Show the currently active cluster config path.

        Returns the active obdiag config file path used for all diagnostic commands.
        """
        cfg = config_path_getter()
        return f"Active cluster config: {cfg or '(none set)'}"

    return [
        gather_log,
        gather_obproxy_log,
        gather_oms_log,
        gather_sysstat,
        gather_perf,
        gather_ash,
        gather_awr,
        gather_plan_monitor,
        gather_scene_run,
        gather_scene_list,
        analyze_log,
        check_cluster,
        check_list,
        rca_run,
        rca_list,
        tool_io_performance,
        tool_sql_syntax,
        list_obdiag_clusters,
        show_current_cluster,
    ]
