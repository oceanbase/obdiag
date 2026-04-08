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
@file: executor.py
@desc: obdiag command execution engine for the agent toolsets (src/handler/agent/).
       Single source of truth for CLI command mappings and subprocess invocation.
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
    "analyze_log": "分析集群 observer 侧日志",
    "check_cluster": "执行集群健康巡检",
    "check_list": "列出可用巡检项",
    "rca_run": "执行根因分析（RCA）",
    "rca_list": "列出 RCA 场景",
    "tool_io_performance": "检查节点磁盘 IO",
    "tool_sql_syntax": "用 EXPLAIN 验证 SQL 语法/语义（不执行）",
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
