# obdiag Agent — Deep Agents SDK Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the pydantic-ai agent implementation in `src/handler/agent/` with `deepagents` SDK (`create_deep_agent()`), eliminating ~420 lines of hand-written history/session/approval logic.

**Architecture:** `AiAgentHandler.handle()` in `handler.py` now calls `create_deep_agent()` with plain Python tool functions (closure-injected deps instead of RunContext). The resulting `CompiledStateGraph` is driven via `stream()` for the interactive REPL loop. Built-in `interrupt_on` replaces the DeferredToolRequests approval loop; `MemorySaver` checkpointer replaces hand-written session JSON.

**Tech Stack:** `deepagents==0.5.1`, `langchain>=1.2.15`, `langgraph>=1.1.6`, `langchain-anthropic` (already in `.venv`). Existing: `rich`, `prompt_toolkit`, `pydantic-ai` (removed after migration).

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| **Rewrite** | `src/handler/agent/handler.py` | `AiAgentHandler`: init, REPL, single-shot, slash cmds, stream render, interrupt |
| **Rewrite** | `src/handler/agent/toolsets/__init__.py` | Export factory functions |
| **Rewrite** | `src/handler/agent/toolsets/obdiag.py` | `create_obdiag_tools()` — 17 tools, no RunContext |
| **Rewrite** | `src/handler/agent/toolsets/database.py` | `create_db_tools()` — `db_query`, no RunContext |
| **Rewrite** | `src/handler/agent/toolsets/config_gen.py` | `create_config_gen_tools()` — no RunContext |
| **Rewrite** | `src/handler/agent/toolsets/knowledge_base.py` | `create_knowledge_tools()` — no RunContext |
| **Delete** | `src/handler/agent/agent.py` | pydantic-ai Agent definition (replaced by handler) |
| **Delete** | `src/handler/agent/toolsets/file_ops.py` | Replaced by DeepAgents built-ins |
| **Keep** | `src/handler/agent/models.py` | `AgentDependencies`, `AgentConfig` — remove pydantic-ai imports |
| **Keep** | `src/handler/agent/config.py` | `load_agent_config()` — unchanged |
| **Keep** | `src/handler/agent/executor.py` | `execute_obdiag_command()` — unchanged |
| **Keep** | `src/handler/agent/cluster_resolve.py` | Cluster path resolution — unchanged |
| **Keep** | `src/handler/agent/tool_output_limits.py` | `truncate_for_agent()` — unchanged |
| **Keep** | `src/handler/agent/toolsets/sql_validator.py` | `validate_sql()` — unchanged |
| **Modify** | `requirements.txt` | Add deepagents; remove pydantic-ai |
| **Create** | `test/handler/test_deepagent_handler.py` | Smoke tests for new handler |

---

## Task 1: Clean Up Deleted Files and Stale Imports in `models.py`

**Files:**
- Delete: `src/handler/agent/agent.py`
- Delete: `src/handler/agent/toolsets/file_ops.py`
- Modify: `src/handler/agent/models.py`

- [ ] **Step 1: Write a test that imports `models.py` without pydantic-ai**

```python
# test/handler/test_deepagent_handler.py
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
import pytest


def test_models_import_without_pydantic_ai():
    """models.py must import cleanly — no pydantic_ai dependency."""
    from src.handler.agent.models import AgentDependencies, AgentConfig
    assert AgentDependencies is not None
    assert AgentConfig is not None


def test_agent_config_from_dict():
    from src.handler.agent.models import AgentConfig
    cfg = AgentConfig.from_dict({
        "llm": {"provider": "openai", "model": "gpt-4o", "api_key": "sk-test"},
        "ui": {"tool_approval": False},
    })
    assert cfg.model == "gpt-4o"
    assert cfg.tool_approval is False
```

- [ ] **Step 2: Run test — expect PASS (models.py has no pydantic-ai imports)**

```bash
cd /data/code/oceanbase-diagnostic-tool
source .venv/bin/activate
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py::test_models_import_without_pydantic_ai test/handler/test_deepagent_handler.py::test_agent_config_from_dict -v
```

Expected: Both PASS. If FAIL with `pydantic_ai` ImportError, proceed to Step 3.

- [ ] **Step 3: Remove pydantic-ai imports from `models.py`**

Open `src/handler/agent/models.py`. Remove any `from pydantic_ai import ...` or `import pydantic_ai` lines (there are none currently — verify by checking the file header and imports). The file currently imports only `os`, `dataclasses`, `typing`, `yaml`, and `src.common.*`.

Verify:
```bash
grep -n "pydantic_ai\|pydantic-ai" src/handler/agent/models.py
```
Expected: no output.

- [ ] **Step 4: Delete `agent.py`**

```bash
rm src/handler/agent/agent.py
```

- [ ] **Step 5: Delete `toolsets/file_ops.py`**

```bash
rm src/handler/agent/toolsets/file_ops.py
```

- [ ] **Step 6: Run test again — must PASS**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py -v
```

- [ ] **Step 7: Commit**

```bash
git add test/handler/test_deepagent_handler.py
git rm src/handler/agent/agent.py src/handler/agent/toolsets/file_ops.py
git commit -m "refactor(agent): delete pydantic-ai agent.py and file_ops toolset"
```

---

## Task 2: Rewrite `toolsets/obdiag.py` — Plain Functions, No RunContext

**Files:**
- Rewrite: `src/handler/agent/toolsets/obdiag.py`

- [ ] **Step 1: Add test for `create_obdiag_tools` factory**

Append to `test/handler/test_deepagent_handler.py`:

```python
def test_create_obdiag_tools_returns_callables():
    """create_obdiag_tools() must return 17 plain callable functions."""
    from src.handler.agent.toolsets.obdiag import create_obdiag_tools

    config_path_ref = {"v": "/dev/null"}
    tools = create_obdiag_tools(lambda: config_path_ref["v"], stdio=None)
    assert len(tools) == 17
    expected_names = {
        "gather_log", "gather_obproxy_log", "gather_oms_log",
        "gather_sysstat", "gather_perf", "gather_ash", "gather_awr",
        "gather_plan_monitor", "analyze_log", "check_cluster", "check_list",
        "rca_run", "rca_list", "tool_io_performance", "tool_sql_syntax",
        "list_obdiag_clusters", "show_current_cluster",
    }
    names = {fn.__name__ for fn in tools}
    assert names == expected_names
    # Must not have pydantic-ai RunContext in any signature
    import inspect
    for fn in tools:
        sig = inspect.signature(fn)
        for param in sig.parameters.values():
            assert "RunContext" not in str(param.annotation), \
                f"{fn.__name__} param {param.name} still uses RunContext"
```

- [ ] **Step 2: Run test — expect FAIL (old file still uses RunContext)**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py::test_create_obdiag_tools_returns_callables -v
```

Expected: FAIL.

- [ ] **Step 3: Write `src/handler/agent/toolsets/obdiag.py`**

Replace the entire file with:

```python
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

       Returns plain Python functions via create_obdiag_tools().
       Dependencies injected via closure (config_path_getter), not RunContext.
"""

import os
from collections.abc import Callable
from typing import List, Optional, Union

from src.handler.agent.cluster_resolve import DEFAULT_CLUSTER_CONFIG, OBDIAG_CONFIG_DIR, resolve_cluster_config_path
from src.handler.agent.executor import execute_obdiag_command, format_command_output
from src.handler.agent.models import discover_obcluster_configs
from src.handler.agent.tool_output_limits import truncate_for_agent


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
        Omit all four to run the full default check suite.

        Args:
            cases: Observer check cases (comma-separated); overridden by observer_tasks
            obproxy_cases: OBProxy check cases (comma-separated); overridden by obproxy_tasks
            observer_tasks: Specific observer task names (comma-separated)
            obproxy_tasks: Specific OBProxy task names (comma-separated)
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

    def check_list(cluster_config_path: Optional[str] = None) -> str:
        """List all available health check tasks.

        Args:
            cluster_config_path: Optional short name or full path for non-default cluster.
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        result = execute_obdiag_command("check_list", {}, cfg, stdio)
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

    def rca_list(cluster_config_path: Optional[str] = None) -> str:
        """List all available root cause analysis scenarios (obdiag rca list).

        Args:
            cluster_config_path: Optional short name or full path for non-default cluster.
        """
        cfg = _resolve_config(cluster_config_path, config_path_getter)
        result = execute_obdiag_command("rca_list", {}, cfg, stdio)
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
        """Validate SQL syntax/semantics using EXPLAIN — does not execute the statement (obdiag tool sql_syntax).

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
            from src.handler.agent.cluster_resolve import OBDIAG_CONFIG_DIR
            return (
                f"No *.yml / *.yaml files under {OBDIAG_CONFIG_DIR}. "
                "The default cluster file is usually ~/.obdiag/config.yml — "
                "create it with `obdiag config` or the generate_obdiag_config tool."
            )
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
        gather_log, gather_obproxy_log, gather_oms_log,
        gather_sysstat, gather_perf, gather_ash, gather_awr,
        gather_plan_monitor, analyze_log, check_cluster, check_list,
        rca_run, rca_list, tool_io_performance, tool_sql_syntax,
        list_obdiag_clusters, show_current_cluster,
    ]
```

- [ ] **Step 4: Run test — must PASS**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py::test_create_obdiag_tools_returns_callables -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/handler/agent/toolsets/obdiag.py test/handler/test_deepagent_handler.py
git commit -m "refactor(agent): rewrite obdiag toolset as plain functions (no RunContext)"
```

---

## Task 3: Rewrite `toolsets/database.py` and `toolsets/config_gen.py`

**Files:**
- Rewrite: `src/handler/agent/toolsets/database.py`
- Rewrite: `src/handler/agent/toolsets/config_gen.py`

- [ ] **Step 1: Add tests**

Append to `test/handler/test_deepagent_handler.py`:

```python
def test_create_db_tools_returns_callables():
    from src.handler.agent.toolsets.database import create_db_tools
    from src.handler.agent.models import AgentDependencies

    deps = AgentDependencies(stdio=None, config_path="")
    tools = create_db_tools(lambda: deps)
    assert len(tools) == 1
    assert tools[0].__name__ == "db_query"
    import inspect
    sig = inspect.signature(tools[0])
    assert "RunContext" not in str(sig)


def test_create_config_gen_tools_returns_callables():
    from src.handler.agent.toolsets.config_gen import create_config_gen_tools
    tools = create_config_gen_tools(stdio=None)
    assert len(tools) >= 1
    import inspect
    for fn in tools:
        sig = inspect.signature(fn)
        assert "RunContext" not in str(sig)
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py::test_create_db_tools_returns_callables test/handler/test_deepagent_handler.py::test_create_config_gen_tools_returns_callables -v
```

Expected: FAIL (old files still use RunContext/FunctionToolset).

- [ ] **Step 3: Write `src/handler/agent/toolsets/database.py`**

```python
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
@file: database.py
@desc: Database query tool for Deep Agents SDK. No RunContext; deps injected via closure.
"""

import json
from collections.abc import Callable
from typing import Optional

from src.handler.agent.models import AgentDependencies
from src.handler.agent.toolsets.sql_validator import validate_sql


def create_db_tools(deps_getter: Callable[[], AgentDependencies]) -> list:
    """Return list of database tool functions.

    Args:
        deps_getter: Callable returning the current AgentDependencies (for connector access).
    """

    def db_query(sql: str, cluster_config_path: Optional[str] = None) -> str:
        """Execute a read-only SQL query on the OceanBase database.

        Only SELECT, SHOW, DESCRIBE, DESC, EXPLAIN, and WITH queries are allowed.

        Args:
            sql: Read-only SQL statement
            cluster_config_path: Optional short name or full path for non-default cluster
        """
        deps = deps_getter()
        is_valid, error_msg = validate_sql(sql)
        if not is_valid:
            return error_msg

        connector = deps.get_db_connector(cluster_config_path)
        if connector is None:
            return (
                "Error: No database connection available. "
                "Ensure ~/.obdiag/config.yml has a valid obcluster section with db_host, db_port, and credentials."
            )

        try:
            results = connector.execute_sql(sql)
            if results is None:
                return "Query executed successfully. No results returned."
            if isinstance(results, list):
                return json.dumps(results, ensure_ascii=False, default=str, indent=2)
            return str(results)
        except Exception as e:
            return f"Error executing SQL: {e}"

    return [db_query]
```

- [ ] **Step 4: Read the current `config_gen.py` to understand the generate_obdiag_config tool signature**

Read `src/handler/agent/toolsets/config_gen.py` lines 60-200 to understand what `generate_obdiag_config` does and its full parameter list, then rewrite as a closure factory.

The rewrite pattern is: replace `@config_gen_toolset.tool` decorator and `ctx: RunContext[AgentDependencies]` first parameter with a closure. The `generate_obdiag_config` function body accesses `ctx.deps.stdio` — replace with the `stdio` closure variable. No other deps are needed.

Write `src/handler/agent/toolsets/config_gen.py` — replace `FunctionToolset` and `RunContext` imports, remove `config_gen_toolset = FunctionToolset()`, add `create_config_gen_tools(stdio) -> list` factory function, move `generate_obdiag_config` inside it as a closure (removing `ctx` parameter, using `stdio` from outer scope), return `[generate_obdiag_config]`.

Keep `_validate_config_args` and all YAML generation logic unchanged.

- [ ] **Step 5: Run tests — must PASS**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py::test_create_db_tools_returns_callables test/handler/test_deepagent_handler.py::test_create_config_gen_tools_returns_callables -v
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/handler/agent/toolsets/database.py src/handler/agent/toolsets/config_gen.py test/handler/test_deepagent_handler.py
git commit -m "refactor(agent): rewrite db and config_gen toolsets as plain functions"
```

---

## Task 4: Rewrite `toolsets/knowledge_base.py` and `toolsets/__init__.py`

**Files:**
- Rewrite: `src/handler/agent/toolsets/knowledge_base.py`
- Rewrite: `src/handler/agent/toolsets/__init__.py`

- [ ] **Step 1: Add test**

Append to `test/handler/test_deepagent_handler.py`:

```python
def test_create_knowledge_tools_returns_callables():
    from src.handler.agent.toolsets.knowledge_base import create_knowledge_tools
    tools = create_knowledge_tools(token_getter=lambda: "")
    assert len(tools) == 1
    assert tools[0].__name__ == "query_oceanbase_knowledge_base"
    import inspect
    sig = inspect.signature(tools[0])
    assert "RunContext" not in str(sig)


def test_toolsets_init_exports():
    from src.handler.agent.toolsets import (
        create_obdiag_tools,
        create_db_tools,
        create_config_gen_tools,
        create_knowledge_tools,
    )
    assert callable(create_obdiag_tools)
    assert callable(create_db_tools)
    assert callable(create_config_gen_tools)
    assert callable(create_knowledge_tools)
```

- [ ] **Step 2: Run tests — expect FAIL**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py::test_create_knowledge_tools_returns_callables test/handler/test_deepagent_handler.py::test_toolsets_init_exports -v
```

- [ ] **Step 3: Rewrite `src/handler/agent/toolsets/knowledge_base.py`**

The current file has `query_oceanbase_knowledge_base` registered on `knowledge_toolset = FunctionToolset()` with `ctx: RunContext[AgentDependencies]`. The bearer token comes from `ctx.deps.oceanbase_knowledge_bearer_token`.

Replace with `create_knowledge_tools(token_getter: Callable[[], str]) -> list` factory. Inside, define `query_oceanbase_knowledge_base` as a closure that calls `token_getter()` instead of `ctx.deps.oceanbase_knowledge_bearer_token`. Keep all HTTP request logic, constants (`OCEANBASE_KNOWLEDGE_GATEWAY_BASE`, `_KNOWLEDGE_RETRIEVAL_PATH`, etc.), and docstrings unchanged. Return `[query_oceanbase_knowledge_base]`.

Remove `from pydantic_ai import FunctionToolset, RunContext` and `from src.handler.agent.models import AgentConfig, AgentDependencies`.

- [ ] **Step 4: Rewrite `src/handler/agent/toolsets/__init__.py`**

```python
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

from src.handler.agent.toolsets.config_gen import create_config_gen_tools
from src.handler.agent.toolsets.database import create_db_tools
from src.handler.agent.toolsets.knowledge_base import create_knowledge_tools
from src.handler.agent.toolsets.obdiag import create_obdiag_tools

__all__ = [
    "create_obdiag_tools",
    "create_db_tools",
    "create_config_gen_tools",
    "create_knowledge_tools",
]
```

- [ ] **Step 5: Run all tests — must PASS**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/handler/agent/toolsets/__init__.py src/handler/agent/toolsets/knowledge_base.py test/handler/test_deepagent_handler.py
git commit -m "refactor(agent): rewrite knowledge_base toolset and update __init__ for factory pattern"
```

---

## Task 5: Write the New `handler.py`

**Files:**
- Rewrite: `src/handler/agent/handler.py`

- [ ] **Step 1: Add handler smoke test**

Append to `test/handler/test_deepagent_handler.py`:

```python
def test_handler_imports_cleanly():
    """handler.py must import without errors and expose AiAgentHandler."""
    from src.handler.agent.handler import AiAgentHandler
    assert AiAgentHandler is not None


def test_handler_init_no_crash():
    """AiAgentHandler.__init__ must not crash with a minimal mock context."""
    from src.handler.agent.handler import AiAgentHandler

    class FakeStdio:
        def verbose(self, *a): pass
        def warn(self, *a): pass
        def print(self, *a, **kw): pass

    class FakeOptions:
        pass

    class FakeContext:
        stdio = FakeStdio()
        options = FakeOptions()

    handler = AiAgentHandler(FakeContext())
    assert handler._agent is None  # not initialized until handle() is called
```

- [ ] **Step 2: Run test — expect FAIL (old handler imports pydantic-ai)**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py::test_handler_imports_cleanly test/handler/test_deepagent_handler.py::test_handler_init_no_crash -v
```

Expected: FAIL with ImportError on `pydantic_ai`.

- [ ] **Step 3: Write `src/handler/agent/handler.py`**

```python
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
@file: handler.py
@desc: obdiag agent handler — Deep Agents SDK (create_deep_agent / LangGraph) implementation.

       AiAgentHandler.handle() is the entry point called by ObdiagHome (src/common/core.py).
       The handler creates a create_deep_agent() CompiledStateGraph and drives an interactive
       REPL loop (or single-shot mode) using stream().
"""

import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from deepagents import create_deep_agent
from deepagents.backends import LocalShellBackend
from langgraph.checkpoint.memory import MemorySaver
from langchain.chat_models import init_chat_model

from src.common.result_type import ObdiagResult
from src.common.tool import Util
from src.handler.agent.config import (
    DEFAULT_SKILLS_DIRECTORY,
    OBDIAG_CONFIG_PATH,
    load_agent_config,
)
from src.handler.agent.models import AgentConfig, AgentDependencies, read_obcluster_config
from src.handler.agent.executor import OBDIAG_TOOL_SUMMARY_ZH
from src.handler.agent.toolsets import (
    create_obdiag_tools,
    create_db_tools,
    create_config_gen_tools,
    create_knowledge_tools,
)

try:
    from rich.console import Console
    from rich.markdown import Markdown
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

try:
    from prompt_toolkit import prompt as pt_prompt
    from prompt_toolkit.completion import Completer, Completion
    from prompt_toolkit.history import InMemoryHistory
    from prompt_toolkit.shortcuts import CompleteStyle
    PROMPT_TOOLKIT_AVAILABLE = True

    _SLASH_COMMANDS_META: List[tuple] = [
        ("/?", "Show help message"),
        ("/clear", "Clear conversation (new session)"),
        ("/cluster", "Show current active cluster"),
        ("/exit", "Exit agent"),
        ("/help", "Show help message"),
        ("/q", "Exit agent"),
        ("/quit", "Exit agent"),
        ("/save", "Show current session ID"),
        ("/tools", "List diagnostic tools"),
        ("/use", "Switch cluster (/use <name|path>)"),
    ]

    class _SlashCompleter(Completer):
        def get_completions(self, document, complete_event):
            text = document.text_before_cursor
            if " " in text or not text.startswith("/"):
                return
            prefix = text.lower()
            for cmd, desc in _SLASH_COMMANDS_META:
                if cmd.lower().startswith(prefix):
                    yield Completion(cmd, start_position=-len(text), display_meta=desc)

    _slash_completer = _SlashCompleter()
except ImportError:
    PROMPT_TOOLKIT_AVAILABLE = False
    _slash_completer = None  # type: ignore[assignment]


# Tools that require user approval before execution
_APPROVAL_TOOLS = [
    "gather_log", "gather_obproxy_log", "gather_oms_log", "gather_sysstat",
    "gather_perf", "gather_ash", "gather_awr", "gather_plan_monitor",
    "analyze_log", "check_cluster", "rca_run", "db_query",
]

SYSTEM_PROMPT = """You are obdiag agent, an intelligent diagnostic assistant for OceanBase database.

Your capabilities include:
1. Executing obdiag diagnostic commands (gather logs, analyze, check health, RCA)
2. Analyzing diagnostic results and providing insights
3. Recommending diagnostic steps based on user descriptions
4. Explaining OceanBase concepts and troubleshooting procedures
5. Querying the OceanBase database directly using SQL
6. Generating obdiag configuration files
7. Diagnosing multiple clusters in a single session

Multi-cluster support:
- Default cluster config is ~/.obdiag/config.yml. Other configs are *.yml/*.yaml in the same directory.
- When the user asks which clusters exist, call list_obdiag_clusters first.
- Every diagnostic tool accepts an optional cluster_config_path parameter (short name or full path).
- The user may type /use <name> to switch the active cluster.

Tool selection for gather operations:
- gather_log: Observer-side logs only (observer/election/rootservice).
- gather_obproxy_log: OBProxy/obproxy/代理日志 — never use gather_log for these.
- gather_oms_log: OMS logs (Ghana, CM, supervisor, CDC/libobcdc, store) — requires oms in config.yml.
- gather_ash: ASH report / 活跃会话历史.
- gather_awr: AWR / ParalleSQL gather awr.
- gather_plan_monitor: ONLY for SQL plan monitor (执行计划监控) for a trace_id.

After a gather completes, if the user asks to analyze/分析/解读 logs:
- Use ls and read_file on the gather output directory to locate and read relevant files, then summarize.
- analyze_log is ONLY for OceanBase cluster observer-node log analysis.

User experience guidelines:
- Respond in the same language as the user's question.
- Keep responses concise. Use bullet points or tables for lists.
- When a tool fails, suggest concrete next steps.
- Only execute read-only SQL queries (SELECT, SHOW, DESCRIBE, EXPLAIN).
- Always confirm before executing potentially long-running operations."""


def _new_thread_id() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _build_model(config: AgentConfig):
    """Build a LangChain BaseChatModel from AgentConfig."""
    provider = config.provider.lower()
    model_name = config.model

    # Map provider to init_chat_model provider string
    _provider_map = {
        "openai": "openai",
        "anthropic": "anthropic",
        "gemini": "google_genai",
        "google": "google_genai",
        "deepseek": "openai",  # OpenAI-compatible
    }
    lc_provider = _provider_map.get(provider, provider)

    kwargs: Dict[str, Any] = {"model": f"{lc_provider}:{model_name}"}
    if config.api_key:
        kwargs["api_key"] = config.api_key
    if config.base_url:
        kwargs["base_url"] = config.base_url

    return init_chat_model(**kwargs)


def _print_welcome(config_dict: dict, stdio) -> None:
    ui = config_dict.get("ui", {})
    if not ui.get("show_welcome", True):
        return
    llm = config_dict.get("llm", {})
    model_str = f"{llm.get('provider', 'openai')}:{llm.get('model', 'gpt-4')}"
    stdio.print(f"\nobdiag agent (Deep Agents SDK) — model: {model_str}")
    stdio.print("Type /help for commands, /exit to quit.\n")


class AiAgentHandler:
    """obdiag agent handler — Deep Agents SDK implementation."""

    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.options = context.options
        self._deps: Optional[AgentDependencies] = None
        self._agent = None  # CompiledStateGraph, set by _init_agent()
        self._thread_id: str = ""
        self._config_path_ref: Dict[str, str] = {"v": ""}  # mutable ref for /use
        self.console = Console() if RICH_AVAILABLE else None
        self._pt_history = InMemoryHistory() if PROMPT_TOOLKIT_AVAILABLE else None

    def handle(self) -> ObdiagResult:
        """Main entry point called by ObdiagHome."""
        config_dict = load_agent_config(stdio=self.stdio)
        self._init_agent(config_dict)
        msg = Util.get_option(self.options, "m")
        if msg:
            return self._run_single_shot(msg)
        return self._run_interactive(config_dict)

    def _init_agent(self, config_dict: dict) -> None:
        """Build and store the CompiledStateGraph agent."""
        config = AgentConfig.from_dict(config_dict)

        model = _build_model(config)

        cluster_config = read_obcluster_config(config.config_path if hasattr(config, "config_path") else OBDIAG_CONFIG_PATH)
        self._deps = AgentDependencies(
            cluster_config=cluster_config,
            stdio=self.stdio,
            config_path=OBDIAG_CONFIG_PATH,
            oceanbase_knowledge_bearer_token=config.oceanbase_knowledge_bearer_token,
        )
        self._config_path_ref["v"] = self._deps.config_path

        def config_path_getter() -> str:
            return self._config_path_ref["v"]

        def deps_getter() -> AgentDependencies:
            return self._deps

        tools = (
            create_obdiag_tools(config_path_getter, self.stdio)
            + create_db_tools(deps_getter)
            + create_config_gen_tools(self.stdio)
            + (create_knowledge_tools(lambda: config.oceanbase_knowledge_bearer_token)
               if config.oceanbase_knowledge_enabled else [])
        )

        interrupt_on = {t: True for t in _APPROVAL_TOOLS} if config.tool_approval else None

        skills = None
        if config.skills_enabled:
            sd = config.skills_directory or DEFAULT_SKILLS_DIRECTORY
            if os.path.isdir(sd):
                skills = [sd]

        self._agent = create_deep_agent(
            model=model,
            tools=tools,
            system_prompt=SYSTEM_PROMPT,
            backend=LocalShellBackend(),
            checkpointer=MemorySaver(),
            interrupt_on=interrupt_on,
            skills=skills,
        )

    def _run_single_shot(self, msg: str) -> ObdiagResult:
        """Run a single-turn query and return the result."""
        self._thread_id = _new_thread_id()
        lc_config = {"configurable": {"thread_id": self._thread_id}}
        result = self._agent.invoke(
            {"messages": [{"role": "user", "content": msg}]},
            config=lc_config,
        )
        messages = result.get("messages", [])
        text = ""
        if messages:
            last = messages[-1]
            text = last.content if hasattr(last, "content") else str(last)
        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": text})

    def _run_interactive(self, config_dict: dict) -> ObdiagResult:
        """Run the interactive REPL loop."""
        self._thread_id = _new_thread_id()
        _print_welcome(config_dict, self.stdio)
        prompt_str = config_dict.get("ui", {}).get("prompt", "obdiag agent> ")

        while True:
            try:
                user_input = self._read_input(prompt_str)
            except (EOFError, KeyboardInterrupt):
                self.stdio.print("\nExiting agent session.")
                break

            if not user_input.strip():
                continue

            slash_result = self._handle_slash(user_input.strip())
            if slash_result == "exit":
                break
            if slash_result == "handled":
                continue

            # Normal chat turn
            self._chat_and_render(user_input)

        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"message": "Agent session ended"})

    def _read_input(self, prompt_str: str) -> str:
        """Read a line of input from the user."""
        if PROMPT_TOOLKIT_AVAILABLE:
            return pt_prompt(
                prompt_str,
                history=self._pt_history,
                completer=_slash_completer,
                complete_style=CompleteStyle.READLINE_LIKE,
            )
        return input(prompt_str)

    def _handle_slash(self, text: str) -> Optional[str]:
        """Handle /command inputs.

        Returns:
            "exit"    — caller should break the REPL loop
            "handled" — command processed, skip chat
            None      — not a slash command, pass to chat
        """
        if not text.startswith("/"):
            return None

        parts = text.split(None, 1)
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else ""

        if cmd in ("/exit", "/quit", "/q"):
            return "exit"

        if cmd in ("/help", "/?"):
            self.stdio.print(
                "\nAvailable commands:\n"
                "  /help, /?          — show this help\n"
                "  /exit, /quit, /q   — exit agent\n"
                "  /clear             — clear conversation (start new session)\n"
                "  /cluster           — show current active cluster\n"
                "  /use <name|path>   — switch active cluster\n"
                "  /tools             — list registered tools\n"
                "  /save              — show current session ID\n"
            )
            return "handled"

        if cmd == "/clear":
            self._thread_id = _new_thread_id()
            self.stdio.print(f"Conversation cleared. New session: {self._thread_id}")
            return "handled"

        if cmd == "/cluster":
            self.stdio.print(self._deps.current_cluster_info() if self._deps else "(not initialized)")
            return "handled"

        if cmd == "/use":
            if not arg.strip():
                self.stdio.print("Usage: /use <cluster_name_or_config_path>")
                return "handled"
            ok, msg = self._deps.switch_cluster(arg.strip())
            if ok:
                self._config_path_ref["v"] = self._deps.config_path
            self.stdio.print(msg)
            return "handled"

        if cmd == "/tools":
            if self._agent is None:
                self.stdio.print("(agent not initialized)")
                return "handled"
            # List tools from the agent's tool node
            self.stdio.print("\nRegistered tools:")
            tool_names = sorted(OBDIAG_TOOL_SUMMARY_ZH.keys())
            for name in tool_names:
                summary = OBDIAG_TOOL_SUMMARY_ZH.get(name, "")
                self.stdio.print(f"  {name:<30} {summary}")
            self.stdio.print("  (+ DeepAgents built-ins: ls, read_file, write_file, edit_file, glob, grep, execute, write_todos, task)")
            return "handled"

        if cmd == "/save":
            self.stdio.print(f"Session ID: {self._thread_id}")
            return "handled"

        # Unknown slash command — pass to chat (LLM may handle it)
        return None

    def _chat_and_render(self, user_input: str) -> None:
        """Send user input to the agent, stream response, handle interrupts, render output."""
        lc_config = {"configurable": {"thread_id": self._thread_id}}
        response_text = ""

        try:
            for chunk in self._agent.stream(
                {"messages": [{"role": "user", "content": user_input}]},
                config=lc_config,
                stream_mode="updates",
            ):
                for node_name, update in chunk.items():
                    if node_name == "__interrupt__":
                        # tool approval interrupt
                        response_text = self._handle_interrupt(update, lc_config)
                        self._render(response_text)
                        return
                    if not isinstance(update, dict):
                        continue
                    for msg in update.get("messages", []):
                        if not hasattr(msg, "content"):
                            continue
                        content = msg.content
                        if not isinstance(content, str) or not content:
                            continue
                        # Tool call messages: print trace line
                        if getattr(msg, "tool_calls", None) or getattr(msg, "type", "") == "tool":
                            tool_name = getattr(msg, "name", None) or ""
                            summary = OBDIAG_TOOL_SUMMARY_ZH.get(tool_name, f"调用工具({tool_name})" if tool_name else "")
                            if summary:
                                self.stdio.print(f"  → {summary}")
                        else:
                            response_text += content
        except KeyboardInterrupt:
            self.stdio.print("\n[Interrupted]")
            return
        except Exception as e:
            self.stdio.print(f"\nError during agent run: {e}")
            return

        self._render(response_text)

    def _handle_interrupt(self, interrupt_value: Any, lc_config: dict) -> str:
        """Show tool approval prompt and resume the agent with the user's decision."""
        from langchain.agents.middleware.human_in_the_loop import ApproveDecision, RejectDecision

        # interrupt_value is a HITLRequest dict: {action_requests: [...], review_configs: [...]}
        action_requests = []
        if isinstance(interrupt_value, dict):
            action_requests = interrupt_value.get("action_requests", [])
        elif isinstance(interrupt_value, (list, tuple)) and len(interrupt_value) > 0:
            # LangGraph wraps interrupts in a list of Interrupt namedtuples
            first = interrupt_value[0]
            val = getattr(first, "value", first)
            if isinstance(val, dict):
                action_requests = val.get("action_requests", [])

        # Display each pending tool call
        self.stdio.print("")
        for req in action_requests:
            name = req.get("name", "?") if isinstance(req, dict) else getattr(req, "name", "?")
            args = req.get("args", {}) if isinstance(req, dict) else getattr(req, "args", {})
            args_preview = json.dumps(args, ensure_ascii=False)[:300]
            self.stdio.print(f"[Tool approval required] {name}")
            self.stdio.print(f"  Args: {args_preview}")

        try:
            decision_input = input("Execute? [y/n]: ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            decision_input = "n"

        from langgraph.types import Command

        if decision_input == "y":
            decisions = [{"type": "approve"} for _ in action_requests] if action_requests else [{"type": "approve"}]
        else:
            decisions = [{"type": "reject"} for _ in action_requests] if action_requests else [{"type": "reject"}]

        resume_cmd = Command(resume={"decisions": decisions})

        response_text = ""
        try:
            for chunk in self._agent.stream(resume_cmd, config=lc_config, stream_mode="updates"):
                for node_name, update in chunk.items():
                    if not isinstance(update, dict):
                        continue
                    for msg in update.get("messages", []):
                        content = getattr(msg, "content", "")
                        if isinstance(content, str) and content:
                            if not getattr(msg, "tool_calls", None):
                                response_text += content
        except Exception as e:
            return f"Error resuming after approval: {e}"

        return response_text

    def _render(self, text: str) -> None:
        """Render response text — Markdown if rich available, plain text otherwise."""
        if not text:
            return
        if RICH_AVAILABLE and self.console:
            self.console.print(Markdown(text))
        else:
            self.stdio.print(text)
```

- [ ] **Step 4: Run tests — must PASS**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py::test_handler_imports_cleanly test/handler/test_deepagent_handler.py::test_handler_init_no_crash -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/handler/agent/handler.py test/handler/test_deepagent_handler.py
git commit -m "feat(agent): rewrite handler.py using create_deep_agent() Deep Agents SDK"
```

---

## Task 6: Update `__init__.py` and `requirements.txt`

**Files:**
- Modify: `src/handler/agent/__init__.py`
- Modify: `requirements.txt`

- [ ] **Step 1: Check current `__init__.py`**

```bash
cat src/handler/agent/__init__.py
```

It should export `AiAgentHandler`. If it imports from `agent.py` (deleted), fix the import.

- [ ] **Step 2: Fix `src/handler/agent/__init__.py` if needed**

The file should contain:

```python
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

from src.handler.agent.handler import AiAgentHandler

__all__ = ["AiAgentHandler"]
```

- [ ] **Step 3: Update `requirements.txt`**

```bash
# Check current pydantic-ai entries
grep -n "pydantic" requirements.txt
```

Remove lines containing `pydantic-ai` and `pydantic-ai-skills`. Add deepagents dependencies if not already present:

```
deepagents>=0.5.1
langchain>=1.2.0
langchain-core>=1.2.0
langgraph>=1.1.0
langchain-anthropic>=0.1.0
```

- [ ] **Step 4: Run all tests**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py -v
```

Expected: all PASS.

- [ ] **Step 5: Verify core.py import still works**

```bash
PYTHONPATH=$(pwd) python -c "
from src.handler.agent import AiAgentHandler
from src.handler.agent.handler import AiAgentHandler as H2
print('AiAgentHandler import OK:', AiAgentHandler is H2)
"
```

Expected: `AiAgentHandler import OK: True`

- [ ] **Step 6: Commit**

```bash
git add src/handler/agent/__init__.py requirements.txt
git commit -m "chore(agent): update __init__ export and requirements for Deep Agents SDK"
```

---

## Task 7: Integration Smoke Test

**Files:**
- Test only (no code changes)

- [ ] **Step 1: Verify no pydantic-ai imports remain in agent directory**

```bash
grep -r "pydantic_ai\|from pydantic_ai\|import pydantic_ai" src/handler/agent/ || echo "CLEAN"
```

Expected: `CLEAN`

- [ ] **Step 2: Verify create_deep_agent builds without error (no LLM call)**

```bash
source .venv/bin/activate
PYTHONPATH=$(pwd) python -c "
from deepagents import create_deep_agent
from deepagents.backends import LocalShellBackend
from langgraph.checkpoint.memory import MemorySaver
from langchain_anthropic import ChatAnthropic
from src.handler.agent.toolsets import (
    create_obdiag_tools, create_db_tools, create_config_gen_tools
)
from src.handler.agent.models import AgentDependencies

deps = AgentDependencies(stdio=None, config_path='')
config_path_ref = {'v': ''}

tools = (
    create_obdiag_tools(lambda: config_path_ref['v'], stdio=None)
    + create_db_tools(lambda: deps)
    + create_config_gen_tools(stdio=None)
)
print(f'Tools registered: {len(tools)}')

model = ChatAnthropic(model='claude-sonnet-4-6', api_key='dummy')
agent = create_deep_agent(
    model=model,
    tools=tools,
    backend=LocalShellBackend(),
    checkpointer=MemorySaver(),
)
print('Agent created:', type(agent).__name__)
print('SMOKE TEST PASSED')
"
```

Expected output:
```
Tools registered: 20
Agent created: CompiledStateGraph
SMOKE TEST PASSED
```

(20 = 17 obdiag + 1 db_query + 1 generate_obdiag_config + 1 from config_gen; adjust assertion if count differs)

- [ ] **Step 3: Run the full test suite**

```bash
PYTHONPATH=$(pwd) python -m pytest test/handler/test_deepagent_handler.py -v
```

Expected: all PASS.

- [ ] **Step 4: Run existing handler tests if any**

```bash
PYTHONPATH=$(pwd) python -m pytest test/ -k "agent" -v 2>&1 | head -40
```

Check for regressions. Fix any failures caused by removed pydantic-ai imports.

- [ ] **Step 5: Final commit**

```bash
git add -A
git commit -m "test(agent): add integration smoke test for Deep Agents SDK migration"
```

---

## Self-Review

**Spec coverage:**
- ✅ handler.py rewrite with `create_deep_agent()` — Task 5
- ✅ Toolsets as plain functions, no RunContext — Tasks 2, 3, 4
- ✅ `interrupt_on` / tool approval — Task 5 (`_handle_interrupt`)
- ✅ `/use`, `/clear`, `/cluster`, `/tools`, `/help`, `/exit` slash commands — Task 5
- ✅ Single-shot mode (`-m` flag) — Task 5
- ✅ `MemorySaver` checkpointer — Task 5
- ✅ `LocalShellBackend` — Task 5
- ✅ Skills support — Task 5
- ✅ Delete `agent.py`, `file_ops.py` — Task 1
- ✅ `requirements.txt` update — Task 6
- ✅ `__init__.py` export — Task 6
- ✅ Smoke tests — Task 7

**Type consistency check:**
- `config_path_getter: Callable[[], str]` — used consistently in Tasks 2 and 5
- `deps_getter: Callable[[], AgentDependencies]` — Task 3 and Task 5
- `_config_path_ref: Dict[str, str]` with key `"v"` — Task 5 handler `_init_agent` and `_handle_slash(/use)`
- `interrupt_on={t: True for t in _APPROVAL_TOOLS}` — matches `HumanInTheLoopMiddleware` API (bool → `["approve","edit","reject"]`)
- `Command(resume={"decisions": [...]})` — matches `HITLRequest` + `decisions` key from `HumanInTheLoopMiddleware` source

**Placeholder scan:** No TBD/TODO in task steps. Task 3 Step 4 (config_gen rewrite) describes the transformation pattern in prose rather than full code — this is intentional because the file is long (~200 lines) and the transformation is mechanical (remove decorator + ctx param, wrap in factory). The pattern is unambiguous.
