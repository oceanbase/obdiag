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
@file: models.py
@desc: Data models for the obdiag agent
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.common.ob_connector import OBConnector
from src.handler.agent.cluster_resolve import (
    DEFAULT_CLUSTER_CONFIG,
    OBDIAG_CONFIG_DIR,
    resolve_cluster_config_path,
)


def read_obcluster_config(config_path: str) -> Dict[str, Any]:
    """Load the ``obcluster`` section from an obdiag YAML file (public API)."""
    return _load_cluster_config_from_file(config_path)


def _load_cluster_config_from_file(config_path: str) -> Dict[str, Any]:
    """
    Load obcluster section from an obdiag config.yml file.

    Returns the obcluster dict, or an empty dict if unavailable.
    """
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        return raw.get("obcluster", {})
    except Exception:
        return {}


def discover_obcluster_configs() -> List[Dict[str, Any]]:
    """
    List ``*.yml`` / ``*.yaml`` files in the obdiag workspace root that can hold cluster config.

    Each entry includes path, short name for ``/use <name>`` in the agent REPL, whether it is the default
    ``config.yml``, and ``ob_cluster_name`` / ``db_host`` when an ``obcluster`` section exists.
    """
    entries: List[Dict[str, Any]] = []
    root = OBDIAG_CONFIG_DIR
    default_abs = os.path.abspath(DEFAULT_CLUSTER_CONFIG)
    if not os.path.isdir(root):
        return entries

    for name in sorted(os.listdir(root), key=str.lower):
        if name.startswith("."):
            continue
        if not (name.endswith(".yml") or name.endswith(".yaml")):
            continue
        path = os.path.join(root, name)
        if not os.path.isfile(path):
            continue
        abs_path = os.path.abspath(path)
        ob = _load_cluster_config_from_file(abs_path)
        is_default = abs_path == default_abs
        stem = name[: -len(".yaml")] if name.endswith(".yaml") else name[: -len(".yml")]
        entries.append(
            {
                "path": abs_path,
                "file_name": name,
                "short_name": stem,
                "is_default": is_default,
                "ob_cluster_name": ob.get("ob_cluster_name") or "",
                "db_host": ob.get("db_host") or "",
                "has_obcluster": bool(ob),
            }
        )

    entries.sort(key=lambda e: (not e["is_default"], e["file_name"].lower()))
    return entries


def _build_connector_from_cluster_config(
    cluster_config: Dict[str, Any],
) -> Optional[OBConnector]:
    """
    Create an OBConnector from a cluster_config dict.

    The dict is expected to match the obcluster section of obdiag's config.yml:
        {db_host, db_port, tenant_sys: {user, password}, ...}

    ``tenant_sys.password`` may be omitted or an empty string (valid when the
    cluster allows blank passwords).

    Returns None if required fields are missing or connection fails.
    """
    db_host = cluster_config.get("db_host")
    db_port = cluster_config.get("db_port")
    tenant_sys = cluster_config.get("tenant_sys", {})
    username = tenant_sys.get("user", "root@sys")
    password = tenant_sys.get("password")
    if password is None:
        password = ""

    if not db_host or not db_port or not username:
        return None

    try:
        return OBConnector(
            context=None,
            ip=db_host,
            port=db_port,
            username=username,
            password=password,
            timeout=100,
        )
    except Exception:
        return None


@dataclass
class AgentDependencies:
    """
    Runtime dependencies injected into every tool via RunContext.

    Multi-cluster support
    ----------------------
    ``config_path`` is the *active* obdiag config.yml that all obdiag CLI
    commands use.  ``cluster_config`` is the corresponding in-memory cluster
    dict for direct DB connections.

    Tools can override both on a per-call basis by passing an explicit
    ``cluster_config_path`` argument.  Connectors are cached per config_path
    in ``_connector_cache`` so repeated queries to the same cluster are cheap.
    """

    cluster_config: Dict[str, Any] = field(default_factory=dict)
    stdio: Any = None
    config_path: str = ""
    # Cache: config_path -> OBConnector (keyed by config file path, or "" for
    # the default cluster_config that was supplied at construction time).
    _connector_cache: Dict[str, OBConnector] = field(default_factory=dict, repr=False)

    # OceanBase official knowledge gateway (Bearer from agent.toml); used by query_oceanbase_knowledge_base.
    oceanbase_knowledge_bearer_token: str = ""

    # ------------------------------------------------------------------
    # Active cluster management
    # ------------------------------------------------------------------

    def switch_cluster(self, config_path: str) -> Tuple[bool, str]:
        """
        Switch the active cluster to the one described in *config_path*.

        config_path can be:
        - A short name (e.g., "obdiag_test") -> resolves to ~/.obdiag/obdiag_test.yml
        - A full path (e.g., ~/.obdiag/obdiag_test.yml or /path/to/config.yml)

        Returns (success, message).
        """
        if not config_path:
            return False, "config_path must be non-empty"

        abs_path = resolve_cluster_config_path(config_path)
        if not abs_path:
            if "/" in config_path or config_path.strip().startswith("~"):
                abs_path = os.path.abspath(os.path.expanduser(config_path))
                return False, f"Config file not found: {abs_path}"
            return False, (f"Config file not found under ~/.obdiag/ " f"(tried: {config_path.strip()}, {config_path.strip()}.yml)")

        cluster_config = _load_cluster_config_from_file(abs_path)
        if not cluster_config:
            return False, f"No 'obcluster' section found in {abs_path}"

        # Invalidate the old default-path connector before switching
        self._connector_cache.pop(self.config_path or "", None)
        self.config_path = abs_path
        self.cluster_config = cluster_config
        # Invalidate cached connector for the new path so next call reconnects
        self._connector_cache.pop(abs_path, None)

        cluster_name = cluster_config.get("ob_cluster_name", abs_path)
        return True, f"Switched to cluster '{cluster_name}' using config {abs_path}"

    def current_cluster_info(self) -> str:
        """Return a human-readable summary of the active cluster."""
        cfg = self.config_path or DEFAULT_CLUSTER_CONFIG
        if not self.cluster_config:
            if cfg and os.path.isfile(cfg):
                return f"Active config file: {cfg}\n" "The file exists but has no usable ``obcluster`` section (missing or empty). " "Run ``list_obdiag_clusters`` to see all configs or fix the YAML."
            return "No cluster configured. Default is ~/.obdiag/config.yml when using obdiag. " "Run ``list_obdiag_clusters`` to discover existing config files."

        name = self.cluster_config.get("ob_cluster_name", "(unnamed)")
        host = self.cluster_config.get("db_host", "?")
        port = self.cluster_config.get("db_port", "?")
        config = self.config_path or DEFAULT_CLUSTER_CONFIG
        return f"Active cluster: {name}  host={host}:{port}  config={config}"

    # ------------------------------------------------------------------
    # Connector management
    # ------------------------------------------------------------------

    def get_db_connector(
        self,
        cluster_config_path: Optional[str] = None,
    ) -> Optional[OBConnector]:
        """
        Get or create a database connector.

        cluster_config_path can be a short name (e.g., "obdiag_test") or full path.
        Short names resolve to ~/.obdiag/{name}.yml.

        Priority order:
        1. If *cluster_config_path* is given, load cluster config from that
           file and return a (cached) connector for it.
        2. Otherwise use ``self.cluster_config`` (the active cluster).
        """
        if cluster_config_path:
            abs_path = resolve_cluster_config_path(cluster_config_path)
            if not abs_path:
                if "/" in cluster_config_path or str(cluster_config_path).strip().startswith("~"):
                    abs_path = os.path.abspath(os.path.expanduser(cluster_config_path))
                else:
                    return None

            if abs_path in self._connector_cache:
                return self._connector_cache[abs_path]

            cluster_config = _load_cluster_config_from_file(abs_path)
            if not cluster_config:
                return None

            connector = _build_connector_from_cluster_config(cluster_config)
            if connector:
                self._connector_cache[abs_path] = connector
            return connector

        # Default path: use constructor-supplied cluster_config, or load from config_path
        cache_key = self.config_path or ""
        if cache_key in self._connector_cache:
            return self._connector_cache[cache_key]

        cluster_config = self.cluster_config
        config_path = self.config_path or DEFAULT_CLUSTER_CONFIG
        if not cluster_config and config_path and os.path.isfile(config_path):
            cluster_config = _load_cluster_config_from_file(config_path)
        if not cluster_config:
            return None

        connector = _build_connector_from_cluster_config(cluster_config)
        if connector:
            self._connector_cache[cache_key] = connector
        return connector

    def close(self):
        """Clean up all cached connectors."""
        for connector in self._connector_cache.values():
            if hasattr(connector, "conn") and connector.conn:
                try:
                    connector.conn.close()
                except Exception:
                    pass
        self._connector_cache.clear()


@dataclass
class AgentConfig:
    """Configuration for the obdiag agent, parsed from ~/.obdiag/config/agent.toml.

    LLM / model settings are NOT stored here — they use deepagents-cli's native
    [models.providers.*] format and are read directly by deepagents-cli's
    ModelConfig / create_model().
    """

    # MCP
    mcp_enabled: bool = True
    mcp_servers: Dict[str, Any] = field(default_factory=dict)

    # Skills
    skills_enabled: bool = True
    skills_directory: str = ""  # Resolved to ~/.obdiag/agent/skills/ when empty
    skills_validate: bool = True
    skills_script_timeout: int = 60
    # When False, ``run_skill_script`` is not registered (avoids LLMs passing args as JSON string).
    skills_run_script_tool: bool = False

    # UI
    show_welcome: bool = True
    show_beta_warning: bool = True
    clear_screen: bool = True
    prompt: str = "obdiag agent> "
    tool_approval: bool = True  # Ask user before executing tools (human-in-the-loop)
    stream_output: bool = False  # Stream agent response token by token
    show_usage_after_turn: bool = False  # Print token usage after each turn
    show_tool_trace: bool = True  # Print tool trace lines during execution
    show_usage_cost: bool = False  # Show cost placeholder in usage footer
    auto_compact: bool = True  # Auto-compact history when context window is nearly full
    context_window_tokens: Optional[int] = None  # Model context window size for auto-compact
    auto_compact_threshold_ratio: float = 0.85  # Fraction of context_window_tokens that triggers compact
    auto_compact_min_messages: int = 2  # Minimum messages required before auto-compact fires

    # OceanBase knowledge gateway
    oceanbase_knowledge_enabled: bool = False
    oceanbase_knowledge_bearer_token: str = ""

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "AgentConfig":
        """Create AgentConfig from a configuration dictionary."""
        mcp = config_dict.get("mcp", {})
        skills = config_dict.get("skills", {})
        ui = config_dict.get("ui", {})
        ok = config_dict.get("oceanbase_knowledge") or {}
        kb_token = (ok.get("bearer_token") or "").strip() if isinstance(ok, dict) else ""
        kb_enabled = bool(ok.get("enabled", False)) if isinstance(ok, dict) else False

        return cls(
            mcp_enabled=mcp.get("enabled", True),
            mcp_servers=mcp.get("servers", {}),
            skills_enabled=skills.get("enabled", True),
            skills_directory=(skills.get("directory") or "").strip(),
            skills_validate=skills.get("validate", True),
            skills_script_timeout=skills.get("script_timeout", 60),
            skills_run_script_tool=skills.get("run_script_tool", False),
            show_welcome=ui.get("show_welcome", True),
            show_beta_warning=ui.get("show_beta_warning", True),
            clear_screen=ui.get("clear_screen", True),
            prompt=ui.get("prompt", "obdiag agent> "),
            tool_approval=ui.get("tool_approval", True),
            stream_output=ui.get("stream_output", False),
            show_usage_after_turn=ui.get("show_usage_after_turn", False),
            show_tool_trace=ui.get("show_tool_trace", True),
            show_usage_cost=ui.get("show_usage_cost", False),
            auto_compact=ui.get("auto_compact", True),
            context_window_tokens=ui.get("context_window_tokens"),
            auto_compact_threshold_ratio=ui.get("auto_compact_threshold_ratio", 0.85),
            auto_compact_min_messages=ui.get("auto_compact_min_messages", 2),
            oceanbase_knowledge_enabled=kb_enabled,
            oceanbase_knowledge_bearer_token=kb_token,
        )
