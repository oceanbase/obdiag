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
from typing import Any, Dict, Optional, Tuple

import yaml

from src.common.ob_connector import OBConnector
from src.handler.agent.cluster_resolve import DEFAULT_CLUSTER_CONFIG, resolve_cluster_config_path


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


def _build_connector_from_cluster_config(
    cluster_config: Dict[str, Any],
) -> Optional[OBConnector]:
    """
    Create an OBConnector from a cluster_config dict.

    The dict is expected to match the obcluster section of obdiag's config.yml:
        {db_host, db_port, tenant_sys: {user, password}, ...}

    Returns None if required fields are missing or connection fails.
    """
    db_host = cluster_config.get("db_host")
    db_port = cluster_config.get("db_port")
    tenant_sys = cluster_config.get("tenant_sys", {})
    username = tenant_sys.get("user", "root@sys")
    password = tenant_sys.get("password")

    if not all([db_host, db_port, username, password]):
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
            return False, (
                f"Config file not found under ~/.obdiag/ "
                f"(tried: {config_path.strip()}, {config_path.strip()}.yml)"
            )

        cluster_config = _load_cluster_config_from_file(abs_path)
        if not cluster_config:
            return False, f"No 'obcluster' section found in {abs_path}"

        self.config_path = abs_path
        self.cluster_config = cluster_config
        # Invalidate cached connector for this path so next call reconnects
        self._connector_cache.pop(abs_path, None)

        cluster_name = cluster_config.get("ob_cluster_name", abs_path)
        return True, f"Switched to cluster '{cluster_name}' using config {abs_path}"

    def current_cluster_info(self) -> str:
        """Return a human-readable summary of the active cluster."""
        if not self.cluster_config:
            return "No cluster configured."

        name = self.cluster_config.get("ob_cluster_name", "(unnamed)")
        host = self.cluster_config.get("db_host", "?")
        port = self.cluster_config.get("db_port", "?")
        config = self.config_path or "~/.obdiag/config.yml (default)"
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
        cache_key = ""
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
    """Configuration for the obdiag agent, parsed from ~/.obdiag/ai.yml."""

    # LLM
    provider: str = "openai"
    api_key: str = ""
    base_url: Optional[str] = None
    model: str = "gpt-4"
    temperature: float = 0.7
    max_tokens: int = 2000
    system_prompt: Optional[str] = None

    # MCP
    mcp_enabled: bool = True
    mcp_servers: Dict[str, Any] = field(default_factory=dict)

    # UI
    show_welcome: bool = True
    show_beta_warning: bool = True
    clear_screen: bool = True
    prompt: str = "obdiag agent> "

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "AgentConfig":
        """Create AgentConfig from a configuration dictionary."""
        llm = config_dict.get("llm", {})
        mcp = config_dict.get("mcp", {})
        ui = config_dict.get("ui", {})

        return cls(
            provider=llm.get("provider", llm.get("api_type", "openai")),
            api_key=llm.get("api_key", ""),
            base_url=(llm.get("base_url") or "").strip() or None,
            model=(llm.get("model") or "gpt-4").strip(),
            temperature=llm.get("temperature", 0.7),
            max_tokens=llm.get("max_tokens", 2000),
            system_prompt=llm.get("system_prompt") or None,
            mcp_enabled=mcp.get("enabled", True),
            mcp_servers=mcp.get("servers", {}),
            show_welcome=ui.get("show_welcome", True),
            show_beta_warning=ui.get("show_beta_warning", True),
            clear_screen=ui.get("clear_screen", True),
            prompt=ui.get("prompt", "obdiag agent> "),
        )
