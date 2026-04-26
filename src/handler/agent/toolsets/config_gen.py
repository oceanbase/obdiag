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
@file: config_gen.py
@desc: obdiag config generation tool for Deep Agents SDK. No RunContext; stdio injected via closure.
"""

import os
import shutil
from datetime import datetime
from typing import Any, Dict, List, Optional

import yaml

from src.common.constant import obdiag_path


def _validate_config_args(arguments: Dict[str, Any]) -> Optional[str]:
    """Return an error message string if required fields are missing, else None."""
    missing = []
    required = ["ob_cluster_name", "db_host", "tenant_sys_password", "nodes"]

    for param in required:
        if param not in arguments or arguments[param] is None:
            missing.append(param)
        elif param == "nodes":
            nodes = arguments.get("nodes", [])
            if not nodes:
                missing.append("nodes (at least one node with ip is required)")

    nodes = arguments.get("nodes", [])
    if nodes:
        for i, node in enumerate(nodes):
            if not node.get("ip"):
                missing.append(f"nodes[{i}].ip")

    # has_global_ssh is True when a global SSH username is set — password may be empty (passwordless SSH is valid).
    has_global_ssh = bool(arguments.get("global_ssh_username"))
    has_global_home = arguments.get("global_home_path")

    if nodes and not has_global_ssh:
        for i, node in enumerate(nodes):
            if not node.get("ssh_username") and not arguments.get("global_ssh_username"):
                missing.append(f"global_ssh_username or nodes[{i}].ssh_username")
                break

    if nodes and not has_global_home:
        for i, node in enumerate(nodes):
            if not node.get("home_path") and not arguments.get("global_home_path"):
                missing.append(f"global_home_path or nodes[{i}].home_path")
                break

    if not missing:
        return None

    msg = "Cannot generate config file. Missing required parameters:\n\n"
    for p in missing:
        msg += f"  - {p}\n"
    msg += (
        "\nPlease provide:\n"
        "  1. ob_cluster_name (required)\n"
        "  2. db_host (required)\n"
        "  3. tenant_sys_password (required, can be '')\n"
        "  4. nodes with at least 'ip' each (required)\n"
        "  5. SSH credentials (global or per-node)\n"
        "  6. home_path (global or per-node)\n"
    )
    return msg


def _build_config(arguments: Dict[str, Any]) -> Dict[str, Any]:
    """Build a config dict suitable for yaml.dump from the tool arguments."""
    obcluster: Dict[str, Any] = {
        "ob_cluster_name": arguments.get("ob_cluster_name"),
        "db_host": arguments.get("db_host"),
        "db_port": arguments.get("db_port", 2881),
        "tenant_sys": {
            "user": arguments.get("tenant_sys_user", "root@sys"),
            "password": arguments.get("tenant_sys_password", ""),
        },
        "servers": {"nodes": [], "global": {}},
    }

    for node in arguments.get("nodes", []):
        nc: Dict[str, Any] = {"ip": node.get("ip")}
        for key in ("ssh_username", "ssh_password", "home_path", "data_dir", "redo_dir"):
            if node.get(key):
                nc[key] = node[key]
        obcluster["servers"]["nodes"].append(nc)

    g: Dict[str, Any] = {}
    for src_key, dst_key in [
        ("global_ssh_username", "ssh_username"),
        ("global_ssh_password", "ssh_password"),
        ("global_ssh_port", "ssh_port"),
        ("global_ssh_key_file", "ssh_key_file"),
        ("global_home_path", "home_path"),
        ("global_data_dir", "data_dir"),
        ("global_redo_dir", "redo_dir"),
    ]:
        if arguments.get(src_key):
            g[dst_key] = arguments[src_key]
    if g:
        obcluster["servers"]["global"] = g

    config: Dict[str, Any] = {"obcluster": obcluster}

    if arguments.get("obproxy_cluster_name") or arguments.get("obproxy_nodes"):
        obproxy: Dict[str, Any] = {
            "obproxy_cluster_name": arguments.get("obproxy_cluster_name", "obproxy"),
            "servers": {"nodes": [], "global": {}},
        }
        for node in arguments.get("obproxy_nodes", []):
            nc = {}
            for key in ("ip", "ssh_username", "ssh_password", "ssh_key_file", "home_path"):
                if node.get(key):
                    nc[key] = node[key]
            if nc:
                obproxy["servers"]["nodes"].append(nc)

        pg: Dict[str, Any] = {}
        for src_key, dst_key in [
            ("obproxy_global_ssh_username", "ssh_username"),
            ("obproxy_global_ssh_password", "ssh_password"),
            ("obproxy_global_ssh_port", "ssh_port"),
            ("obproxy_global_home_path", "home_path"),
        ]:
            if arguments.get(src_key):
                pg[dst_key] = arguments[src_key]
        if pg:
            obproxy["servers"]["global"] = pg

        config["obproxy"] = obproxy

    return config


def create_config_gen_tools(stdio: Any = None) -> list:
    """Return list of config generation tool functions.

    Args:
        stdio: Optional stdio object for logging.
    """

    def generate_obdiag_config(
        ob_cluster_name: str,
        db_host: str,
        tenant_sys_password: str,
        nodes: List[Dict[str, Any]],
        db_port: int = 2881,
        tenant_sys_user: str = "root@sys",
        global_ssh_username: Optional[str] = None,
        global_ssh_password: Optional[str] = None,
        global_ssh_port: int = 22,
        global_ssh_key_file: Optional[str] = None,
        global_home_path: Optional[str] = None,
        global_data_dir: Optional[str] = None,
        global_redo_dir: Optional[str] = None,
        obproxy_cluster_name: Optional[str] = None,
        obproxy_nodes: Optional[List[Dict[str, Any]]] = None,
        obproxy_global_ssh_username: Optional[str] = None,
        obproxy_global_ssh_password: Optional[str] = None,
        obproxy_global_ssh_port: int = 22,
        obproxy_global_home_path: Optional[str] = None,
    ) -> str:
        """
        Generate obdiag configuration file (~/.obdiag/config.yml).

        Automatically backs up existing config if present.

        Args:
            ob_cluster_name: OceanBase cluster name
            db_host: Database host IP address
            tenant_sys_password: System tenant password
            nodes: Server nodes, each dict must contain 'ip'; optional keys: ssh_username, ssh_password, home_path, data_dir, redo_dir
            db_port: Database port (default 2881)
            tenant_sys_user: System tenant user (default root@sys)
            global_ssh_username: Global SSH username for all nodes
            global_ssh_password: Global SSH password for all nodes
            global_ssh_port: Global SSH port (default 22)
            global_ssh_key_file: Global SSH key file path
            global_home_path: Global OceanBase home directory
            global_data_dir: Global data directory
            global_redo_dir: Global redo log directory
            obproxy_cluster_name: OBProxy cluster name (optional)
            obproxy_nodes: OBProxy nodes (optional)
            obproxy_global_ssh_username: OBProxy global SSH username
            obproxy_global_ssh_password: OBProxy global SSH password
            obproxy_global_ssh_port: OBProxy global SSH port
            obproxy_global_home_path: OBProxy global home directory
        """
        args = {
            "ob_cluster_name": ob_cluster_name,
            "db_host": db_host,
            "tenant_sys_password": tenant_sys_password,
            "nodes": nodes,
            "db_port": db_port,
            "tenant_sys_user": tenant_sys_user,
            "global_ssh_username": global_ssh_username,
            "global_ssh_password": global_ssh_password,
            "global_ssh_port": global_ssh_port,
            "global_ssh_key_file": global_ssh_key_file,
            "global_home_path": global_home_path,
            "global_data_dir": global_data_dir,
            "global_redo_dir": global_redo_dir,
            "obproxy_cluster_name": obproxy_cluster_name,
            "obproxy_nodes": obproxy_nodes,
            "obproxy_global_ssh_username": obproxy_global_ssh_username,
            "obproxy_global_ssh_password": obproxy_global_ssh_password,
            "obproxy_global_ssh_port": obproxy_global_ssh_port,
            "obproxy_global_home_path": obproxy_global_home_path,
        }

        err = _validate_config_args(args)
        if err:
            return err

        config = _build_config(args)
        output_path = obdiag_path("config.yml")
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        backup_path = None
        if os.path.exists(output_path):
            try:
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"{output_path}.backup_{ts}"
                shutil.copy2(output_path, backup_path)
            except Exception as e:
                return f"Failed to backup existing config file: {e}"

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                yaml.dump(config, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

            file_size = os.path.getsize(output_path)
            if file_size == 0:
                return f"Generated file is empty at {output_path}"

            msg = f"Configuration file generated successfully!\n\nOutput: {output_path}\nSize: {file_size} bytes\n"
            if backup_path:
                msg += f"Backup: {backup_path}\n"
            msg += "\n" + yaml.dump(config, default_flow_style=False, allow_unicode=True, sort_keys=False)

            if stdio:
                stdio.verbose(f"Config generated at {output_path}")
            return msg

        except Exception as e:
            if backup_path and os.path.exists(backup_path):
                try:
                    shutil.copy2(backup_path, output_path)
                except Exception:
                    pass
            return f"Failed to write config file: {e}"

    return [generate_obdiag_config]
