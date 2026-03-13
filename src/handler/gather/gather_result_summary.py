#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
"""
@file: gather_result_summary.py
@desc: Helper for result_summary.txt header (collect time, observer/obproxy version).
       Reference: check_report.py export_report_table.
       Supports offline: when cluster unavailable, try manifest.json or show N/A.
"""
import datetime
import json
import os

from src.common.version import OBDIAG_VERSION
from src.common.command import get_observer_version, get_obproxy_version


def _try_load_version_from_pack(pack_dir):
    """
    Try to load ob_version from manifest.json in pack_dir or parent (offline).
    Returns version string or None.
    """
    if not pack_dir or not os.path.isdir(pack_dir):
        return None
    for base in (os.path.abspath(pack_dir), os.path.dirname(os.path.abspath(pack_dir))):
        if not base:
            continue
        manifest_path = os.path.join(base, "manifest.json")
        if os.path.isfile(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return data.get("ob_version") or None
            except Exception:
                pass
    return None


def get_result_summary_header(context, pack_dir=None):
    """
    Build header lines for result_summary.txt (obdiag version, collect time, observer/obproxy version).
    Reference: check_report export_report_table.
    Offline: when cluster unavailable, try manifest.json in pack_dir; if still none, show N/A.
    """
    lines = []
    lines.append("obdiag version: {0}".format(OBDIAG_VERSION))
    report_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append("collect time: {0}".format(report_time))
    lines.append("")

    # observer version (online or offline from manifest)
    ob_version = None
    try:
        cluster_config = getattr(context, "cluster_config", None)
        if cluster_config:
            ob_version = cluster_config.get("version") if isinstance(cluster_config, dict) else None
        if not ob_version:
            ob_version = get_observer_version(context)
    except Exception:
        pass
    if not ob_version and pack_dir:
        ob_version = _try_load_version_from_pack(pack_dir)
    lines.append("observer version: {0}".format(ob_version if ob_version else "N/A"))

    # obproxy version (if obproxy nodes exist; offline: N/A)
    obproxy_version = None
    try:
        obproxy_config = getattr(context, "obproxy_config", None)
        obproxy_nodes = obproxy_config.get("servers") if obproxy_config and isinstance(obproxy_config, dict) else []
        if obproxy_nodes:
            obproxy_version = get_obproxy_version(context)
    except Exception:
        pass
    lines.append("obproxy version: {0}".format(obproxy_version if obproxy_version else "N/A"))
    lines.append("")
    return "\n".join(lines)


def ensure_result_summary_header(pack_dir, context):
    """
    If result_summary.txt does not exist or is empty, write header first.
    Call this before appending content to result_summary.txt.
    Pass pack_dir for offline version lookup from manifest.json.
    """
    filepath = os.path.join(pack_dir, "result_summary.txt")
    if not os.path.exists(filepath) or os.path.getsize(filepath) == 0:
        header = get_result_summary_header(context, pack_dir=pack_dir)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(header)
