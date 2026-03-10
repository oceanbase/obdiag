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
@file: cluster_resolve.py
@desc: Resolve cluster config short names to file paths (no dependency on models/config).
"""

import os
from typing import Optional

OBDIAG_CONFIG_DIR = os.path.expanduser("~/.obdiag")
DEFAULT_CLUSTER_CONFIG = os.path.join(OBDIAG_CONFIG_DIR, "config.yml")


def resolve_cluster_config_path(name_or_path: str) -> Optional[str]:
    """
    Resolve a cluster config short name or path to an absolute file path.

    - If name_or_path contains '/' or starts with '~', treat as file path and expand.
    - Otherwise treat as short name: look for ~/.obdiag/{name}, ~/.obdiag/{name}.yml,
      ~/.obdiag/{name}.yaml (e.g., "obdiag_test" -> ~/.obdiag/obdiag_test.yml).

    Returns absolute path if file exists, else None.
    """
    if not name_or_path or not str(name_or_path).strip():
        return None
    s = str(name_or_path).strip()

    # Path-like: contains / or starts with ~
    if "/" in s or s.startswith("~"):
        expanded = os.path.abspath(os.path.expanduser(s))
        return expanded if os.path.isfile(expanded) else None

    # Short name: look in ~/.obdiag/
    for name in [s, s + ".yml", s + ".yaml"]:
        p = os.path.join(OBDIAG_CONFIG_DIR, name)
        if os.path.isfile(p):
            return os.path.abspath(p)
    return None
