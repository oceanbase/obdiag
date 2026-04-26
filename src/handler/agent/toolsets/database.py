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
            if deps.stdio:
                deps.stdio.verbose(f"SQL validation failed: {error_msg}")
            return error_msg

        connector = deps.get_db_connector(cluster_config_path)
        if not connector:
            if cluster_config_path:
                return f"Error: Cannot connect to cluster from config '{cluster_config_path}'. " "Please verify the file exists and contains valid 'obcluster' settings " "(db_host, db_port, tenant_sys.user; tenant_sys.password may be empty)."
            return "Error: No database connection available. " "Use `/use <config_path>` in the agent REPL to switch cluster, or pass cluster_config_path."

        target = cluster_config_path or deps.config_path or "default cluster"
        try:
            if deps.stdio:
                deps.stdio.verbose(f"Executing SQL on [{target}]: {sql[:100]}...")

            cursor = connector.execute_sql_return_cursor_dictionary(sql)
            results = cursor.fetchall()
            cursor.close()

            if not results:
                return "Query executed successfully. No rows returned."

            result_text = f"Query executed successfully on [{target}]. Returned {len(results)} row(s):\n\n"
            result_text += json.dumps(results, indent=2, ensure_ascii=False, default=str)
            return result_text

        except Exception as e:
            error_msg = f"SQL query execution failed on [{target}]: {e}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg

    return [db_query]
