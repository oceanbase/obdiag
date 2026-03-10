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
@time: 2026/03/09
@file: database.py
@desc: Database query tools for obdiag agent
"""

import json

from pydantic_ai import Agent, RunContext

from src.handler.agent.models import AgentDependencies
from src.handler.agent.tools.sql_validator import validate_sql


def register_database_tools(agent: Agent[AgentDependencies, str]):
    """
    Register database tools with the agent
    
    Args:
        agent: The Pydantic-AI agent to register tools with
    """
    
    @agent.tool
    def db_query(ctx: RunContext[AgentDependencies], sql: str) -> str:
        """
        Execute a read-only SQL query on the OceanBase database.
        
        Only SELECT, SHOW, DESCRIBE, EXPLAIN, and WITH queries are allowed.
        The database connection is automatically established using the configured cluster.
        
        Args:
            sql: The SQL query to execute (must be read-only)
            
        Returns:
            Query results as formatted JSON, or error message if query fails
        """
        deps = ctx.deps
        
        # Validate SQL
        is_valid, error_msg = validate_sql(sql)
        if not is_valid:
            if deps.stdio:
                deps.stdio.verbose(f"SQL validation failed: {error_msg}")
            return error_msg
        
        # Get database connector
        connector = deps.get_db_connector()
        if not connector:
            return "Error: Database connection not available. Please ensure cluster is configured with valid credentials."
        
        try:
            if deps.stdio:
                deps.stdio.verbose(f"Executing SQL query: {sql[:100]}...")
            
            cursor = connector.execute_sql_return_cursor_dictionary(sql)
            results = cursor.fetchall()
            cursor.close()
            
            if not results:
                return "Query executed successfully. No rows returned."
            else:
                result_text = f"Query executed successfully. Returned {len(results)} row(s):\n\n"
                result_text += json.dumps(results, indent=2, ensure_ascii=False, default=str)
                return result_text
                
        except Exception as e:
            error_msg = f"SQL query execution failed: {str(e)}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg
