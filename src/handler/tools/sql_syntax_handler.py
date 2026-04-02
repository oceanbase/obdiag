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
@time: 2026/03/27
@file: sql_syntax_handler.py
@desc: Validate SQL syntax/semantics against a live OceanBase instance
       using EXPLAIN — without executing the SQL.
       See https://github.com/oceanbase/obdiag/issues/1181
"""

import re

import pymysql as mysql

from src.common.ob_connector import OBConnector
from src.common.result_type import ObdiagResult
from src.common.tool import StringUtils, Util


def normalize_sql_for_syntax_check(sql):
    """
    Strip trailing semicolons/whitespace and reject obvious multi-statement input
    (semicolon followed by more SQL). Reduces risk when prefixing EXPLAIN.
    Returns (normalized_sql, error_message). error_message is None if OK.
    """
    if not sql or not sql.strip():
        return None, "empty SQL"
    s = sql.strip()
    while s.endswith(';'):
        s = s[:-1].rstrip()
    if '\x00' in s:
        return None, "null byte in SQL is not allowed"
    if re.search(r';\s*\S', s):
        return None, "multiple statements are not allowed; pass a single statement only"
    return s, None


class SqlSyntaxHandler:
    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.options = context.options

    def handle(self):
        sql = self._get_sql()
        if sql is None:
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="--sql is required")

        sql, norm_err = normalize_sql_for_syntax_check(sql)
        if norm_err:
            self.stdio.error(norm_err)
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data=norm_err)

        host, port, user, password, database = self._resolve_connection()
        if host is None:
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="missing connection info")

        db_label = "database: {0}".format(database) if database else "no database selected"
        self.stdio.print("SQL syntax check on {0}:{1} (user: {2}, {3})".format(host, port, user, db_label))
        self.stdio.print("SQL: {0}".format(sql))

        connector = OBConnector(
            context=self.context,
            ip=host,
            port=port,
            username=user,
            password=password or '',
            database=database,
        )
        if connector.conn is None:
            self.stdio.error("Failed to connect to OceanBase at {0}:{1}. Check your connection info.".format(host, port))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="connection failed")

        try:
            return self._check_syntax(connector, sql)
        finally:
            if getattr(connector, "conn", None) is not None:
                try:
                    connector.conn.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    def _get_sql(self):
        sql = Util.get_option(self.options, 'sql')
        if not sql or not sql.strip():
            self.stdio.error("--sql is required. Usage: obdiag tool sql_syntax --sql 'SELECT ...' " "[--env host=... --env port=... --env user=... --env password=... --env database=...]")
            return None
        return sql.strip()

    def _resolve_connection(self):
        """Return (host, port, user, password, database) using --env first, then config.yml fallback."""
        env_list = Util.get_option(self.options, 'env')
        env_dict = StringUtils.parse_env_display(env_list) if env_list else {}

        if env_dict:
            db_info = StringUtils.build_db_info_from_env(env_dict, self.stdio)
            if db_info is None:
                return None, None, None, None, None
            host = db_info.get('host')
            port = db_info.get('port')
            user = db_info.get('user')
            password = db_info.get('password', '')
            database = db_info.get('database')
            if host and port and user:
                return host, int(port), user, password, database
            self.stdio.warn("Incomplete connection info in --env (need host, port, user), falling back to config.yml obcluster")

        cluster_config = self.context.cluster_config or {}
        host = cluster_config.get('db_host')
        port = cluster_config.get('db_port')
        tenant_sys = cluster_config.get('tenant_sys') or {}
        user = tenant_sys.get('user')
        password = tenant_sys.get('password', '')
        database = env_dict.get('database') or env_dict.get('db')

        if not host or not port or not user:
            self.stdio.error("Missing connection info. Provide via --env or set obcluster.db_host / db_port / tenant_sys.user in config.yml.")
            return None, None, None, None, None

        return host, int(port), user, password, database

    def _check_syntax(self, connector, sql):
        """Run EXPLAIN against the SQL and interpret the result."""
        explain_sql = "EXPLAIN {0}".format(sql)
        self.stdio.verbose("[sql-syntax] exec: {0}".format(explain_sql))

        try:
            connector.execute_sql(explain_sql)
            self.stdio.print("Result: VALID")
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"result": "VALID", "sql": sql})

        except mysql.Error as e:
            error_code = e.args[0] if e.args else None
            error_msg = e.args[1] if len(e.args) > 1 else str(e)

            if error_code == 1064:
                self.stdio.print("Result: SYNTAX ERROR")
                self.stdio.print("Detail: {0}".format(error_msg))
                return ObdiagResult(
                    ObdiagResult.SUCCESS_CODE,
                    data={"result": "SYNTAX_ERROR", "error_code": error_code, "detail": error_msg},
                )
            else:
                self.stdio.print("Result: VALID (syntax OK, but semantic error [{0}]: {1})".format(error_code, error_msg))
                return ObdiagResult(
                    ObdiagResult.SUCCESS_CODE,
                    data={"result": "SEMANTIC_ERROR", "error_code": error_code, "detail": error_msg},
                )

        except Exception as e:
            self.stdio.error("Unexpected error during SQL syntax check: {0}".format(e))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=str(e))
