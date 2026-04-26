#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Connect with vanilla PyMySQL (no obdiag patches) and SELECT from sql_audit.
If query_sql contains invalid UTF-8, UnicodeDecodeError is raised — issue #1202.

Run from this directory WITHOUT setting PYTHONPATH to project src, so obdiag's
pymysql_binary_compat is NOT loaded:

  cd workflow_data/repro_issue_1202
  env -u PYTHONPATH python3 repro_sql_audit_fetch.py

Or explicitly:

  env OB_HOST=... OB_PORT=... OB_USER='root@sys' OB_PASSWORD='...' python3 repro_sql_audit_fetch.py
"""
from __future__ import print_function

import os
import sys

try:
    import pymysql
except ImportError:
    print("pip install pymysql", file=sys.stderr)
    sys.exit(2)


def main():
    host = os.environ.get("OB_HOST", "127.0.0.1")
    port = int(os.environ.get("OB_PORT", "2881"))
    user = os.environ.get("OB_USER", "root@sys")
    password = os.environ.get("OB_PASSWORD", "")
    database = os.environ.get("OB_DATABASE", "").strip()
    view = os.environ.get("OB_SQL_AUDIT_VIEW", "oceanbase.gv$ob_sql_audit")

    default_sql = """
SELECT *
FROM {view}
WHERE REQUEST_TIME > (UNIX_TIMESTAMP() - 86400) * 1000000
ORDER BY REQUEST_TIME DESC
LIMIT 100
""".format(
        view=view
    )
    sql = os.environ.get("OB_SQL_AUDIT_SQL", default_sql).strip()

    print("Connecting {0}:{1} user={2} db={3}".format(host, port, user, database or "(none)"))
    kw = dict(
        host=host,
        port=port,
        user=user,
        password=password,
        charset="utf8mb4",
        connect_timeout=30,
    )
    if database:
        kw["database"] = database
    conn = pymysql.connect(**kw)
    try:
        cur = conn.cursor()
        print("Executing audit query (first 200 chars of SQL):\n", sql[:200], "...\n")
        cur.execute(sql)
        rows = cur.fetchall()
        print("OK: fetched", len(rows), "rows (no UnicodeDecodeError).")
        print("If your cluster has binary in query_sql, use unpatched PyMyQL and expect failure.")
        cur.close()
        return 0
    except UnicodeDecodeError as e:
        print("REPRODUCED UnicodeDecodeError (issue #1202):", repr(e), file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
