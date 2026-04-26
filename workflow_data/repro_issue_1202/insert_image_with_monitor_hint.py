#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Insert JPEG into repro_image_store with /*+ monitor*/ so sql_plan_monitor may collect DFO data.

Usage (env override optional):
  OB_HOST=6.12.235.247 OB_PORT=2893 OB_USER=root@sys OB_PASSWORD= OB_DATABASE=test \\
  python insert_image_with_monitor_hint.py

Default image path: repo root 20260325141356.jpg (run from repo root or set IMAGE_PATH).

After INSERT, runs SELECT last_trace_id() in the **same session before COMMIT** so the trace
matches this statement (OceanBase behavior may vary; if it fails, take trace_id from sql_audit).
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
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    image = os.environ.get("IMAGE_PATH", os.path.join(repo_root, "20260325141356.jpg"))

    if not os.path.isfile(image):
        print("ERROR: image not found: %s" % image, file=sys.stderr)
        return 1

    host = os.environ.get("OB_HOST", "6.12.235.247")
    port = int(os.environ.get("OB_PORT", "2893"))
    user = os.environ.get("OB_USER", "root@sys")
    password = os.environ.get("OB_PASSWORD", "")
    database = os.environ.get("OB_DATABASE", "test")

    with open(image, "rb") as f:
        blob = f.read()

    sql = "INSERT /*+ monitor*/ INTO repro_image_store (filename, image_data) " "VALUES (%s, %s)"

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        charset="utf8mb4",
        autocommit=False,
    )
    trace_id = None
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (os.path.basename(image), blob))
            # Same transaction as INSERT — trace_id usually refers to the last executed stmt
            try:
                cur.execute("SELECT last_trace_id()")
                row = cur.fetchone()
                trace_id = row[0] if row else None
            except Exception as e:
                print("WARN: last_trace_id() failed (ignore if your OB version has no it): %s" % e, file=sys.stderr)
        conn.commit()
        print("OK bytes=%s head=%s" % (len(blob), sql.split("VALUES")[0].strip() + " ..."))
        if trace_id is not None:
            print("trace_id=%s" % trace_id)
            print("obdiag gather plan_monitor --trace_id %s --env host=%s --env port=%s --env user=%s --env database=%s --env password=<YOUR_PASSWORD>" % (trace_id, host, port, user, database))
        else:
            print(
                "WARN: no trace_id; query oceanbase.gv$ob_sql_audit ORDER BY REQUEST_TIME DESC LIMIT 3",
                file=sys.stderr,
            )
        return 0
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        print("ERROR: %s" % e, file=sys.stderr)
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
