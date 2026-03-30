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
@file: freeze_dump.py
@desc: RCA for OceanBase 4.x minor freeze / flush (转储) — SQL collection and heuristics.
       SQL aligned with plugins/display/tasks/observer/compaction.yaml where applicable.
"""

import json
import os
from datetime import datetime

from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException
from src.handler.rca.rca_handler import RcaScene
from src.common.tool import DateTimeEncoder
from src.common.tool import StringUtils


class FreezeDumpScene(RcaScene):
    """Collect freeze / minor-freeze / compaction signals and apply lightweight rules."""

    SQL_HINT = "SELECT /*+ READ_CONSISTENCY(WEAK) query_timeout(100000000)*/ "
    RUNNING_STUCK_MINUTES = 30

    LOG_GREP_KEYWORDS = (
        "ret=-4263",
        "dag_ret:",
        "minor merge",
        "MEMTable cannot be minor merged",
        "memtable not ready",
        "failed to create memtable",
        "cannot create more memtable",
    )

    def __init__(self):
        super().__init__()
        self.local_path = ""
        self.filter_tenant_id = None
        self.collect_logs = False

    @staticmethod
    def _row_get(row, *keys):
        if row is None:
            return None
        for k in keys:
            if k in row:
                return row[k]
            for rk, rv in row.items():
                if rk and rk.upper() == k.upper():
                    return rv
        return None

    def _save_to_file(self, filename, data, tenant_id=None):
        try:
            if tenant_id is not None:
                file_path = os.path.join(self.local_path, "rca_freeze_dump_{0}_{1}".format(tenant_id, filename))
            else:
                file_path = os.path.join(self.local_path, "rca_freeze_dump_{0}".format(filename))
            with open(file_path, "w", encoding="utf-8") as f:
                if isinstance(data, (list, dict)):
                    json.dump(data, f, cls=DateTimeEncoder, indent=2, ensure_ascii=False)
                else:
                    f.write(str(data))
            self.stdio.verbose("Saved data to {0}".format(file_path))
            return file_path
        except Exception as e:
            self.stdio.warn("Failed to save file {0}: {1}".format(filename, e))
            return None

    def _execute_sql_safe(self, sql, description=""):
        try:
            self.stdio.verbose("Executing SQL ({0})".format(description))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            return cursor.fetchall()
        except Exception as e:
            self.stdio.warn("SQL execution failed ({0}): {1}".format(description, e))
            return []

    def _parse_collect_logs_flag(self):
        raw = self.input_parameters.get("collect_logs") if self.input_parameters else None
        if raw is None:
            return False
        if isinstance(raw, bool):
            return raw
        return str(raw).strip().lower() in ("1", "true", "yes", "y")

    def init(self, context):
        try:
            super().init(context)
            self.local_path = context.get_variable("store_dir")
            if self.observer_version is None or len(str(self.observer_version).strip()) == 0:
                raise RCAInitException("observer version is None. Please check the NODES conf.")
            if not (self.observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0")):
                raise RCAInitException("observer version must be >= 4.0.0.0, got {0}".format(self.observer_version))
            if self.ob_connector is None:
                raise RCAInitException("ob_connector is None. Please check the NODES conf.")
            if not os.path.exists(self.local_path):
                os.makedirs(self.local_path)

            tid = self.input_parameters.get("tenant_id") if self.input_parameters else None
            if tid is not None and str(tid).strip() != "":
                try:
                    self.filter_tenant_id = int(tid)
                except (ValueError, TypeError):
                    raise RCAInitException("tenant_id is not a valid integer: {0}".format(tid))
            else:
                self.filter_tenant_id = None

            self.collect_logs = self._parse_collect_logs_flag()
            self.record.add_record("freeze_dump init: tenant_filter={0}, collect_logs={1}".format(self.filter_tenant_id if self.filter_tenant_id is not None else "all", self.collect_logs))
        except RCAInitException:
            raise
        except Exception as e:
            raise RCAInitException("FreezeDumpScene init error: {0}".format(e))

    def _sql_cdb_freeze_info(self):
        tenant_clause = ""
        if self.filter_tenant_id is not None:
            tenant_clause = "WHERE TENANT_ID = {0} ".format(self.filter_tenant_id)
        return (
            self.SQL_HINT
            + """
              TENANT_ID,
              FROZEN_SCN,
              CLUSTER_VERSION,
              SCHEMA_VERSION,
              GMT_CREATE,
              GMT_MODIFIED
              FROM oceanbase.CDB_OB_FREEZE_INFO
              """
            + tenant_clause
            + """
              ORDER BY TENANT_ID, GMT_CREATE DESC
              LIMIT 50;
              """
        ).replace("\n", " ")

    def _sql_minor_freeze_info(self):
        tenant_clause = ""
        if self.filter_tenant_id is not None:
            tenant_clause = " AND tenant_id = {0}".format(self.filter_tenant_id)
        return (
            self.SQL_HINT
            + """
              svr_ip,
              svr_port,
              tenant_id,
              ls_id,
              tablet_id,
              is_force,
              freeze_clock,
              freeze_snapshot_version,
              start_time,
              end_time,
              ret_code,
              state,
              diagnose_info,
              memtables_info
              FROM oceanbase.__all_virtual_minor_freeze_info
              WHERE state IN ('RUNNING', 'WAIT_READY_FOR_FLUSH', 'FINISH', 'FAILED')
              """
            + tenant_clause
            + """
              ORDER BY tenant_id, start_time DESC
              LIMIT 100;
              """
        ).replace("\n", " ")

    def _sql_compaction_diagnose(self):
        tenant_clause = ""
        if self.filter_tenant_id is not None:
            tenant_clause = " AND tenant_id = {0}".format(self.filter_tenant_id)
        return (
            self.SQL_HINT
            + """
              tenant_id,
              svr_ip,
              svr_port,
              ls_id,
              tablet_id,
              type,
              status,
              create_time,
              diagnose_info
              FROM oceanbase.__all_virtual_compaction_diagnose_info
              WHERE status IN ('FAILED', 'RUNNING')
              """
            + tenant_clause
            + """
              ORDER BY tenant_id, create_time DESC
              LIMIT 100;
              """
        ).replace("\n", " ")

    def _sql_compaction_suggestions(self):
        tenant_clause = ""
        if self.filter_tenant_id is not None:
            tenant_clause = "WHERE TENANT_ID = {0} ".format(self.filter_tenant_id)
        else:
            tenant_clause = ""
        order_from = "FROM oceanbase.GV$OB_COMPACTION_SUGGESTIONS"
        if tenant_clause:
            sql_body = order_from + " " + tenant_clause
        else:
            sql_body = order_from
        return (
            self.SQL_HINT
            + """
              TENANT_ID,
              SVR_IP,
              SVR_PORT,
              TYPE,
              LS_ID,
              TABLET_ID,
              START_TIME,
              FINISH_TIME,
              SUGGESTION
              """
            + sql_body
            + """
              ORDER BY TENANT_ID, START_TIME DESC
              LIMIT 50;
              """
        ).replace("\n", " ")

    def _sql_compaction_progress(self):
        tenant_clause = ""
        if self.filter_tenant_id is not None:
            tenant_clause = " AND TENANT_ID = {0}".format(self.filter_tenant_id)
        return (
            self.SQL_HINT
            + """
              TENANT_ID,
              SVR_IP,
              SVR_PORT,
              ZONE,
              COMPACTION_SCN,
              STATUS,
              TOTAL_TABLET_COUNT,
              UNFINISHED_TABLET_COUNT,
              round(DATA_SIZE/1024/1024/1024, 2) AS DATA_SIZE_GB,
              round(UNFINISHED_DATA_SIZE/1024/1024/1024, 2) AS UNFINISHED_DATA_SIZE_GB,
              START_TIME,
              ESTIMATED_FINISH_TIME
              FROM oceanbase.GV$OB_COMPACTION_PROGRESS
              WHERE STATUS <> 'FINISH'
              """
            + tenant_clause
            + """
              ORDER BY TENANT_ID, SVR_IP;
              """
        ).replace("\n", " ")

    def _sql_tenant_memstore_info(self):
        if self.filter_tenant_id is not None:
            where = "WHERE tenant_id = {0}".format(self.filter_tenant_id)
        else:
            where = ""
        return (
            """
            SELECT
                tenant_id,
                svr_ip,
                svr_port,
                ROUND(memstore_used / 1024 / 1024 / 1024, 2) as memstore_used_gb,
                ROUND(memstore_limit / 1024 / 1024 / 1024, 2) as memstore_limit_gb,
                CASE
                    WHEN memstore_limit > 0 THEN ROUND(memstore_used / memstore_limit * 100, 2)
                    ELSE 0
                END as memstore_usage_percent,
                ROUND(freeze_trigger / 1024 / 1024 / 1024, 2) as freeze_trigger_gb
            FROM oceanbase.__all_virtual_tenant_memstore_info
            """
            + where
            + ";"
        ).replace("\n", " ")

    def _parse_start_time(self, val):
        if val is None:
            return None
        if isinstance(val, datetime):
            return val
        s = str(val).strip()
        if not s:
            return None
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00").split("+")[0])
        except ValueError:
            pass
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(s[:26], fmt)
            except ValueError:
                continue
        return None

    def _running_too_long(self, row):
        state = self._row_get(row, "state", "STATE")
        if state != "RUNNING":
            return False
        st = self._parse_start_time(self._row_get(row, "start_time", "START_TIME"))
        if st is None:
            return False
        delta = datetime.now() - st
        return delta.total_seconds() > self.RUNNING_STUCK_MINUTES * 60

    def _drill_memstore_and_suggest(self, svr_ip, svr_port, tenant_id, context_label):
        sql = ("SELECT * FROM oceanbase.__all_virtual_memstore_info " "WHERE svr_ip = '{0}' AND svr_port = {1} AND tenant_id = {2} AND is_active = 'NO' " "ORDER BY start_scn LIMIT 3").format(svr_ip, svr_port, tenant_id)
        rows = self._execute_sql_safe(sql, "memstore drill {0}".format(context_label))
        tag = "{0}_{1}_{2}".format(svr_ip, svr_port, tenant_id)
        self._save_to_file("memstore_drill_{0}.json".format(tag), rows)
        for mem_row in rows:
            freeze_state = self._row_get(mem_row, "freeze_state", "FREEZE_STATE")
            self.record.add_record("[drill {0}] freeze_state={1}".format(context_label, freeze_state))
            if freeze_state == "NOT_READY_FOR_FLUSH":
                try:
                    uc = int(self._row_get(mem_row, "unsubmitted_count", "UNSUBMITTED_COUNT") or 0)
                    wr = int(self._row_get(mem_row, "write_ref_count", "WRITE_REF_COUNT") or 0)
                except (ValueError, TypeError):
                    uc, wr = 0, 0
                us_raw = self._row_get(mem_row, "unsynced_count", "UNSYNCED_COUNT")
                try:
                    us = int(us_raw) if us_raw is not None else None
                except (ValueError, TypeError):
                    us = None
                if us is not None:
                    self.record.add_record("unsubmitted_count={0}, unsynced_count={1}, write_ref_count={2}".format(uc, us, wr))
                    if uc == us and uc != 0:
                        self.record.add_suggest("Possible multi-source data; check whether the table has multiple data sources.")
                    elif uc == 0 and wr != 0 and us != 0:
                        self.record.add_suggest("Possible logs without callbacks (write_ref / unsynced).")
                    elif uc == 0 and us == 0 and wr == 0:
                        self.record.add_suggest("Replay/callback may not have passed memtable boundary; check ReplayService / log replay.")
                else:
                    self.record.add_record("unsubmitted_count={0}, write_ref_count={1} (unsynced_count not exposed on this version)".format(uc, wr))
                    if uc == 0 and wr == 0:
                        self.record.add_suggest("NOT_READY_FOR_FLUSH with zero unsubmitted/write_ref; check replay boundary and tenant log sync on this OB version.")
            elif freeze_state == "READY_FOR_FLUSH":
                self.record.add_suggest("Memtable ready for flush but flush may not run; check __all_virtual_compaction_diagnose_info and DAG backlog on this server.")
            elif freeze_state == "RELEASED":
                self.record.add_suggest("Memtable released from mgr but refcount may block destruction; continue with OceanBase support if memory does not drop.")

    def _drill_dag_scheduler(self, svr_ip, svr_port, tenant_id):
        sql = ("SELECT * FROM oceanbase.__all_virtual_dag_scheduler " "WHERE svr_ip = '{0}' AND svr_port = '{1}' AND tenant_id = '{2}'").format(svr_ip, svr_port, tenant_id)
        data = self._execute_sql_safe(sql, "dag_scheduler")
        self._save_to_file("dag_scheduler_{0}_{1}_{2}.json".format(svr_ip, svr_port, tenant_id), data)

    def _heuristics_minor_freeze(self, rows):
        drill_keys = set()
        for row in rows:
            state = self._row_get(row, "state", "STATE")
            if state == "FAILED":
                self.record.add_record(
                    "minor_freeze FAILED: tenant={0} tablet={1} ret_code={2} diagnose_info={3}".format(
                        self._row_get(row, "tenant_id", "TENANT_ID"),
                        self._row_get(row, "tablet_id", "TABLET_ID"),
                        self._row_get(row, "ret_code", "RET_CODE"),
                        self._row_get(row, "diagnose_info", "DIAGNOSE_INFO"),
                    )
                )
                self.record.add_suggest("Minor freeze FAILED: search observer logs for dag_ret and match tablet_id/ls_id; check GV$OB_COMPACTION_SUGGESTIONS for the same tablet.")
                drill_keys.add(
                    (
                        str(self._row_get(row, "svr_ip", "SVR_IP")),
                        str(self._row_get(row, "svr_port", "SVR_PORT")),
                        str(self._row_get(row, "tenant_id", "TENANT_ID")),
                    )
                )
            elif state == "WAIT_READY_FOR_FLUSH" or state == "FINISH":
                self.record.add_record(
                    "minor_freeze state={0} tenant={1} svr={2}:{3} tablet={4}".format(
                        state,
                        self._row_get(row, "tenant_id", "TENANT_ID"),
                        self._row_get(row, "svr_ip", "SVR_IP"),
                        self._row_get(row, "svr_port", "SVR_PORT"),
                        self._row_get(row, "tablet_id", "TABLET_ID"),
                    )
                )
                drill_keys.add(
                    (
                        str(self._row_get(row, "svr_ip", "SVR_IP")),
                        str(self._row_get(row, "svr_port", "SVR_PORT")),
                        str(self._row_get(row, "tenant_id", "TENANT_ID")),
                    )
                )
            elif self._running_too_long(row):
                self.record.add_record(
                    "minor_freeze RUNNING > {0}m: tenant={1} svr={2}:{3} tablet={4}".format(
                        self.RUNNING_STUCK_MINUTES,
                        self._row_get(row, "tenant_id", "TENANT_ID"),
                        self._row_get(row, "svr_ip", "SVR_IP"),
                        self._row_get(row, "svr_port", "SVR_PORT"),
                        self._row_get(row, "tablet_id", "TABLET_ID"),
                    )
                )
                self.record.add_suggest("Long-running minor freeze: check memstore pressure, DAG scheduler queue, and disk/CPU on the observer.")
                drill_keys.add(
                    (
                        str(self._row_get(row, "svr_ip", "SVR_IP")),
                        str(self._row_get(row, "svr_port", "SVR_PORT")),
                        str(self._row_get(row, "tenant_id", "TENANT_ID")),
                    )
                )

        for svr_ip, svr_port, tenant_id in drill_keys:
            try:
                try:
                    sp = int(svr_port)
                except (ValueError, TypeError):
                    sp = svr_port
                self._drill_memstore_and_suggest(svr_ip, sp, tenant_id, "minor_freeze")
                self._drill_dag_scheduler(svr_ip, svr_port, tenant_id)
            except Exception as e:
                self.stdio.warn("drill-down failed for {0}:{1} tenant {2}: {3}".format(svr_ip, svr_port, tenant_id, e))

    def _heuristics_diagnose(self, rows):
        for row in rows:
            status = self._row_get(row, "status", "STATUS")
            if status == "FAILED":
                self.record.add_record(
                    "compaction_diagnose FAILED: type={0} tablet={1} info={2}".format(
                        self._row_get(row, "type", "TYPE"),
                        self._row_get(row, "tablet_id", "TABLET_ID"),
                        self._row_get(row, "diagnose_info", "DIAGNOSE_INFO"),
                    )
                )

    def _maybe_gather_logs(self):
        if not self.collect_logs:
            return
        if self.gather_log is None:
            self.record.add_record("collect_logs requested but gather_log is not available.")
            return
        log_dir = os.path.join(self.local_path, "freeze_dump_logs")
        self.gather_log.init_parameters()
        self.gather_log.set_parameters("scope", "observer")
        for kw in self.LOG_GREP_KEYWORDS:
            self.gather_log.grep(kw)
        try:
            names = self.gather_log.execute(save_path=log_dir)
            self.record.add_record("gather_log saved under {0}, files={1}".format(log_dir, len(names or [])))
        except Exception as e:
            self.stdio.warn("gather_log failed: {0}".format(e))
            self.record.add_record("gather_log failed: {0}".format(e))

    def execute(self):
        try:
            self.record.add_record("Starting freeze_dump RCA (minor freeze / flush path).")

            freeze_rows = self._execute_sql_safe(self._sql_cdb_freeze_info(), "CDB_OB_FREEZE_INFO")
            self._save_to_file("freeze_info.json", freeze_rows)

            minor_rows = self._execute_sql_safe(self._sql_minor_freeze_info(), "__all_virtual_minor_freeze_info")
            self._save_to_file("minor_freeze_info.json", minor_rows)

            diag_rows = self._execute_sql_safe(self._sql_compaction_diagnose(), "__all_virtual_compaction_diagnose_info")
            self._save_to_file("compaction_diagnose.json", diag_rows)

            sugg_rows = self._execute_sql_safe(self._sql_compaction_suggestions(), "GV$OB_COMPACTION_SUGGESTIONS")
            self._save_to_file("compaction_suggestions.json", sugg_rows)

            prog_rows = self._execute_sql_safe(self._sql_compaction_progress(), "GV$OB_COMPACTION_PROGRESS")
            self._save_to_file("compaction_progress.json", prog_rows)

            mem_rows = self._execute_sql_safe(self._sql_tenant_memstore_info(), "__all_virtual_tenant_memstore_info")
            self._save_to_file("tenant_memstore_info.json", mem_rows)

            for row in mem_rows or []:
                try:
                    pct = float(self._row_get(row, "memstore_usage_percent", "MEMSTORE_USAGE_PERCENT") or 0)
                except (ValueError, TypeError):
                    pct = 0
                if pct >= 85:
                    self.record.add_record(
                        "High memstore usage {0}% on {1}:{2} tenant {3}".format(
                            pct,
                            self._row_get(row, "svr_ip", "SVR_IP"),
                            self._row_get(row, "svr_port", "SVR_PORT"),
                            self._row_get(row, "tenant_id", "TENANT_ID"),
                        )
                    )
                    self.record.add_suggest("Memstore usage is high; freeze/flush may be under pressure. Check tenant memory and write throughput.")

            self._heuristics_diagnose(diag_rows)
            self._heuristics_minor_freeze(minor_rows)

            if not minor_rows and not diag_rows:
                self.record.add_record("No minor_freeze rows (filtered states) and no FAILED/RUNNING compaction_diagnose rows; cluster may be healthy on these checks.")

            self._maybe_gather_logs()
            self.record.add_suggest("Artifacts saved under {0}; share with OceanBase community/support if needed.".format(self.local_path))
        except Exception as e:
            raise RCAExecuteException("FreezeDumpScene execute error: {0}".format(e))

    def get_scene_info(self):
        return {
            "name": "freeze_dump",
            "info_en": "[OB 4.x] Minor freeze / flush (转储) RCA: virtual tables + light heuristics.",
            "info_cn": "[OB 4.x] 冻结与转储根因采集：内部视图与初步判断。",
            "example": "obdiag rca run --scene=freeze_dump [--env tenant_id=1002] [--env collect_logs=true]",
        }


freeze_dump = FreezeDumpScene()
