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
@time: 2025/01/27
@file: clog_replay_rate.py
@desc: Check OceanBase 4.x clog replay rate and unreplayed size statistics
       Analyzes follower replay lag and unreplayed log size to identify potential replay issues.
       issue #933
"""

from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class ClogReplayRateTask(TaskBase):
    def init(self, context, report):
        super().init(context, report)
        # Thresholds for warnings
        self.UNREPLAYED_SIZE_WARNING_MB = 1024  # 1GB
        self.UNREPLAYED_SIZE_CRITICAL_MB = 5120  # 5GB
        self.REPLAY_DELAY_WARNING_MINUTES = 3  # 3 minutes
        self.REPLAY_DELAY_CRITICAL_MINUTES = 10  # 10 minutes

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")

            # Check version requirement: >= 4.0.0.0
            if not super().check_ob_version_min("4.0.0.0"):
                return self.report.add_warning("clog replay rate analysis is only supported in OceanBase 4.0.0.0 and above. " "Current version: {0}".format(self.observer_version))

            self.stdio.verbose("Starting clog replay rate and unreplayed size analysis")

            # Analyze replay statistics
            self._analyze_replay_statistics()

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def _analyze_replay_statistics(self):
        """Analyze clog replay rate and unreplayed size statistics"""
        try:
            # Query replay statistics for FOLLOWER nodes
            sql = """
            SELECT 
                a.svr_ip,
                a.svr_port,
                a.tenant_id,
                a.ls_id,
                a.role,
                a.unsubmitted_lsn,
                a.unsubmitted_log_scn,
                a.end_lsn,
                a.pending_cnt,
                b.end_lsn as leader_end_lsn,
                b.end_scn as leader_end_scn,
                CASE 
                    WHEN b.end_lsn >= a.unsubmitted_lsn THEN (b.end_lsn - a.unsubmitted_lsn)
                    ELSE 0
                END as unreplayed_size_bytes,
                CASE 
                    WHEN b.end_scn > a.unsubmitted_log_scn AND b.end_scn > 0 AND a.unsubmitted_log_scn > 0 THEN 
                        TIMESTAMPDIFF(SECOND, 
                            scn_to_timestamp(a.unsubmitted_log_scn), 
                            scn_to_timestamp(b.end_scn)
                        )
                    ELSE 0
                END as replay_delay_seconds
            FROM oceanbase.__all_virtual_replay_stat a
            LEFT JOIN oceanbase.__all_virtual_log_stat b 
                ON a.tenant_id = b.tenant_id 
                AND a.ls_id = b.ls_id 
                AND b.role = 'LEADER'
            WHERE a.role = 'FOLLOWER'
            ORDER BY a.tenant_id, a.ls_id, a.svr_ip
            """

            self.stdio.verbose("Executing replay statistics query")
            results = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()

            if not results or len(results) == 0:
                self.stdio.verbose("No FOLLOWER replay statistics found (single node or no followers)")
                return

            self.stdio.verbose("Found {0} FOLLOWER replay statistics records".format(len(results)))

            # Analyze each record
            critical_issues = []
            warning_issues = []
            total_unreplayed_size_mb = 0.0

            for row in results:
                svr_ip = row.get("svr_ip") or row.get("SVR_IP")
                svr_port = row.get("svr_port") or row.get("SVR_PORT")
                tenant_id = row.get("tenant_id") or row.get("TENANT_ID")
                ls_id = row.get("ls_id") or row.get("LS_ID")
                # Coerce to float to avoid float += Decimal (DB drivers may return Decimal)
                unreplayed_size_bytes = float(row.get("unreplayed_size_bytes") or 0)
                replay_delay_seconds = float(row.get("replay_delay_seconds") or 0)
                pending_cnt = int(row.get("pending_cnt") or row.get("PENDING_CNT") or 0)

                # Convert bytes to MB
                unreplayed_size_mb = unreplayed_size_bytes / (1024 * 1024)
                replay_delay_minutes = replay_delay_seconds / 60

                total_unreplayed_size_mb += unreplayed_size_mb

                # Build location identifier
                location = "{0}:{1} tenant_id={2} ls_id={3}".format(svr_ip, svr_port, tenant_id, ls_id)

                # Check unreplayed size
                if unreplayed_size_mb >= self.UNREPLAYED_SIZE_CRITICAL_MB:
                    critical_issues.append("{0}: unreplayed size is {1:.2f} MB (>= {2} MB threshold). " "This indicates severe replay lag.".format(location, unreplayed_size_mb, self.UNREPLAYED_SIZE_CRITICAL_MB))
                elif unreplayed_size_mb >= self.UNREPLAYED_SIZE_WARNING_MB:
                    warning_issues.append("{0}: unreplayed size is {1:.2f} MB (>= {2} MB threshold). " "Monitor replay progress.".format(location, unreplayed_size_mb, self.UNREPLAYED_SIZE_WARNING_MB))

                # Check replay delay
                if replay_delay_minutes >= self.REPLAY_DELAY_CRITICAL_MINUTES:
                    critical_issues.append("{0}: replay delay is {1:.2f} minutes (>= {2} minutes threshold). " "SCN-based delay indicates slow replay.".format(location, replay_delay_minutes, self.REPLAY_DELAY_CRITICAL_MINUTES))
                elif replay_delay_minutes >= self.REPLAY_DELAY_WARNING_MINUTES:
                    warning_issues.append("{0}: replay delay is {1:.2f} minutes (>= {2} minutes threshold). " "Monitor replay progress.".format(location, replay_delay_minutes, self.REPLAY_DELAY_WARNING_MINUTES))

                # Check pending count
                if pending_cnt > 1000:
                    warning_issues.append("{0}: pending_cnt is {1} (high pending count may indicate replay bottleneck).".format(location, pending_cnt))

                self.stdio.verbose("{0}: unreplayed_size={1:.2f}MB, delay={2:.2f}min, pending_cnt={3}".format(location, unreplayed_size_mb, replay_delay_minutes, pending_cnt))

            # Report issues
            if critical_issues:
                for issue in critical_issues:
                    self.report.add_critical(issue)

            if warning_issues:
                for issue in warning_issues:
                    self.report.add_warning(issue)

            # Summary statistics
            if total_unreplayed_size_mb > 0:
                self.stdio.verbose("Total unreplayed size across all followers: {0:.2f} MB".format(total_unreplayed_size_mb))

            # Additional analysis: check for tenants with high replay lag
            self._analyze_tenant_replay_statistics(results)

        except Exception as e:
            self.stdio.error("Error analyzing replay statistics: {0}".format(e))
            self.report.add_warning("Failed to analyze replay statistics: {0}".format(e))

    def _analyze_tenant_replay_statistics(self, replay_results):
        """Analyze replay statistics aggregated by tenant"""
        try:
            # Aggregate by tenant
            tenant_stats = {}
            for row in replay_results:
                tenant_id = row.get("tenant_id") or row.get("TENANT_ID")
                # Coerce to float to avoid int/float += Decimal (DB drivers may return Decimal)
                unreplayed_size_bytes = float(row.get("unreplayed_size_bytes") or 0)
                replay_delay_seconds = float(row.get("replay_delay_seconds") or 0)

                if tenant_id not in tenant_stats:
                    tenant_stats[tenant_id] = {
                        "total_unreplayed_mb": 0.0,
                        "max_delay_minutes": 0.0,
                        "follower_count": 0,
                    }

                tenant_stats[tenant_id]["total_unreplayed_mb"] += unreplayed_size_bytes / (1024 * 1024)
                delay_minutes = replay_delay_seconds / 60
                if delay_minutes > tenant_stats[tenant_id]["max_delay_minutes"]:
                    tenant_stats[tenant_id]["max_delay_minutes"] = delay_minutes
                tenant_stats[tenant_id]["follower_count"] += 1

            # Check for tenants with high aggregate unreplayed size
            for tenant_id, stats in tenant_stats.items():
                if stats["total_unreplayed_mb"] >= self.UNREPLAYED_SIZE_CRITICAL_MB * 2:
                    self.report.add_warning("Tenant {0}: total unreplayed size is {1:.2f} MB across {2} followers. " "This may indicate tenant-level replay issues.".format(tenant_id, stats["total_unreplayed_mb"], stats["follower_count"]))

        except Exception as e:
            self.stdio.warn("Error in tenant replay statistics analysis: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "clog_replay_rate",
            "info": "Check OceanBase 4.x clog replay rate and unreplayed size statistics. " "Analyzes follower replay lag and unreplayed log size to identify potential replay issues.",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/933",
        }


clog_replay_rate = ClogReplayRateTask()
