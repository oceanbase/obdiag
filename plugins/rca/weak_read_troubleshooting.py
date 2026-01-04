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
@time: 2025/12/01
@file: weak_read_troubleshooting.py
@desc: Troubleshooting of weak reading FAQs
"""

import os
from typing import List, Dict
from src.handler.rca.rca_exception import RCAInitException, RCAExecuteException, RCANotNeedExecuteException
from src.handler.rca.rca_handler import RcaScene
from src.common.tool import StringUtils


class WeakReadTroubleshooting(RcaScene):
    def __init__(self):
        super().__init__()
        self.tenant_id = None
        self.work_path = None
        self.local_path = None

    def verbose(self, info):
        self.stdio.verbose("[WeakReadTroubleshooting] {0}".format(info))

    def get_scene_info(self):
        return {
            "name": "weak_read_troubleshooting",
            "info_en": "Troubleshooting of weak reading FAQs",
            "info_cn": "弱读常见问题排查",
            "example": "obdiag rca run --scene=weak_read_troubleshooting --env tenant_id=1001",
        }

    def init(self, context):
        super().init(context)
        min_supported_version = "4.0.0.0"
        observer_version = self.observer_version
        if observer_version is None or len(observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (observer_version == "4.0.0.0" or StringUtils.compare_versions_greater(observer_version, "4.0.0.0")):
            self.stdio.error("observer version is {0}, which is less than {1}.".format(observer_version, min_supported_version))
            raise RCAInitException("observer version is {0}, which is less than {1}.".format(observer_version, min_supported_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")
        self.work_path = context.get_variable('store_dir')
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        self.local_path = self.work_path

        # Get tenant_id from input parameters
        tenant_id_str = self.input_parameters.get("tenant_id")
        if tenant_id_str:
            try:
                self.tenant_id = int(tenant_id_str)
            except Exception as e:
                raise RCAInitException("tenant_id is not a valid number: {0}".format(tenant_id_str))
        else:
            # If not specified, check all tenants
            self.tenant_id = None

        self.record.add_record("Starting weak read troubleshooting. tenant_id: {0}".format(self.tenant_id if self.tenant_id else "all"))

    def execute(self):
        try:
            # Check 1: weak read timestamp not ready
            self._check_weak_read_timestamp_not_ready()

            # Check 2: weak read lag
            self._check_weak_read_lag()

            # Check 3: max_stale_time_for_weak_consistency configuration
            self._check_max_stale_time_config()

            # Check 4: weak read timeout issues
            self._check_weak_read_timeout()

            # Check 5: log disk space issues
            self._check_log_disk_space()

            # Check 6: weak read consistency issues
            self._check_weak_read_consistency()

            # Check 7: network latency (EASY SLOW logs)
            self._check_network_latency()

            # Check 8: Leader distribution
            self._check_leader_distribution()

            # Check 9: GTS timestamp service
            self._check_gts_timestamp_service()

        except RCANotNeedExecuteException as e:
            self.stdio.print("[Not Need Execute]WeakReadTroubleshooting need not execute: {0}".format(e))
        except Exception as e:
            self.stdio.error("WeakReadTroubleshooting execute error: {0}".format(e))
            raise RCAExecuteException("WeakReadTroubleshooting execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end WeakReadTroubleshooting execute")

    def _check_weak_read_timestamp_not_ready(self):
        """Check weak read timestamp not ready issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 1: Weak read timestamp not ready")
        self.record.add_record("=" * 60)

        try:
            # Check __all_virtual_ls_info for weak_read_scn
            # Note: Use checkpoint_scn instead of readable_scn (which doesn't exist in OceanBase 4.4+)
            if self.tenant_id:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, weak_read_scn, checkpoint_scn from oceanbase.__all_virtual_ls_info where tenant_id={0}".format(self.tenant_id)
            else:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, weak_read_scn, checkpoint_scn from oceanbase.__all_virtual_ls_info"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            ls_info_data = cursor.fetchall()

            if not ls_info_data:
                self.record.add_record("No log stream information found")
                return

            # Save results
            self._save_sql_result(ls_info_data, "weak_read_ls_info")

            issues_found = False
            for ls_info in ls_info_data:
                tenant_id = ls_info.get("tenant_id")
                ls_id = ls_info.get("ls_id")
                svr_ip = ls_info.get("svr_ip")
                svr_port = ls_info.get("svr_port")
                weak_read_scn = ls_info.get("weak_read_scn")
                checkpoint_scn = ls_info.get("checkpoint_scn")

                # Check for zero or invalid weak_read_scn first
                if not weak_read_scn or weak_read_scn == "0" or weak_read_scn == 0:
                    issues_found = True
                    self.record.add_record("WARNING: tenant_id={0}, ls_id={1}, svr_ip={2}:{3}, weak_read_scn is zero or invalid: {4}".format(tenant_id, ls_id, svr_ip, svr_port, weak_read_scn))
                    self.record.add_suggest("Weak read SCN is zero or invalid. This indicates weak read timestamp is not ready. Check log disk space and replica sync status.")
                    continue

                # Check if weak_read_scn is significantly behind checkpoint_scn
                if weak_read_scn and checkpoint_scn:
                    try:
                        weak_read_scn_val = int(weak_read_scn) if weak_read_scn else 0
                        checkpoint_scn_val = int(checkpoint_scn) if checkpoint_scn else 0
                        lag = checkpoint_scn_val - weak_read_scn_val

                        if lag > 1000000:  # Significant lag threshold
                            issues_found = True
                            self.record.add_record("WARNING: tenant_id={0}, ls_id={1}, svr_ip={2}:{3}, weak_read_scn={4}, checkpoint_scn={5}, lag={6}".format(tenant_id, ls_id, svr_ip, svr_port, weak_read_scn, checkpoint_scn, lag))
                            self.record.add_suggest("Weak read SCN is significantly behind checkpoint SCN. This may cause weak read timestamp not ready issues.")
                    except (ValueError, TypeError) as e:
                        self.verbose("Error parsing SCN values: {0}".format(e))
                        pass

            if not issues_found:
                self.record.add_record("No weak read timestamp not ready issues found")
            else:
                # Gather related logs
                self._gather_weak_read_logs()

        except Exception as e:
            self.record.add_record("Error checking weak read timestamp: {0}".format(str(e)))
            self.stdio.error("Error in _check_weak_read_timestamp_not_ready: {0}".format(e))

    def _check_weak_read_lag(self):
        """Check weak read lag issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 2: Weak read lag")
        self.record.add_record("=" * 60)

        try:
            # Check weak read lag by comparing weak_read_scn and checkpoint_scn
            # Note: Use checkpoint_scn instead of readable_scn (which doesn't exist in OceanBase 4.4+)
            # Note: Don't calculate lag in SQL to avoid BIGINT UNSIGNED overflow when checkpoint_scn < weak_read_scn
            if self.tenant_id:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, weak_read_scn, checkpoint_scn from oceanbase.__all_virtual_ls_info where tenant_id={0} and weak_read_scn > 0 and checkpoint_scn > 0".format(self.tenant_id)
            else:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, weak_read_scn, checkpoint_scn from oceanbase.__all_virtual_ls_info where weak_read_scn > 0 and checkpoint_scn > 0"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            lag_data = cursor.fetchall()

            if lag_data:
                self._save_sql_result(lag_data, "weak_read_lag_info")

                high_lag_count = 0
                for ls_info in lag_data:
                    try:
                        weak_read_scn = int(ls_info.get("weak_read_scn", 0))
                        checkpoint_scn = int(ls_info.get("checkpoint_scn", 0))
                        # Calculate lag in Python to avoid BIGINT UNSIGNED overflow
                        # If weak_read_scn > checkpoint_scn, lag is negative (normal case, no lag)
                        lag_val = checkpoint_scn - weak_read_scn
                        if lag_val > 1000000:  # Significant lag threshold (positive means weak_read is behind)
                            high_lag_count += 1
                            if high_lag_count <= 5:  # Show first 5
                                self.record.add_record("WARNING: tenant_id={0}, ls_id={1}, svr_ip={2}:{3}, weak_read_scn lag={4}".format(ls_info.get("tenant_id"), ls_info.get("ls_id"), ls_info.get("svr_ip"), ls_info.get("svr_port"), lag_val))
                    except (ValueError, TypeError):
                        pass

                if high_lag_count > 0:
                    self.record.add_record("Found {0} log streams with significant weak read lag".format(high_lag_count))
                    self.record.add_suggest("Weak read SCN is significantly behind checkpoint SCN, indicating lag. Check replica sync status and network connectivity.")
                else:
                    self.record.add_record("No significant weak read lag detected")
            else:
                self.record.add_record("No weak read lag data available")

        except Exception as e:
            self.record.add_record("Error checking weak read lag: {0}".format(str(e)))
            self.stdio.error("Error in _check_weak_read_lag: {0}".format(e))

    def _check_max_stale_time_config(self):
        """Check max_stale_time_for_weak_consistency configuration"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 3: max_stale_time_for_weak_consistency configuration")
        self.record.add_record("=" * 60)

        try:
            sql = "select * from oceanbase.GV$OB_PARAMETERS where name='max_stale_time_for_weak_consistency'"
            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            param_data = cursor.fetchall()

            if not param_data:
                self.record.add_record("max_stale_time_for_weak_consistency parameter not found")
                return

            self._save_sql_result(param_data, "max_stale_time_config")

            default_value = "5s"
            issues_found = False

            for param in param_data:
                tenant_id = param.get("TENANT_ID")
                svr_ip = param.get("SVR_IP")
                svr_port = param.get("SVR_PORT")
                value = param.get("VALUE", "")

                if value.lower() != default_value.lower():
                    issues_found = True
                    self.record.add_record("WARNING: tenant_id={0}, svr_ip={1}:{2}, max_stale_time_for_weak_consistency={3} (default is {4})".format(tenant_id, svr_ip, svr_port, value, default_value))
                    self.record.add_suggest("max_stale_time_for_weak_consistency is not set to default value {0}. This may affect weak read behavior.".format(default_value))

            if not issues_found:
                self.record.add_record("max_stale_time_for_weak_consistency is configured correctly (default: {0})".format(default_value))

        except Exception as e:
            self.record.add_record("Error checking max_stale_time config: {0}".format(str(e)))
            self.stdio.error("Error in _check_max_stale_time_config: {0}".format(e))

    def _check_weak_read_timeout(self):
        """Check weak read timeout issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 4: Weak read timeout")
        self.record.add_record("=" * 60)

        try:
            # Gather logs for weak read timeout errors
            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("weak read.*timeout")
            self.gather_log.grep("weak_read.*timeout")
            self.gather_log.grep("weak read service.*timeout")

            log_path = os.path.join(self.work_path, "weak_read_timeout_logs")
            logs_name = self.gather_log.execute(save_path=log_path)

            if logs_name and len(logs_name) > 0:
                self.record.add_record("Found weak read timeout related logs: {0}".format(len(logs_name)))
                self.record.add_suggest("Review the collected logs for weak read timeout errors. Check network connectivity and replica availability.")
                for log_name in logs_name:
                    self.record.add_record("Log file: {0}".format(log_name))
            else:
                self.record.add_record("No weak read timeout errors found in logs")

        except Exception as e:
            self.record.add_record("Error checking weak read timeout: {0}".format(str(e)))
            self.stdio.error("Error in _check_weak_read_timeout: {0}".format(e))

    def _check_log_disk_space(self):
        """Check log disk space issues that may affect weak read"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 5: Log disk space")
        self.record.add_record("=" * 60)

        try:
            # Check for log disk space warnings
            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("log disk space is almost full")
            self.gather_log.grep("log disk.*full")
            self.gather_log.grep("clog.*disk.*full")

            log_path = os.path.join(self.work_path, "log_disk_space_logs")
            logs_name = self.gather_log.execute(save_path=log_path)

            if logs_name and len(logs_name) > 0:
                self.record.add_record("WARNING: Found log disk space warnings in logs: {0} files".format(len(logs_name)))
                self.record.add_suggest("Log disk space is almost full. This may cause weak read timestamp generation issues. Please free up disk space.")
                for log_name in logs_name:
                    self.record.add_record("Log file: {0}".format(log_name))
            else:
                self.record.add_record("No log disk space warnings found")

        except Exception as e:
            self.record.add_record("Error checking log disk space: {0}".format(str(e)))
            self.stdio.error("Error in _check_log_disk_space: {0}".format(e))

    def _check_weak_read_consistency(self):
        """Check weak read consistency issues"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 6: Weak read consistency")
        self.record.add_record("=" * 60)

        try:
            # Check for weak read consistency errors in logs
            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("weak read.*consistency")
            self.gather_log.grep("weak_read.*consistency")
            self.gather_log.grep("READ_CONSISTENCY.*WEAK.*error")

            log_path = os.path.join(self.work_path, "weak_read_consistency_logs")
            logs_name = self.gather_log.execute(save_path=log_path)

            if logs_name and len(logs_name) > 0:
                self.record.add_record("Found weak read consistency related logs: {0} files".format(len(logs_name)))
                self.record.add_suggest("Review the collected logs for weak read consistency issues.")
                for log_name in logs_name:
                    self.record.add_record("Log file: {0}".format(log_name))
            else:
                self.record.add_record("No weak read consistency errors found in logs")

            # Check replica availability using __all_ls_meta_table (compatible with OceanBase 4.x)
            if self.tenant_id:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, role, replica_type, replica_status from oceanbase.__all_ls_meta_table where tenant_id={0}".format(self.tenant_id)
            else:
                sql = "select tenant_id, ls_id, svr_ip, svr_port, role, replica_type, replica_status from oceanbase.__all_ls_meta_table"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            replica_data = cursor.fetchall()

            if replica_data:
                self._save_sql_result(replica_data, "ls_replica_info")

                # Check for replicas with abnormal status (not NORMAL)
                abnormal_replicas = [r for r in replica_data if r.get("replica_status") != "NORMAL"]
                if abnormal_replicas:
                    self.record.add_record("WARNING: Found {0} replicas with abnormal status".format(len(abnormal_replicas)))
                    for replica in abnormal_replicas[:5]:  # Show first 5
                        self.record.add_record(
                            "tenant_id={0}, ls_id={1}, svr_ip={2}:{3}, role={4}, replica_status={5}".format(replica.get("tenant_id"), replica.get("ls_id"), replica.get("svr_ip"), replica.get("svr_port"), replica.get("role"), replica.get("replica_status"))
                        )
                    self.record.add_suggest("Some replicas have abnormal status, which may affect weak read consistency.")
                else:
                    self.record.add_record("All replicas have normal status")

        except Exception as e:
            self.record.add_record("Error checking weak read consistency: {0}".format(str(e)))
            self.stdio.error("Error in _check_weak_read_consistency: {0}".format(e))

    def _gather_weak_read_logs(self):
        """Gather logs related to weak read issues"""
        try:
            work_path_weak_read = os.path.join(self.local_path, "weak_read_timestamp_logs")
            if not os.path.exists(work_path_weak_read):
                os.makedirs(work_path_weak_read)

            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("generate_weak_read_timestamp_")
            self.gather_log.grep("weak_read_scn")
            self.gather_log.grep("weak read ts is not ready")
            self.gather_log.grep("weak_read.*not.*ready")

            logs_name = self.gather_log.execute(save_path=work_path_weak_read)

            if logs_name and len(logs_name) > 0:
                self.record.add_record("Gathered weak read related logs: {0} files in {1}".format(len(logs_name), work_path_weak_read))
                for log_name in logs_name[:5]:  # Show first 5 log files
                    self.record.add_record("  - {0}".format(log_name))
            else:
                self.record.add_record("No weak read related logs found")

        except Exception as e:
            self.stdio.error("Error gathering weak read logs: {0}".format(e))
            self.record.add_record("Error gathering weak read logs: {0}".format(str(e)))

    def _save_sql_result(self, data: List[Dict], filename: str):
        """Save SQL query result to file"""
        try:
            if not data:
                return

            file_path = os.path.join(self.work_path, "{0}.txt".format(filename))
            with open(file_path, 'w', encoding='utf-8') as f:
                if data:
                    # Write header
                    columns = list(data[0].keys())
                    f.write('\t'.join(columns) + '\n')
                    # Write data
                    for row in data:
                        line = '\t'.join([str(row.get(col, '')) for col in columns])
                        f.write(line + '\n')

            self.verbose("Saved SQL result to {0}".format(file_path))

        except Exception as e:
            self.stdio.error("Error saving SQL result: {0}".format(e))

    def _check_network_latency(self):
        """Check network latency issues by analyzing EASY SLOW logs"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 7: Network latency (EASY SLOW)")
        self.record.add_record("=" * 60)

        try:
            # Gather EASY SLOW logs which indicate network latency issues
            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("EASY SLOW")

            log_path = os.path.join(self.work_path, "easy_slow_logs")
            logs_name = self.gather_log.execute(save_path=log_path)

            if logs_name and len(logs_name) > 0:
                # Count EASY SLOW occurrences
                total_count = 0
                for log_file in logs_name:
                    try:
                        with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            count = content.count("EASY SLOW")
                            total_count += count
                    except Exception as e:
                        self.verbose("Error reading log file {0}: {1}".format(log_file, e))

                if total_count >= 1000:
                    self.record.add_record("CRITICAL: Found {0} EASY SLOW entries in logs (>= 1000, severe network latency)".format(total_count))
                    self.record.add_suggest("Severe network latency detected. Check network connectivity, switch status, and network bandwidth. Use 'tsar' or 'sar -n DEV' to monitor network performance.")
                elif total_count > 0:
                    self.record.add_record("WARNING: Found {0} EASY SLOW entries in logs (network latency exists)".format(total_count))
                    self.record.add_suggest("Network latency detected. Check network connectivity between OceanBase nodes. Consider using 'ping' and 'netstat -s | grep retransmit' to diagnose.")
                else:
                    self.record.add_record("EASY SLOW logs found but count is 0, logs saved to: {0}".format(log_path))
            else:
                self.record.add_record("No EASY SLOW logs found, network latency is normal")

        except Exception as e:
            self.record.add_record("Error checking network latency: {0}".format(str(e)))
            self.stdio.error("Error in _check_network_latency: {0}".format(e))

    def _check_leader_distribution(self):
        """Check Leader distribution across servers"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 8: Leader distribution")
        self.record.add_record("=" * 60)

        try:
            # Query Leader distribution from __all_ls_meta_table
            # role = 1 means LEADER, role = 2 means FOLLOWER
            if self.tenant_id:
                sql = """
                SELECT svr_ip, svr_port, COUNT(*) as leader_count 
                FROM oceanbase.__all_ls_meta_table 
                WHERE role = 1 AND tenant_id = {0}
                GROUP BY svr_ip, svr_port 
                ORDER BY leader_count DESC
                """.format(
                    self.tenant_id
                )
            else:
                sql = """
                SELECT svr_ip, svr_port, COUNT(*) as leader_count 
                FROM oceanbase.__all_ls_meta_table 
                WHERE role = 1 
                GROUP BY svr_ip, svr_port 
                ORDER BY leader_count DESC
                """

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            leader_data = cursor.fetchall()

            if leader_data:
                self._save_sql_result(leader_data, "leader_distribution")

                # Check if Leader distribution is balanced
                leader_counts = [int(row.get("leader_count", 0)) for row in leader_data]
                if len(leader_counts) > 1:
                    max_count = max(leader_counts)
                    min_count = min(leader_counts)
                    avg_count = sum(leader_counts) / len(leader_counts)

                    # Calculate imbalance ratio
                    if avg_count > 0:
                        imbalance_ratio = (max_count - min_count) / avg_count
                    else:
                        imbalance_ratio = 0

                    self.record.add_record("Leader distribution across {0} servers:".format(len(leader_data)))
                    for row in leader_data:
                        self.record.add_record("  svr_ip={0}:{1}, leader_count={2}".format(row.get("svr_ip"), row.get("svr_port"), row.get("leader_count")))

                    if imbalance_ratio > 0.5:  # More than 50% difference
                        self.record.add_record("WARNING: Leader distribution is unbalanced (imbalance_ratio={0:.2f})".format(imbalance_ratio))
                        self.record.add_suggest("Leader distribution is unbalanced. This may cause uneven load and affect weak read performance. Consider using 'ALTER SYSTEM ADMIN SWITCH REPLICA' to rebalance.")
                    else:
                        self.record.add_record("Leader distribution is balanced (imbalance_ratio={0:.2f})".format(imbalance_ratio))
                else:
                    self.record.add_record("Only one server found with Leaders, cannot check distribution balance")
                    for row in leader_data:
                        self.record.add_record("  svr_ip={0}:{1}, leader_count={2}".format(row.get("svr_ip"), row.get("svr_port"), row.get("leader_count")))
            else:
                self.record.add_record("No Leader information found")

        except Exception as e:
            self.record.add_record("Error checking Leader distribution: {0}".format(str(e)))
            self.stdio.error("Error in _check_leader_distribution: {0}".format(e))

    def _check_gts_timestamp_service(self):
        """Check GTS (Global Timestamp Service) status"""
        self.record.add_record("=" * 60)
        self.record.add_record("Check 9: GTS timestamp service")
        self.record.add_record("=" * 60)

        try:
            # Query GTS timestamp service status from V$OB_TIMESTAMP_SERVICE
            if self.tenant_id:
                sql = "SELECT TENANT_ID, TS_TYPE, TS_VALUE, SVR_IP, SVR_PORT FROM oceanbase.V$OB_TIMESTAMP_SERVICE WHERE TENANT_ID = {0}".format(self.tenant_id)
            else:
                sql = "SELECT TENANT_ID, TS_TYPE, TS_VALUE, SVR_IP, SVR_PORT FROM oceanbase.V$OB_TIMESTAMP_SERVICE"

            self.verbose("Execute SQL: {0}".format(sql))
            cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
            gts_data = cursor.fetchall()

            if gts_data:
                self._save_sql_result(gts_data, "gts_timestamp_service")

                self.record.add_record("GTS timestamp service status:")
                issues_found = False

                # Group by tenant to check timestamp consistency across servers
                tenant_ts_map = {}
                for row in gts_data:
                    tenant_id = row.get("TENANT_ID")
                    ts_value = row.get("TS_VALUE")
                    svr_ip = row.get("SVR_IP")
                    svr_port = row.get("SVR_PORT")
                    ts_type = row.get("TS_TYPE")

                    if tenant_id not in tenant_ts_map:
                        tenant_ts_map[tenant_id] = []
                    tenant_ts_map[tenant_id].append({"ts_value": int(ts_value) if ts_value else 0, "svr_ip": svr_ip, "svr_port": svr_port, "ts_type": ts_type})

                for tenant_id, ts_list in tenant_ts_map.items():
                    if len(ts_list) > 1:
                        ts_values = [t["ts_value"] for t in ts_list]
                        max_ts = max(ts_values)
                        min_ts = min(ts_values)
                        ts_diff = max_ts - min_ts

                        # If timestamp difference is too large (> 1 second = 1000000000 ns in SCN)
                        # SCN format: timestamp_ns * 1000 + sequence, so 1s diff is roughly 1e12
                        if ts_diff > 1000000000000:  # 1 second difference threshold
                            issues_found = True
                            self.record.add_record("WARNING: tenant_id={0}, GTS timestamp difference is large: {1}".format(tenant_id, ts_diff))
                            for t in ts_list:
                                self.record.add_record("  svr_ip={0}:{1}, ts_type={2}, ts_value={3}".format(t["svr_ip"], t["svr_port"], t["ts_type"], t["ts_value"]))
                            self.record.add_suggest("GTS timestamp difference is large across servers. Check NTP synchronization and network connectivity.")
                        else:
                            self.record.add_record("tenant_id={0}, GTS timestamp is synchronized (diff={1})".format(tenant_id, ts_diff))
                    else:
                        # Single server, just record the info
                        t = ts_list[0]
                        self.record.add_record("tenant_id={0}, svr_ip={1}:{2}, ts_type={3}, ts_value={4}".format(tenant_id, t["svr_ip"], t["svr_port"], t["ts_type"], t["ts_value"]))

                # Check for invalid timestamp values
                for row in gts_data:
                    ts_value = row.get("TS_VALUE")
                    if not ts_value or int(ts_value) == 0:
                        issues_found = True
                        self.record.add_record("CRITICAL: tenant_id={0}, GTS timestamp is zero or invalid!".format(row.get("TENANT_ID")))
                        self.record.add_suggest("GTS timestamp is invalid. This will severely affect weak read. Check timestamp service health.")

                if not issues_found:
                    self.record.add_record("GTS timestamp service is healthy")
            else:
                self.record.add_record("No GTS timestamp service information found")

            # Also check NTP-related logs
            self.gather_log.set_parameters("scope", "observer")
            self.gather_log.grep("clock.*skew")
            self.gather_log.grep("time.*drift")
            self.gather_log.grep("ntp.*error")

            log_path = os.path.join(self.work_path, "clock_sync_logs")
            logs_name = self.gather_log.execute(save_path=log_path)

            if logs_name and len(logs_name) > 0:
                self.record.add_record("WARNING: Found clock synchronization related logs: {0} files".format(len(logs_name)))
                self.record.add_suggest("Clock synchronization issues detected. Check NTP service status with 'ntpq -p' or 'chronyc sources'.")
                for log_name in logs_name[:3]:
                    self.record.add_record("Log file: {0}".format(log_name))
            else:
                self.record.add_record("No clock synchronization issues found in logs")

        except Exception as e:
            self.record.add_record("Error checking GTS timestamp service: {0}".format(str(e)))
            self.stdio.error("Error in _check_gts_timestamp_service: {0}".format(e))


weak_read_troubleshooting = WeakReadTroubleshooting()
