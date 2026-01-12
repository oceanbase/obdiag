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
@time: 2025/04/8
@file: tenant_locality_consistency_check.py
@desc: Check tenant locality consistency with log stream member count
"""

import re
from collections import defaultdict
from src.handler.check.check_task import TaskBase
from src.common.tool import StringUtils


class TenantLocalityConsistencyCheck(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def parse_locality_replica_count(self, locality):
        """
        Parse LOCALITY string to extract FULL replica count
        Example: "FULL@zone1, FULL@zone2" -> 2
        Example: "FULL{1}@zone1" -> 1
        Support abbreviated form F@zone and F{1}@zone
        """
        if not locality:
            return 0

        total_replicas = 0

        # Match FULL{1}@zone or F{1}@zone format (with replica count)
        replica_count_pattern = r'\b(FULL|F)\{(\d+)\}@'
        replica_count_matches = re.findall(replica_count_pattern, locality, re.IGNORECASE)

        for match in replica_count_matches:
            count = int(match[1])
            total_replicas += count

        # Match FULL@zone or F@zone format (without replica count, default to 1)
        simple_pattern = r'\b(FULL|F)(?!\{)@'
        simple_matches = re.findall(simple_pattern, locality, re.IGNORECASE)

        total_replicas += len(simple_matches)

        return total_replicas

    def execute(self):
        try:
            if self.ob_connector is None:
                return self.report.add_critical("Database connection is not available.")
            if StringUtils.compare_versions_greater(self.observer_version, "4.0.0.0"):
                pass
            else:
                return None
            self.stdio.verbose("Starting tenant locality consistency check...")

            # Query tenant information
            tenant_query = """
            SELECT TENANT_ID, TENANT_NAME, LOCALITY, STATUS 
            FROM oceanbase.DBA_OB_TENANTS 
            WHERE TENANT_TYPE = 'USER'
            """
            tenants = self.ob_connector.execute_sql_return_cursor_dictionary(tenant_query).fetchall()
            self.stdio.verbose(f"Found {len(tenants)} user tenants")

            if not tenants:
                self.stdio.verbose("No user tenants found")
                return

            # Query log stream information
            log_stat_query = """
            SELECT TENANT_ID, LS_ID, PAXOS_REPLICA_NUM, ROLE, PAXOS_MEMBER_LIST
            FROM oceanbase.GV$OB_LOG_STAT
            """
            log_stats = self.ob_connector.execute_sql_return_cursor_dictionary(log_stat_query).fetchall()
            self.stdio.verbose(f"Found {len(log_stats)} log stream records")

            # Group log streams by tenant
            log_by_tenant = defaultdict(list)
            for row in log_stats:
                log_by_tenant[row['TENANT_ID']].append(row)

            # Check each tenant
            for tenant in tenants:
                tenant_id = tenant['TENANT_ID']
                tenant_name = tenant['TENANT_NAME']
                locality = tenant['LOCALITY']
                status = tenant['STATUS']

                self.stdio.verbose(f"Checking tenant: {tenant_name} (ID: {tenant_id})")
                self.stdio.verbose(f"Tenant locality: {locality}, status: {status}")

                expected_replicas = self.parse_locality_replica_count(locality)
                actual_logs = log_by_tenant.get(tenant_id, [])

                if not actual_logs:
                    self.report.add_critical(f"Tenant {tenant_name} (ID: {tenant_id}) has no log streams, tenant may be unavailable")
                    continue

                # Check if all log streams have consistent PAXOS_REPLICA_NUM
                replica_nums = set()
                for log in actual_logs:
                    replica_nums.add(log['PAXOS_REPLICA_NUM'])

                if len(replica_nums) > 1:
                    self.report.add_critical(f"Tenant {tenant_name} (ID: {tenant_id}) has inconsistent PAXOS_REPLICA_NUM across log streams: {replica_nums}")
                    continue

                actual_replicas = list(replica_nums)[0]
                self.stdio.verbose(f"Tenant {tenant_name} expected replicas: {expected_replicas}, actual replicas: {actual_replicas}")

                # Check if LOCALITY replica count matches log stream replica count
                if actual_replicas != expected_replicas:
                    self.report.add_warning(f"Tenant {tenant_name} (ID: {tenant_id}) locality replica count ({expected_replicas}) " f"does not match log stream member count ({actual_replicas}), tenant status abnormal, " f"tenant may be unavailable")

                # Check if there is a LEADER
                has_leader = any(log['ROLE'] == 'LEADER' for log in actual_logs)
                if not has_leader:
                    self.report.add_critical(f"Tenant {tenant_name} (ID: {tenant_id}) has no LEADER in log streams, tenant is unavailable")
                else:
                    self.stdio.verbose(f"Tenant {tenant_name} has LEADER in log streams")

            self.stdio.verbose("Tenant locality consistency check completed")

        except Exception as e:
            self.report.add_fail(f"Execution error during tenant locality consistency check: {e}")
            self.stdio.warn(f"Exception occurred: {e}")

    def get_task_info(self):
        return {
            "name": "tenant_locality_consistency_check",
            "info": "Check tenant locality consistency with log stream member count to ensure tenant availability",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/1048",
        }


# Register the task
tenant_locality_consistency_check = TenantLocalityConsistencyCheck()
