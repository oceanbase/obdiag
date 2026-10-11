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
@time: 2026/6/8
@file: major_freeze_not_allow.py
@desc: RCA scene for manual major freeze rejected by OB_MAJOR_FREEZE_NOT_ALLOW.
"""

import os

from src.handler.rca.rca_exception import RCAExecuteException, RCAInitException
from src.handler.rca.rca_handler import RcaScene
from src.common.tool import StringUtils


class MajorFreezeNotAllowScene(RcaScene):
    ERROR_PATTERNS = ("OB_MAJOR_FREEZE_NOT_ALLOW", "ERROR 4217", "-4217", "MAJOR FREEZE NOT ALLOWED NOW")

    def __init__(self):
        super().__init__()
        self.tenant_name = None
        self.error_msg = None
        self.collect_logs = True

    def verbose(self, info):
        self.stdio.verbose("[MajorFreezeNotAllowScene] {0}".format(info))

    def get_scene_info(self):
        return {
            "name": "major_freeze_not_allow",
            "info_en": "Diagnose OB_MAJOR_FREEZE_NOT_ALLOW when manually executing major freeze.",
            "info_cn": "诊断手动执行合并时出现 OB_MAJOR_FREEZE_NOT_ALLOW 的原因",
            "example": "obdiag rca run --scene=major_freeze_not_allow --env tenant_name=xxx [--env error_msg='ERROR 4217 (HY000): Major freeze not allowed now']",
        }

    def init(self, context):
        super().init(context)
        min_supported_version = "4.0.0.0"
        if self.observer_version is None or len(self.observer_version.strip()) == 0:
            raise RCAInitException("observer version is None. Please check the NODES conf.")
        if not (self.observer_version == min_supported_version or StringUtils.compare_versions_greater(self.observer_version, min_supported_version)):
            raise RCAInitException("observer version is {0}, which is less than {1}.".format(self.observer_version, min_supported_version))
        if self.ob_connector is None:
            raise RCAInitException("ob_connector is None. Please check the NODES conf.")

        self.tenant_name = self.input_parameters.get("tenant_name")
        if not self.tenant_name:
            raise RCAInitException("tenant_name is required. Please use --env tenant_name=xxx.")

        self.error_msg = self.input_parameters.get("error_msg")
        collect_logs = str(self.input_parameters.get("collect_logs", "true")).lower()
        self.collect_logs = collect_logs not in ("0", "false", "no")
        self.work_path = context.get_variable("store_dir")
        if not os.path.exists(self.work_path):
            os.makedirs(self.work_path)
        self.record.add_record("major_freeze_not_allow init: tenant_name={0}, collect_logs={1}".format(self.tenant_name, self.collect_logs))

    @classmethod
    def is_major_freeze_not_allow_error(cls, error_msg):
        if not error_msg:
            return True
        normalized_error_msg = str(error_msg).upper()
        return any(pattern in normalized_error_msg for pattern in cls.ERROR_PATTERNS)

    @staticmethod
    def _escape_sql_literal(value):
        return str(value).replace("'", "''")

    def _query_tenant_role(self, tenant_name):
        tenant_name_escaped = self._escape_sql_literal(tenant_name)
        sql = "SELECT tenant_name, tenant_role FROM oceanbase.DBA_OB_TENANTS WHERE tenant_name = '{0}'".format(tenant_name_escaped)
        self.record.add_record("Query tenant role: {0}".format(sql))
        cursor = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
        rows = cursor.fetchall()
        if not rows:
            return None
        return rows[0]

    @staticmethod
    def _row_get(row, *keys):
        for key in keys:
            if key in row:
                return row.get(key)
        return None

    def _check_tenant_role(self):
        row = self._query_tenant_role(self.tenant_name)
        if row is None:
            self.record.add_record("Tenant '{0}' was not found in oceanbase.DBA_OB_TENANTS.".format(self.tenant_name))
            self.record.add_suggest("Check whether tenant_name is correct, then rerun this RCA scene.")
            return

        tenant_role = self._row_get(row, "tenant_role", "TENANT_ROLE")
        tenant_name = self._row_get(row, "tenant_name", "TENANT_NAME") or self.tenant_name
        self.record.add_record("Tenant role: tenant_name={0}, tenant_role={1}".format(tenant_name, tenant_role))
        if str(tenant_role).upper() == "STANDBY":
            self.record.add_suggest("Tenant '{0}' is a standby tenant. Manual `ALTER SYSTEM MAJOR FREEZE tenant = {0}` is not allowed on standby tenants; execute manual major freeze on the primary tenant or wait for synchronization.".format(tenant_name))
        else:
            self.record.add_suggest("Tenant '{0}' role is {1}, not STANDBY. OB_MAJOR_FREEZE_NOT_ALLOW is not explained by standby tenant role; continue checking observer/rootservice logs around the failed major freeze.".format(tenant_name, tenant_role))

    def _gather_related_logs(self):
        if not self.collect_logs or self.gather_log is None:
            self.record.add_record("Skip collecting logs for OB_MAJOR_FREEZE_NOT_ALLOW.")
            return
        log_path = os.path.join(self.work_path, "major_freeze_not_allow_logs")
        self.gather_log.set_parameters("scope", "observer")
        self.gather_log.grep("OB_MAJOR_FREEZE_NOT_ALLOW")
        logs_name = self.gather_log.execute(save_path=log_path)
        if logs_name:
            self.record.add_record("Collected OB_MAJOR_FREEZE_NOT_ALLOW logs: {0}".format(logs_name))
        else:
            self.record.add_record("No OB_MAJOR_FREEZE_NOT_ALLOW logs were collected.")

    def execute(self):
        try:
            if not self.is_major_freeze_not_allow_error(self.error_msg):
                self.record.add_record("error_msg does not contain OB_MAJOR_FREEZE_NOT_ALLOW patterns: {0}".format(self.error_msg))
                self.record.add_suggest("This scene is intended for ERROR 4217 / OB_MAJOR_FREEZE_NOT_ALLOW. Please check the error message or choose another RCA scene.")
                return
            self.record.add_record("Start diagnosing OB_MAJOR_FREEZE_NOT_ALLOW for tenant '{0}'.".format(self.tenant_name))
            self._gather_related_logs()
            self._check_tenant_role()
        except Exception as e:
            raise RCAExecuteException("MajorFreezeNotAllowScene execute error: {0}".format(e))
        finally:
            self.stdio.verbose("end MajorFreezeNotAllowScene execute")


major_freeze_not_allow = MajorFreezeNotAllowScene()
