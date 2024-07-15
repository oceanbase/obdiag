#!/usr/bin/env python
# -*- coding: UTF-8 -*
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
@time: 2024/6/21
@file: sys_tenant_meta.py
@desc:
"""
from handler.meta.sql_meta import GlobalSqlMeta
from common.ob_connector import OBConnector
from common.tool import StringUtils


class SysTenantMeta(object):

    def __init__(self, connector: OBConnector, stdio, ob_version='4.0.0.0'):
        self.sys_connector = connector
        self.stdio = stdio
        self.ob_version = ob_version

    def get_tables(self, tenant_id: int, db_name: str):
        if StringUtils.compare_versions_greater(self.ob_version, '4.0.0.0'):
            sql = str(GlobalSqlMeta().get_value(key="get_tables_for_ob4"))
        else:
            sql = str(GlobalSqlMeta().get_value(key="get_tables"))
        sql = sql.replace('##REPLACE_DATABASE_NAME##', db_name)
        self.stdio.verbose("get tables excute SQL: {0}".format(sql))
        columns, rows = self.sys_connector.execute_sql_return_columns_and_data(sql)
        results = dict(zip(columns, rows))
        return results

    def get_database_name(self, tenant_id, database_id):
        sql = str(GlobalSqlMeta().get_value(key="get_database_name"))
        sql = sql.replace('##REPLACE_TENANT_ID##', str(tenant_id)).replace('REPLACE_DATABASE_ID', str(database_id))
        columns, rows = self.sys_connector.execute_sql_return_columns_and_data(sql)
        results = dict(zip(columns, rows))
        return results

    def get_plain_explain(self, tenant_id: int, svr_ip: str, port: int, plan_id: int):
        if StringUtils.compare_versions_greater(self.ob_version, '4.0.0.0'):
            sql = str(GlobalSqlMeta().get_value(key="get_plan_explains_for_ob4"))
        else:
            sql = str(GlobalSqlMeta().get_value(key="get_plan_explains"))
        replacements = {"##REPLACE_TENANT_ID##": str(tenant_id), "##REPLACE_SVR_IP##": svr_ip, "##REPLACE_SVR_PORT##": str(port), "##REPLACE_PLAN_ID##": str(plan_id)}
        for old, new in replacements.items():
            sql = sql.replace(old, new)
        columns, rows = self.sys_connector.execute_sql_return_columns_and_data(sql)
        results = dict(zip(columns, rows))
        return results

    def get_plain_explain_raw(self, tenant_id: int, svr_ip: str, port: int, plan_id: int):
        if StringUtils.compare_versions_greater(self.ob_version, '4.0.0.0'):
            sql = str(GlobalSqlMeta().get_value(key="get_plan_explains_for_ob4"))
        else:
            sql = str(GlobalSqlMeta().get_value(key="get_plan_explains"))
        replacements = {"##REPLACE_TENANT_ID##": str(tenant_id), "##REPLACE_SVR_IP##": svr_ip, "##REPLACE_SVR_PORT##": str(port), "##REPLACE_PLAN_ID##": str(plan_id)}
        for old, new in replacements.items():
            sql = sql.replace(old, new)
        columns, rows = self.sys_connector.execute_sql_return_columns_and_data(sql)
        return columns, rows

    def get_ob_tenant_name_list(self):
        if StringUtils.compare_versions_greater(self.ob_version, '4.0.0.0'):
            sql = str(GlobalSqlMeta().get_value(key="get_tenant_name_list_for_v4"))
        else:
            sql = str(GlobalSqlMeta().get_value(key="get_tenant_name_list"))
        results = self.sys_connector.execute_sql(sql)
        return results
