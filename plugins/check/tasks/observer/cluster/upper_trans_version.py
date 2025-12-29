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
@time: 2025/04/29
@file: upper_trans_version.py
@desc:
"""
from src.common.tool import StringUtils
from src.handler.check.check_task import TaskBase


class UpperTransVersion(TaskBase):

    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            sql = """
            select svr_ip, svr_port, table_type, tenant_id, ls_id, tablet_id, size, usec_to_time(END_LOG_SCN/1000), (CONVERT(UNIX_TIMESTAMP(NOW(6)) * 1000000, UNSIGNED) - (END_LOG_SCN/1000))/1000/1000 as end_scn_gap_seconds from oceanbase.gv$ob_sstables where TABLE_TYPE != 'MEMTABLE' and UPPER_TRANS_VERSION = 9223372036854775807 and (CONVERT(UNIX_TIMESTAMP(NOW(6)) * 1000000, UNSIGNED) - (END_LOG_SCN/1000))/1000/1000 > 3600 * 24 * 2 order by end_scn_gap_seconds desc;
            """
            if self.ob_connector is None:
                return self.report.add_critical("can't build obcluster connection")
            # check version [4.2.5.0, 4.2.5.3)
            if super().check_ob_version_min("4.2.5.0") and StringUtils.compare_versions_greater("4.2.5.3", self.observer_version):
                error_data = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if error_data and len(error_data) > 0:
                    self.report.add_critical(
                        "The upper_trans_version is not empty and cannot be calculated for a long time, which may lead to increased disk usage, slower execution of minor queries, and slower queries. It is recommended to upgrade to OB 4.2.5.3 version. more info: issue #838"
                    )

        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {
            "name": "upper_trans_version",
            "info": "If the OB version is within the range [4.2.5.0, 4.2.5.3), when executing the SQL query in the sys tenant returns non-empty results, prompt the user to upgrade to version ob425_bp3 or higher",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/838",
        }


upper_trans_version = UpperTransVersion()
