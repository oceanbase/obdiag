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
@time: 2024/12/29
@file: bug_182.py
@desc: Check for partition table inconsistency issue after upgrade to 4.2.1
       GitHub issue: https://github.com/oceanbase/obdiag/issues/182
"""

from src.handler.check.check_task import TaskBase


class Bug182Task(TaskBase):
    def init(self, context, report):
        super().init(context, report)

    def execute(self):
        try:
            # Check version requirement: >= 4.2.1.0
            if not super().check_ob_version_min("4.2.1.0"):
                self.stdio.verbose("Version not supported (requires >= 4.2.1.0), skip check")
                return

            if self.ob_connector is None:
                return self.report.add_fail("Database connection is not available")

            # Check for inconsistent partition tables
            sql = '''
                select tenant_id, table_id, table_name, database_id, table_type, load_type, def_type, 
                       rowkey_column_num, index_column_num, max_used_column_id, autoinc_column_id, 
                       auto_increment, read_only, rowkey_split_pos, compress_func_name, expire_condition, 
                       is_use_bloomfilter, comment, block_size, collation_type, data_table_id, index_status, 
                       tablegroup_id, progressive_merge_num, index_type, part_level, part_func_type, 
                       part_func_expr, part_num, sub_part_func_type, sub_part_func_expr, sub_part_num, 
                       schema_version, view_definition, view_check_option, view_is_updatable, index_using_type, 
                       parser_name, index_attributes_set, tablet_size, pctfree, partition_status, 
                       partition_schema_version, session_id, pk_comment, sess_active_time, row_store_type, 
                       store_format, duplicate_scope, progressive_merge_round, storage_format_version, 
                       table_mode, encryption, tablespace_id, sub_part_template_flags, dop, 
                       character_set_client, collation_connection, auto_part_size, auto_part, 
                       association_table_id, tablet_id, max_dependency_version, define_user_id, 
                       transition_point, b_transition_point, interval_range, b_interval_range, 
                       object_status, table_flags, truncate_version, 0 as is_deleted 
                from OCEANBASE.__all_table
                EXCEPT select
                    t1.tenant_id, t1.table_id, t1.table_name, t1.database_id, t1.table_type, t1.load_type, 
                    t1.def_type, t1.rowkey_column_num, t1.index_column_num, t1.max_used_column_id, 
                    t1.autoinc_column_id, t1.auto_increment, t1.read_only, t1.rowkey_split_pos, 
                    t1.compress_func_name, t1.expire_condition, t1.is_use_bloomfilter, t1.comment, 
                    t1.block_size, t1.collation_type, t1.data_table_id, t1.index_status, t1.tablegroup_id, 
                    t1.progressive_merge_num, t1.index_type, t1.part_level, t1.part_func_type, 
                    t1.part_func_expr, t1.part_num, t1.sub_part_func_type, t1.sub_part_func_expr, 
                    t1.sub_part_num, t1.schema_version, t1.view_definition, t1.view_check_option, 
                    t1.view_is_updatable, t1.index_using_type, t1.parser_name, t1.index_attributes_set, 
                    t1.tablet_size, t1.pctfree, t1.partition_status, t1.partition_schema_version, 
                    t1.session_id, t1.pk_comment, t1.sess_active_time, t1.row_store_type, t1.store_format, 
                    t1.duplicate_scope, t1.progressive_merge_round, t1.storage_format_version, 
                    t1.table_mode, t1.encryption, t1.tablespace_id, t1.sub_part_template_flags, t1.dop, 
                    t1.character_set_client, t1.collation_connection, t1.auto_part_size, t1.auto_part, 
                    t1.association_table_id, t1.tablet_id, t1.max_dependency_version, t1.define_user_id, 
                    t1.transition_point, t1.b_transition_point, t1.interval_range, t1.b_interval_range, 
                    t1.object_status, t1.table_flags, t1.truncate_version, t1.is_deleted
                from
                    OCEANBASE.__all_table_history t1
                    inner join (
                        select t2.table_id, max(t2.schema_version) as schema_version 
                        from OCEANBASE.__all_table_history t2 
                        group by t2.table_id
                    ) as t3 on t1.table_id = t3.table_id 
                        and t1.schema_version = t3.schema_version 
                        and t1.is_deleted = 0
            '''

            try:
                result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
                if result and len(result) > 0:
                    self.report.add_warning("Some partition tables are inconsistent. Please get bug's info on https://github.com/oceanbase/obdiag/issues/182")
                    self.stdio.warn("Found {0} inconsistent partition tables".format(len(result)))
                else:
                    self.stdio.verbose("No inconsistent partition tables found")
            except Exception as e:
                self.report.add_fail("Failed to check partition table consistency: {0}".format(e))
                self.stdio.error("SQL execution error: {0}".format(e))

        except Exception as e:
            self.report.add_fail("Execution error: {0}".format(e))

    def get_task_info(self):
        return {
            "name": "bug_182",
            "info": "OB has been upgraded to version 4.2.1, and some partition tables are executing DDL with error code -4109",
            "issue_link": "https://github.com/oceanbase/obdiag/issues/182",
        }


bug_182 = Bug182Task()
