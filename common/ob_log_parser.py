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
@time: 2024/8/1
@file: ob_log_parser.py
@desc:
"""

import re

OceanbaseObjDict = {
    'ObDMLBaseParam': [
        'timeout',
        'schema_version',
        'sql_mode',
        'is_total_quantity_log',
        'table_param',
        'tenant_schema_version',
        'is_ignore',
        'prelock',
        'encrypt_meta',
        'is_batch_stmt',
        'write_flag',
        'spec_seq_no',
        'snapshot',
        'branch_id',
        'check_schema_version',
    ],
    'ObStoreCtx': ['this', 'ls_id', 'ls', 'branch', 'timeout', 'tablet_id', 'table_iter', 'table_version', 'mvcc_acc_ctx', 'tablet_stat', 'is_read_store_ctx'],
    'ObTableDMLParam': ['tenant_schema_version', 'data_table', 'col_descs', 'col_map'],
    'ObTableSchemaParam': ['table_id', 'schema_version', 'table_type', 'index_type', 'index_status', 'shadow_rowkey_column_num', 'fulltext_col_id', 'index_name', 'pk_name', 'columns', 'read_info', 'lob_inrow_threshold'],
    'ObMemtable': [
        'ObITable',
        'this',
        'timestamp',
        'state',
        'freeze_clock',
        'max_schema_version',
        'max_data_schema_version',
        'max_column_cnt',
        'write_ref_cnt',
        'local_allocator',
        'unsubmitted_cnt',
        'unsynced_cnt',
        'logging_blocked',
        'unset_active_memtable_logging_blocked',
        'resolve_active_memtable_left_boundary',
        'contain_hotspot_row',
        'max_end_scn',
        'rec_scn',
        'snapshot_version',
        'migration_clog_checkpoint_scn',
        'is_tablet_freeze',
        'is_force_freeze',
        ['contain_hotspot_row', 'contain_hotspot_row2'],
        'read_barrier',
        'is_flushed',
        'freeze_state',
        'allow_freeze',
        ['mt_stat_.frozen_time', 'frozen_time'],
        ['mt_stat_.ready_for_flush_time', 'ready_for_flush_time'],
        ['mt_stat_.create_flush_dag_time', 'create_flush_dag_time'],
        ['mt_stat_.release_time', 'release_time'],
        ['mt_stat_.push_table_into_gc_queue_time', 'push_table_into_gc_queue_time'],
        ['mt_stat_.last_print_time', 'last_print_time'],
        'ls_id',
        'transfer_freeze_flag',
        'recommend_snapshot_version',
    ],
    'ObMemtable2': [
        'ObITabletMemtable',
        'this',
        'state',
        'max_data_schema_version',
        'max_column_cnt',
        'local_allocator',
        'contain_hotspot_row',
        'snapshot_version',
        ['contain_hotspot_row', 'contain_hotspot_row2'],
        'ls_id',
        'transfer_freeze_flag',
        'recommend_snapshot_version',
    ],
    'ObITabletMemtable': [
        'ObITable',
        'ls_id_',
        'allow_freeze_',
        'is_flushed_',
        'is_tablet_freeze_',
        'logging_blocked_',
        'resolved_active_memtable_left_boundary_',
        'unset_active_memtable_logging_blocked_',
        'has_backoffed_',
        'read_barrier_',
        'freeze_clock_',
        'freeze_state_',
        'unsubmitted_cnt_',
        'init_timestamp_',
        'max_schema_version_',
        'write_ref_cnt_',
        'max_end_scn_',
        'rec_scn_',
        'freeze_scn_',
        'migration_clog_checkpoint_scn_',
        'freezer_',
        'memtable_mgr_handle_',
        ['mt_stat_.frozen_time_', 'frozen_time'],
        ['mt_stat_.ready_for_flush_time_', 'ready_for_flush_time'],
        ['mt_stat_.create_flush_dag_time_', 'create_flush_dag_time'],
        ['mt_stat_.release_time_', 'release_time'],
        ['mt_stat_.push_table_into_gc_queue_time_', 'push_table_into_gc_queue_time'],
        ['mt_stat_.last_print_time_', 'last_print_time'],
    ],
    'ObDagTypeStruct': ['init_dag_prio', 'sys_task_type', 'dag_type_str', 'dag_module_str'],
    'ObTabletMergeInfo': ['is_inited', 'sstable_merge_info', 'sstable_builder'],
    'ObSSTableMergeInfo': [
        'tenant_id',
        'ls_id',
        'tablet_id',
        'compaction_scn',
        'merge_type',
        'merge_cost_time',
        'merge_start_time',
        'merge_finish_time',
        'dag_id',
        'occupy_size',
        'new_flush_occupy_size',
        'original_size',
        'compressed_size',
        'macro_block_count',
        'multiplexed_macro_block_count',
        'new_micro_count_in_new_macro',
        'multiplexed_micro_count_in_new_macro',
        'total_row_count',
        'incremental_row_count',
        'new_flush_data_rate',
        'is_full_merge',
        'progressive_merge_round',
        'progressive_merge_num',
        'concurrent_cnt',
        'start_cg_idx',
        'end_cg_idx',
        'suspect_add_time',
        'early_create_time',
        'dag_ret',
        'retry_cnt',
        'task_id',
        'error_location',
        'kept_snapshot_info',
        'merge_level',
        'parallel_merge_info',
        'filter_statistics',
        'participant_table_info',
        'macro_id_list',
        'comment',
    ],
    'SCN1': ['val'],
    'SCN': ['val', 'v'],
    'ObLSID': ['id'],
}

OceanbaseObjDetailDict = {
    'ObDMLBaseParam': {
        'table_param': 'ObTableDMLParam',
    },
    'ObTableDMLParam': {
        'data_table': 'ObTableSchemaParam',
    },
    'ObMemtable2': {
        'ObITabletMemtable': 'ObITabletMemtable',
    },
    'ObTabletMergeInfo': {
        'sstable_merge_info': 'ObSSTableMergeInfo',
    },
}

OceanbaseObjCompilePattern = {}

OceanbaseLogVarDict = {
    'Main4377Log': ['column_id', 'storage_old_row', 'sql_old_row', 'dml_param', 'dml_flag', 'store_ctx', 'relative_table'],
    'OldestFrozenLog': ['list'],
    'DumpDagStatusLog': ['type', 'dag_count', 'running_dag_count', 'added_dag_count', 'scheduled_dag_count', 'scheduled_task_count', 'scheduled_data_size'],
    'TenantMemoryLog': [
        'tenant_id',
        'now',
        'active_memstore_used',
        'total_memstore_used',
        'total_memstore_hold',
        'memstore_freeze_trigger_limit',
        'memstore_limit',
        'mem_tenant_limit',
        'mem_tenant_hold',
        'max_mem_memstore_can_get_now',
        'memstore_alloc_pos',
        'memstore_frozen_pos',
        'memstore_reclaimed_pos',
    ],
    'MergeFinishLog': ['ret', 'merge_info', 'sstable', 'mem_peak', 'compat_mode', 'time_guard'],
    'ClogDiskFullLog': [
        'msg',
        'ret',
        ['total_size\(MB\)', 'total_size'],
        ['used_size\(MB\)', 'used_size'],
        ['used_percent\(%\)', 'used_percent'],
        ['warn_size\(MB\)', 'warn_size'],
        ['warn_percent\(%\)', 'warn_percent'],
        ['limit_size\(MB\)', 'limit_size'],
        ['limit_percent\(%\)', 'limit_percent'],
        ['maximum_used_size\(MB\)', 'maximum_used_size'],
        'maximum_log_stream',
        'oldest_log_stream',
        'oldest_scn',
    ],
    'ClogDiskFullLog2': [
        'msg',
        'ret',
        ['total_size\(MB\)', 'total_size'],
        ['used_size\(MB\)', 'used_size'],
        ['used_percent\(%\)', 'used_percent'],
        ['warn_size\(MB\)', 'warn_size'],
        ['warn_percent\(%\)', 'warn_percent'],
        ['limit_size\(MB\)', 'limit_size'],
        ['limit_percent\(%\)', 'limit_percent'],
        ['total_unrecyclable_size_byte\(MB\)', 'total_unrecyclable_size_byte'],
        ['maximum_used_size\(MB\)', 'maximum_used_size'],
        'maximum_log_stream',
        'oldest_log_stream',
        'oldest_scn',
        'in_shrinking',
    ],
    'ClogCPTNoChangeLog': ['checkpoint_scn', 'checkpoint_scn_in_ls_meta', 'ls_id', 'service_type'],
    'LSReplayStatLog': ['id', 'replayed_log_size', 'unreplayed_log_size'],
}

OceanbaseLogVarObjDict = {
    'Main4377Log': {
        'dml_param': 'ObDMLBaseParam',
        'store_ctx': 'ObStoreCtx',
    },
    'OldestFrozenLog': {
        'list': 'not_standard_obj_list',
    },
    'DumpDagStatusLog': {
        'type': 'ObDagTypeStruct',
    },
    'MergeFinishLog': {
        'merge_info': 'ObTabletMergeInfo',
    },
    'ClogDiskFullLog': {
        'oldest_scn': 'SCN1',
    },
    'ClogDiskFullLog2': {'oldest_scn': 'SCN'},
    'ClogCPTNoChangeLog': {
        'checkpoint_scn': 'SCN',
        'checkpoint_scn_in_ls_meta': 'SCN',
        'ls_id': 'ObLSID',
    },
    'LSReplayStatLog': {
        'id': 'ObLSID',
    },
}

OceanbaseLogVarCompilePattern = {}


class ObLogParser:
    compiled_log_pattern = None
    compiled_raw_log_pattern = None

    @staticmethod
    def get_obj_list(list_str):
        # will split with the {}
        res = []
        depth = 0
        obj_start = None
        for i, char in enumerate(list_str):
            if char == '{':
                if depth == 0:
                    # find a Object start position
                    obj_start = i
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0 and obj_start is not None:
                    res.append(list_str[obj_start : i + 1])
                    obj_start = None
        return res

    @staticmethod
    def get_obj_key_list(obj_str):
        # will split with the {}
        key_list = []
        depth = 0
        key_start = None
        for i, char in enumerate(obj_str):
            if char == '{':
                if depth == 0 and key_start is None:
                    key_start = i + 1
                depth += 1
            elif char == '}':
                depth -= 1
            elif char == ',':
                if depth == 1:
                    # 1 for , 1 for ' '
                    key_start = i + 2
            elif char == ':':
                if depth == 1 and key_start is not None:
                    key_list.append(obj_str[key_start:i])
                    key_start = None
        return key_list

    @staticmethod
    def get_obj_parser_pattern(key_list):
        parray = []
        for k in key_list:
            if isinstance(k, list):
                tp = '({0}:(?P<{1}>.*))'.format(k[0], k[1])
            else:
                replace_list = ['.', '(', ')']
                r_k = k
                for ri in replace_list:
                    r_k = r_k.replace(ri, '_')
                s_k = k
                s_k = s_k.replace('(', '\(')
                s_k = s_k.replace(')', '\)')
                tp = '({0}:(?P<{1}>.*))'.format(s_k, r_k)
            parray.append(tp)
        p = '\s*\,\s*'.join(parray) + '\}'
        return p

    @staticmethod
    def get_log_var_parser_pattern(key_list):
        parray = []
        for k in key_list:
            if isinstance(k, list):
                tp = '({0}=(?P<{1}>.*))'.format(k[0], k[1])
            else:
                tp = '({0}=(?P<{0}>.*))'.format(k)
            parray.append(tp)
        p = '\s*\,\s*'.join(parray) + '\)'
        return p

    @staticmethod
    def get_raw_log_var_parser_pattern(key_list):
        parray = []
        for k in key_list:
            if isinstance(k, list):
                tp = '({0}=(?P<{1}>.*))'.format(k[0], k[1])
            else:
                tp = '({0}=(?P<{0}>.*))'.format(k)
            parray.append(tp)
        p = '\s*\ \s*'.join(parray)
        return p

    @staticmethod
    def parse_obj_detail(obj_name, obj_dict):
        # parse all the child str to child obj
        obj_detail_dict = OceanbaseObjDetailDict.get(obj_name, None)
        if not obj_detail_dict:
            print('{} obj detail cannot be parsed'.format(obj_name))
        else:
            for k in obj_dict.keys():
                child_obj_name = obj_detail_dict.get(k, None)
                if child_obj_name:
                    td = ObLogParser.parse_obj(child_obj_name, obj_dict[k])
                    obj_dict[k] = td
                    ObLogParser.parse_obj_detail(child_obj_name, obj_dict[k])

    @staticmethod
    def parse_obj_detail_v2(obj_name, obj_dict):
        # parse all the child str to child obj
        obj_detail_dict = OceanbaseObjDetailDict.get(obj_name, None)
        if not obj_detail_dict:
            # parse all the detail if it start with {
            for k in obj_dict.keys():
                if obj_dict[k].startswith('{'):
                    td = ObLogParser.parse_obj_v2(k, obj_dict[k])
                    obj_dict[k] = td
                    ObLogParser.parse_obj_detail_v2(k, obj_dict[k])
        else:
            for k in obj_dict.keys():
                child_obj_name = obj_detail_dict.get(k, None)
                if child_obj_name:
                    td = ObLogParser.parse_obj(child_obj_name, obj_dict[k])
                    obj_dict[k] = td
                    ObLogParser.parse_obj_detail(child_obj_name, obj_dict[k])

    @staticmethod
    def parse_obj(obj_name, obj_str):
        d = dict()
        key_list = OceanbaseObjDict.get(obj_name, [])
        if len(key_list) == 0:
            print('{} obj cannot be parsed'.format(obj_name))
        else:
            p = OceanbaseObjCompilePattern.get(obj_name, None)
            if p is None:
                tp = ObLogParser.get_obj_parser_pattern(key_list)
                OceanbaseObjCompilePattern[obj_name] = re.compile(tp)
                p = OceanbaseObjCompilePattern[obj_name]
            m = p.finditer(obj_str)
            for i in m:
                d.update(i.groupdict())
        return d

    @staticmethod
    def parse_obj_v2(obj_name, obj_str):
        is_tmp_pattern = False
        d = dict()
        key_list = OceanbaseObjDict.get(obj_name, [])
        if len(key_list) == 0:
            is_tmp_pattern = True
            key_list = ObLogParser.get_obj_key_list(obj_str)
        if len(key_list) != 0:
            p = OceanbaseObjCompilePattern.get(obj_name, None)
            if p is None:
                tp = ObLogParser.get_obj_parser_pattern(key_list)
                OceanbaseObjCompilePattern[obj_name] = re.compile(tp)
                p = OceanbaseObjCompilePattern[obj_name]
            m = p.finditer(obj_str)
            for i in m:
                d.update(i.groupdict())
            if is_tmp_pattern:
                OceanbaseObjCompilePattern[obj_name] = None
        return d

    @staticmethod
    def parse_log_vars_detail(log_name, var_dict):
        var_obj_dict = OceanbaseLogVarObjDict.get(log_name, None)
        if not var_obj_dict:
            print('{} vars detail cannot be parsed'.format(log_name))
        else:
            for k in var_dict.keys():
                var_obj_name = var_obj_dict.get(k, None)
                if var_obj_name == "not_standard_obj_list":
                    tp_obj_list = ObLogParser.get_obj_list(var_dict[k])
                    var_dict[k] = tp_obj_list
                elif var_obj_name:
                    td = ObLogParser.parse_obj(var_obj_name, var_dict[k])
                    var_dict[k] = td
                    ObLogParser.parse_obj_detail(var_obj_name, var_dict[k])

    @staticmethod
    def parse_log_vars_detail_v2(log_name, var_dict):
        var_obj_dict = OceanbaseLogVarObjDict.get(log_name, None)
        if not var_obj_dict:
            # get obj list
            for k in var_dict.keys():
                if var_dict[k].startswith('{'):
                    td = ObLogParser.parse_obj_v2(k, var_dict[k])
                    var_dict[k] = td
                    ObLogParser.parse_obj_detail_v2(k, var_dict[k])
        else:
            for k in var_dict.keys():
                var_obj_name = var_obj_dict.get(k, None)
                if var_obj_name == "not_standard_obj_list":
                    tp_obj_list = ObLogParser.get_obj_list(var_dict[k])
                    var_dict[k] = tp_obj_list
                elif var_obj_name:
                    td = ObLogParser.parse_obj(var_obj_name, var_dict[k])
                    var_dict[k] = td
                    ObLogParser.parse_obj_detail(var_obj_name, var_dict[k])

    @staticmethod
    def parse_raw_log_vars(log_name, var_str):
        d = dict()
        key_list = OceanbaseLogVarDict.get(log_name, [])
        if len(key_list) == 0:
            print('{} lob vars cannot be parsed'.format(log_name))
        else:
            p = OceanbaseLogVarCompilePattern.get(log_name, None)
            if p is None:
                tp = ObLogParser.get_raw_log_var_parser_pattern(key_list)
                OceanbaseLogVarCompilePattern[log_name] = re.compile(tp)
                p = OceanbaseLogVarCompilePattern[log_name]
            m = p.finditer(var_str)
            for i in m:
                d.update(i.groupdict())
        return d

    @staticmethod
    def parse_normal_log_vars(log_name, var_str):
        d = dict()
        key_list = OceanbaseLogVarDict.get(log_name, [])
        if len(key_list) == 0:
            print('{} lob vars cannot be parsed'.format(log_name))
        else:
            p = OceanbaseLogVarCompilePattern.get(log_name, None)
            if p is None:
                tp = ObLogParser.get_log_var_parser_pattern(key_list)
                OceanbaseLogVarCompilePattern[log_name] = re.compile(tp)
                p = OceanbaseLogVarCompilePattern[log_name]
            m = p.finditer(var_str)
            for i in m:
                d.update(i.groupdict())
        return d

    @staticmethod
    def parse_normal_log_vars_v2(var_str):
        d = dict()
        log_name = 'log_vars_v2'
        p = OceanbaseLogVarCompilePattern.get(log_name, None)
        if p is None:
            tp = r'(\w+)=(.*?)(?=\s\w+=|$)'
            OceanbaseLogVarCompilePattern[log_name] = re.compile(tp)
            p = OceanbaseLogVarCompilePattern[log_name]
        m = p.findall(var_str)
        for i in m:
            key = i[0]
            val = i[1].strip(',')
            d[key] = val
        return d

    @staticmethod
    def parse_log_vars(log_name, var_str, log_type=1):
        d = dict()
        if log_type == 1:
            d = ObLogParser.parse_normal_log_vars(log_name, var_str)
        if log_type == 2:
            # raw log
            d = ObLogParser.parse_raw_log_vars(log_name, var_str)
        return d

    @staticmethod
    def parse_raw_print_log(line):
        # parse a log that produced by raw print
        d = dict()
        if ObLogParser.compiled_raw_log_pattern is None:
            msg = "(?P<msg>\[.*\])"
            vars = "(?P<vars>.*)"
            parray = [msg, vars]
            p = '\s*'.join(parray)
            ObLogParser.compiled_raw_log_pattern = re.compile(p)
        m = ObLogParser.compiled_raw_log_pattern.finditer(line)
        for i in m:
            d.update(i.groupdict())
        return d

    @staticmethod
    def parse_log_vars_v2(log_name, var_str, log_type=1):
        d = dict()
        if log_type == 1:
            d = ObLogParser.parse_normal_log_vars_v2(var_str)
        if log_type == 2:
            # raw log
            d = ObLogParser.parse_raw_log_vars(log_name, var_str)
        return d

    @staticmethod
    def parse_log(line):
        # parse a normal log, get all the element
        # get raw print log if it is not a normal log.
        d = dict()
        # 1, means normal log
        # 2, means raw print log
        log_type = 1
        if ObLogParser.compiled_log_pattern is None:
            date_time = "\[(?P<date_time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]"
            log_level = "(?P<log_level>[A-Z]+)"
            module = "\[?(?P<module>[A-Z]+\.*[A-Z]*)?\]?"
            func = "(?P<func>[a-zA-Z_0-9]+\(?\)?)"
            file_no = "\((?P<filename>[a-zA-Z0-9_\.\-]+):(?P<lineno>[0-9]+)\)"
            thread_no = "\[(?P<th_no>[0-9]+)\]"
            thread_name = "\[(?P<th_name>[A-Za-z]+[0-9_A-Za-z]*)?\]"
            tenant_id = "\[T(?P<tenant>[0-9]+)\]"
            trace_id = "\[(?P<trace_id>[A-Za-z\-0-9]+)\]"
            lt = "\[lt=(?P<lt>[0-9]+)\]"
            errcode = "(\[errcode=\-?)?(?P<errcode>[0-9]+)?(\])?"
            msg = "(?P<msg>[A-Za-z\s\,\.\[\]\!\_]+)?"
            variables = "\(?(?P<vars>.*)?\)?$"
            parray = [date_time, log_level, module, func, file_no, thread_no, thread_name, tenant_id, trace_id, lt, errcode, msg, variables]
            p = '\s*'.join(parray)
            ObLogParser.compiled_log_pattern = re.compile(p)
        m = ObLogParser.compiled_log_pattern.finditer(line)
        for i in m:
            d.update(i.groupdict())
        if not d:
            log_type = 2
            d = ObLogParser.parse_raw_print_log(line)
        if d:
            d['log_type'] = log_type
        return d
