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
@time: 2023/11/07
@file: log_entry.py
@desc:
"""
import time
from common.tool import TimeUtils


def find_field_end(data, end_chs=",)}({|][", start=0, end=-1):
    if len(data) == 0:
        return 0
    if end == -1:
        end = len(data)
    old_end_offset = end
    for ch in end_chs:
        end_offset = data.find(ch, start, old_end_offset)
        if end_offset != -1:
            old_end_offset = end_offset
    return old_end_offset


class LogEntry(object):
    WF_LEVELS = {"ERROR", "WARN", "FATAL"}
    __slots__ = ("id", "log_type", "log_file_offset", "log_text_length", "timestamp_us", "parse_succ", "is_trace", "log_level", "component", "source_func", "source_file", "source_line", "th_id", "co_id", "trace_id", "lt", "dc", "log_text", "content")
    """
    Log entry parsed from the log text.
    This entry should be complete, meaning that we can resume the original log from this entry.
    At this stage we store the raw text in the field "log_text".
    """

    def __init__(self, log_id, log_type, file_offset, file_length):
        # key_info: only these vars dumped to files
        self.id = log_id
        self.log_type = log_type  # deprecated
        # offset and length is used to locate this entry in original log file
        self.log_file_offset = file_offset
        # space occupation of this log entry in the file
        self.log_text_length = file_length
        self.timestamp_us = 0
        self.parse_succ = False
        # trace 是指那些带有[T]的日志
        self.is_trace = False
        # self.datetime = None
        self.log_level = None
        self.component = None
        self.source_func = None
        self.source_file = None
        self.source_line = None
        self.th_id = None
        self.co_id = None
        self.trace_id = None
        self.lt = None
        self.dc = None
        # content: parse from log text just in time if used
        self.log_text = None
        self.content = None

    # 根据方括号解析日志头，拆分日志头和日志内容部分。
    # 一般来说根据括号位置可以确定所有元素，如果log打的是用模板且raw text部分没有方括号的话
    def parse_from_data(self, data, time_slice):
        self.log_text = data
        in_brac_elems = []
        bare_elems = []
        idx = 1
        cur_brac = "["
        next_brac = "]"
        st = time.time()
        # 分析日志头的[]部分，最多8个[]，其他[]全放在bare里面
        while idx != -1:
            next_idx = data.find(next_brac, idx)
            if next_idx == -1:
                bare_elems.append(data[idx : len(data)].strip())
                break
            if cur_brac == "[":
                in_brac_elems.append(data[idx:next_idx].strip())
            else:
                if len(in_brac_elems) >= 8:
                    bare_elems.append(data[idx:])
                    break
                else:
                    bare_elem = data[idx:next_idx].strip()
                    if bare_elem != '':
                        bare_elems.append(bare_elem)
            idx = next_idx + 1
            cur_brac, next_brac = next_brac, cur_brac
        time_slice[0] += time.time() - st
        st = time.time()
        if len(in_brac_elems) < 3 and "(tid:" in data:
            return
        if len(bare_elems[0]) > 6:
            # 处理某些log开头混乱的情况，需要把这部分包含的Level, Component, Source分开
            real_bare_content = bare_elems[0].split(' ')
            self.log_level = real_bare_content[0]
            # no component name and no func name, level is INFO DEBUG
            if len(real_bare_content) == 2:
                bare_elems = real_bare_content + bare_elems[1:]
                in_brac_elems = [in_brac_elems[0], ""] + in_brac_elems[1:]
            # no func(have component), level is INFO DEBUG or no component(have func), level = WARN ERROR FATAL
            elif len(real_bare_content) == 3:
                if self.log_level in LogEntry.WF_LEVELS:
                    # no func but have component
                    bare_elems = [real_bare_content[0], real_bare_content[2]] + bare_elems[1:]
                    in_brac_elems = [in_brac_elems[0], real_bare_content[1]] + in_brac_elems[1:]
                else:
                    # no component but have func
                    bare_elems = [real_bare_content[0], real_bare_content[2]] + bare_elems[1:]
                    in_brac_elems = [in_brac_elems[0], ""] + in_brac_elems[1:]
            # has func and component, level is WARN ERROR FATAL
            elif len(real_bare_content) == 4:
                bare_elems = [real_bare_content[0], real_bare_content[2] + " " + real_bare_content[3]] + bare_elems[1:]
                in_brac_elems = [in_brac_elems[0], real_bare_content[1]] + in_brac_elems[1:]
            else:
                time_slice[1] += time.time() - st
                raise Exception()
        time_slice[1] += time.time() - st
        st = time.time()
        self.timestamp_us = TimeUtils.datetime_to_timestamp(in_brac_elems[0])
        self.log_level = bare_elems[0]
        self.component = in_brac_elems[1]
        location = bare_elems[1].strip()
        if self.log_level in LogEntry.WF_LEVELS:
            func_file_tuple = location.split(" ")
            self.source_func = func_file_tuple[0]
            file_line_str = func_file_tuple[1].strip('()')
        else:
            file_line_str = location
        fl_tup = file_line_str.split(':')
        self.source_file = fl_tup[0]
        self.source_line = int(fl_tup[1])
        time_slice[2] += time.time() - st
        st = time.time()
        th_idx = 2
        # 寻找lt和dc的位置
        dc_idx = len(in_brac_elems) - 1
        while dc_idx >= 0:
            if "dc=" in in_brac_elems[dc_idx]:
                break
            dc_idx -= 1
        content_brac_idx = dc_idx + 1
        content_bare_idx = 2
        if dc_idx == -1:
            # 没有 dc，找lt
            lt_idx = len(in_brac_elems) - 1
            while lt_idx >= 0:
                if "lt=" in in_brac_elems[lt_idx]:
                    break
                lt_idx -= 1
            content_brac_idx = lt_idx + 1
            if lt_idx == -1:
                # 无dc，也无lt， 有可能是[T] trace log
                t_idx = len(in_brac_elems) - 1
                while t_idx >= 0:
                    if in_brac_elems[t_idx] == "T":
                        break
                    t_idx -= 1
                if t_idx == -1:
                    raise Exception('No dc= field, no lt= field and no [T] field. data={0}'.format(data))
                self.is_trace = True
                self.trace_id = in_brac_elems[t_idx - 1]
                if t_idx - 2 == th_idx:
                    # 没有协程号
                    self.th_id = int(in_brac_elems[th_idx])
                else:
                    self.th_id = int(in_brac_elems[th_idx])
                    self.co_id = int(in_brac_elems[th_idx + 1])
                content_brac_idx = t_idx
            else:
                # 无dc 有lt
                self.lt = in_brac_elems[lt_idx]
                self.trace_id = in_brac_elems[lt_idx - 1]
                self.th_id = int(in_brac_elems[th_idx])
                if lt_idx - 2 != th_idx:
                    self.co_id = int(in_brac_elems[lt_idx - 2])

        else:
            # 有dc
            self.dc = in_brac_elems[dc_idx]
            content_brac_idx = dc_idx + 1
            self.lt = in_brac_elems[dc_idx - 1]
            self.trace_id = in_brac_elems[dc_idx - 2]
            if dc_idx - 3 == th_idx:
                # 没有协程号
                self.th_id = in_brac_elems[th_idx]
            else:
                self.co_id = in_brac_elems[dc_idx - 3]
                self.th_id = in_brac_elems[th_idx]

        time_slice[3] += time.time() - st
        st = time.time()
        # 日志内容解析
        # 将非头部分提取为content
        content_offset = data.rfind(bare_elems[content_bare_idx][:10])
        if content_brac_idx < len(in_brac_elems):
            # -1 是因为brac的内容要带上左中括号本身
            new_offset = data.rfind(in_brac_elems[content_brac_idx][:10]) - 1
            if new_offset < content_offset:
                content_offset = new_offset
        self.content = data[content_offset:]
        # 格式转换
        self.th_id = int(self.th_id)
        self.co_id = int(self.co_id) if self.co_id is not None else None
        # self.lt = int(self.lt.split("=")[1]) if self.lt is not None else None
        # self.dc = int(self.dc.split("=")[1]) if self.dc is not None else None
        time_slice[4] += time.time() - st
        self.parse_succ = True
        return

    @staticmethod
    def generate_log_entries_from_string_reader(reader_io, parse_time_slice, max_read_n=-1, start_offset=0, start_log_id=0):
        """
        parse log entries from start point that user specified.
        A posix read-like stream reader interface.
        :param reader_io: the TextIO object
        :param parse_time_slice: xxx
        :param max_read_n: how many entries should be parsed. -1 means all.
        :param start_offset: the offset start point of reader_io
        :param start_log_id: the start log_id of entries
        :return: success_parsed_entries, failed_entries, n_read, next_start_offset
        """
        reader_io.seek(start_offset)
        log_id = start_log_id
        success_log_entries = []
        irregular_logs = []
        log_entry_begin_offset = reader_io.tell()
        line_offset = reader_io.tell()
        log_entry_texts = []
        line_idx = 0
        n_read = 0
        if max_read_n == -1:
            max_read_n = (1 << 64) - 1
        while n_read < max_read_n:
            line = reader_io.readline()
            if line == '':
                if len(log_entry_texts) == 0:
                    # 最后一个且没有文本内容
                    break
                # make up the last record
                log_entry_text = "\n".join(log_entry_texts)
                try:
                    log_entry = LogEntry(log_id, None, log_entry_begin_offset, line_offset - log_entry_begin_offset)
                    log_entry.parse_from_data(log_entry_text, parse_time_slice)
                    success_log_entries.append(log_entry)
                    n_read += 1
                    log_entry_begin_offset = line_offset
                except Exception as e:
                    irregular_logs.append(log_entry_text)
                break
            if len(line) > 28 and line[0] == '[' and line[5] == "-" and line[8] == "-":
                # if current at the first line, this does not mean the end of a log entry.
                # So we need to wait for another line that marks the begin of another log entry.
                if line_idx != 0:
                    log_entry_text = "\n".join(log_entry_texts)
                    st = time.time()
                    try:
                        log_entry = LogEntry(log_id, None, log_entry_begin_offset, line_offset - log_entry_begin_offset)
                        log_entry.parse_from_data(log_entry_text, parse_time_slice)
                        success_log_entries.append(log_entry)
                        log_id += 1
                        n_read += 1
                    except Exception as e:
                        irregular_logs.append(log_entry_text)
                    parse_time_slice[5] += time.time() - st
                    log_entry_begin_offset = line_offset
                log_entry_texts = [line]
            else:
                log_entry_texts.append(line)
            # indeed the offset of the line in the next loop in the file
            line_offset = reader_io.tell()
            line_idx += 1
        return success_log_entries, irregular_logs, n_read, log_entry_begin_offset
