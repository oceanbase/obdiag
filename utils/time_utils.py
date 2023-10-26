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
@time: 2022/6/21
@file: time_utils.py
@desc:
"""
import time
import traceback
import datetime
from datetime import timedelta

from common.obdiag_exception import OBDIAGFormatException


class Timer(object):
    def __init__(self, timer_name):
        self.timer = time.time()
        self.timer_name = timer_name

    def __enter__(self):
        self.timer = time.time()
        return self.timer

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_timer = time.time()
        # print("{0} consume time {1} s".format(self.timer_name, self.end_timer - self.timer))


def _get_timestamp(year, mon, day, hour, minute, sec):
    mon -= 2
    if 0 >= mon:
        mon += 12
        year -= 1

    return ((((year / 4 - year / 100 + year / 400 + 367 * mon / 12 + day) +
              year * 365 - 719499) * 24 + hour) * 60 + minute) * 60 + sec + -8 * 60 * 60


def get_current_us_timestamp():
    time_second = time.time()
    return int(time_second * 1000000)


def get_invalid_us_timestamp():
    return int(0)


def parse_time_length_to_sec(time_length_str):
    unit = time_length_str[-1]
    value = int(time_length_str[:-1])
    if unit == "m":
        value *= 60
    elif unit == "h":
        value *= 3600
    elif unit == "d":
        value *= 3600 * 24
    else:
        raise OBDIAGFormatException("time length must be format 'n'<m|h|d>")
    return int(value)


def datetime_to_timestamp(datetime_str):
    # yyyy-mm-dd hh:mm:ss.uuuuus or yyyy-mm-dd hh:mm:ss
    try:
        if datetime_str[4] != "-" or datetime_str[7] != "-" or datetime_str[13] != ":" or datetime_str[16] != ":":
            raise OBDIAGFormatException(
                'datetime_str is not valid. datetime_str={0}.'.format(datetime_str))
        yyyy, mm, dd = int(datetime_str[0:4]), int(datetime_str[5:7]), int(datetime_str[8:10])
        hh, mi, ss = int(datetime_str[11:13]), int(datetime_str[14:16]), int(datetime_str[17:19])
        second_timestamp = _get_timestamp(yyyy, mm, dd, hh, mi, ss)
        us = 0
        if len(datetime_str) > 19:
            if datetime_str[19] != ".":
                raise OBDIAGFormatException(
                    'datetime {0} is not valid. datetime_str={1}.'.format(datetime_str, datetime_str))
            us = int(datetime_str[20:26])
        us_timestamp = second_timestamp * 1000000 + int(us)
    except Exception as e:
        traceback.print_exc()
        except_str = traceback.format_exc()
        raise OBDIAGFormatException(except_str + "datetime_str={0}".format(datetime_str))
    return int(us_timestamp)


def trans_datetime_utc_to_local(datetime_str):
    utct_date = datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")  # 2020-12-01 03:21:57
    local_date = utct_date + datetime.timedelta(hours=8)  # 加上时区
    local_date_srt = datetime.datetime.strftime(local_date, "%Y-%m-%d %H:%M:%S")  # 2020-12-01 11:21:57
    trans_res = datetime.datetime.strptime(local_date_srt, "%Y-%m-%d %H:%M:%S")
    return str(trans_res)


def timestamp_to_datetime(date_timestamp):
    second_timestamp, us = int(date_timestamp) // 1000000, int(date_timestamp) % 1000000
    time_obj = time.localtime(int(second_timestamp))
    datetime_str = time.strftime('%Y-%m-%d %H:%M:%S', time_obj)
    datetime_str += '.{0}'.format(us)
    return datetime_str


def timestamp_to_datetime_no_us(date_timestamp):
    second_timestamp = int(date_timestamp) // 1000000
    time_obj = time.localtime(int(second_timestamp))
    datetime_str = time.strftime('%Y-%m-%d %H:%M:%S', time_obj)
    return datetime_str


# transform timestamp(in us) to yyyymmdd hhmmss (filename_time style)
def timestamp_to_filename_time(timestamp):
    second_timestamp = timestamp / 1000000
    time_obj = time.localtime(int(second_timestamp))
    filename_time_str = time.strftime('%Y%m%d%H%M%S', time_obj)
    return filename_time_str


def parse_time_str(arg_time):
    try:
        format_time = datetime.datetime.strptime(arg_time, "%Y-%m-%d %H:%M:%S")
    except Exception as e:
        traceback.print_exc()
        except_str = traceback.format_exc()
        raise OBDIAGFormatException(except_str + "arg_time={0}".format(arg_time))
    return format_time


def filename_time_to_datetime(filename_time):
    """ transform yyyymmddhhmmss to yyyy-mm-dd hh:mm:ss"""
    if filename_time != "":
        return "{0}-{1}-{2} {3}:{4}:{5}".format(filename_time[0:4],
                                                filename_time[4:6],
                                                filename_time[6:8],
                                                filename_time[8:10],
                                                filename_time[10:12],
                                                filename_time[12:14])
    else:
        return ""


def extract_filename_time_from_log_name(log_name):
    """ eg: xxx.20221226231617 """
    log_name_fields = log_name.split(".")
    if bytes.isdigit(log_name_fields[-1].encode("utf-8")) and len(log_name_fields[-1]) >= 14:
        return log_name_fields[-1]
    return ""


def extract_time_from_log_file_text(log_text):
    # 因为 yyyy-mm-dd hh:mm:ss.000000 的格式已经占了27个字符，所以如果传进来的字符串包含时间信息，那长度一定大于27
    if len(log_text) > 27:
        if log_text.startswith("["):
            time_str = log_text[1: log_text.find(']')]
        else:
            time_str = log_text[0: log_text.find(',')]
        time_without_us = time_str[0: time_str.find('.')]
        try:
            format_time = datetime.datetime.strptime(time_without_us, "%Y-%m-%d %H:%M:%S")
            format_time_str = time.strftime("%Y-%m-%d %H:%M:%S", format_time.timetuple())
        except Exception as e:
            format_time_str = ""
    else:
        format_time_str = ""
    return format_time_str


def get_time_rounding(dt, step=0, rounding_level="s"):
    """
    计算整分钟，整小时，整天的时间
    :param step: 往前或往后跳跃取整值，默认为0，即当前所在的时间，正数为往后，负数往前。
                例如：
                step = 0 时 2022-07-26 17:38:21.869993 取整秒后为 2022-07-26 17:38:21
                step = 1 时 2022-07-26 17:38:21.869993 取整秒后为 2022-07-26 17:38:22
                step = -1 时 2022-07-26 17:38:21.869993 取整秒后为 2022-07-26 17:38:20
    :param rounding_level: 字符串格式。
                "s": 按秒取整；"min": 按分钟取整；"hour": 按小时取整；"days": 按天取整
    :return: 处理后的时间
    """
    if rounding_level == "days":
        td = timedelta(days=-step, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute,
                       hours=dt.hour, weeks=0)
        new_dt = dt - td
    elif rounding_level == "hour":
        td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute,
                       hours=-step, weeks=0)
        new_dt = dt - td
    elif rounding_level == "min":
        td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=-step, hours=0,
                       weeks=0)
        new_dt = dt - td
    elif rounding_level == "s":
        td = timedelta(days=0, seconds=-step, microseconds=dt.microsecond, milliseconds=0, minutes=0, hours=0, weeks=0)
        new_dt = dt - td
    else:
        new_dt = dt
    return str(new_dt)
