#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/20
@file: snowflake.py
@desc:
"""
import threading
import time

strptime_lock = threading.RLock()


class SnowFlake(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(SnowFlake, cls).__new__(cls)
        return cls._inst

    def __init__(self, source_id):
        # thread safe
        with strptime_lock:
            self.start = int(time.mktime(time.strptime('2018-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")))
        self.last = int(time.time())
        self.count_id = 0
        self.source_id = source_id

    def get_id(self):
        # 时间差部分
        now = int(time.time())
        temp = now-self.start
        if len(str(temp)) < 9:
            length = len(str(temp))
            s = "0" * (9-length)
            temp = s + str(temp)
        if now == self.last:
            self.count_id += 1
        else:
            self.count_id = 0
            self.last = now
        # 标识ID部分
        if len(str(self.source_id)) < 2:
            length = len(str(self.source_id))
            s = "0" * (2-length)
            self.source_id = s + str(self.source_id)
        # 自增序列号部分
        if self.count_id == 99999:
            time.sleep(1)
        count_id_date = str(self.count_id)
        if len(count_id_date) < 5:
            length = len(count_id_date)
            s = "0"*(5-length)
            count_id_date = s + count_id_date
        _id = str(temp) + str(self.source_id) + count_id_date
        return _id


def sleep():
    time.sleep(2)
