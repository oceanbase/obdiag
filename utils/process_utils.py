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
@time: 2023/11/20
@file: process_utils.py
@desc:
"""

import sys
import threading
from math import ceil


class ProgressBar:
    def __init__(self, total, prefix='process'):
        self.prefix = prefix
        self.total = total
        self.cur = 0
        self.counter = 0
        self.is_end = False
        self.step = 1

    def bar(self):
        self.counter += 1
        if self.counter * 100 / self.step / self.total >= self.cur:
            sys.stdout.write('\r{}: {}%'.format(self.prefix, self.cur * self.step))
            sys.stdout.flush()
            self.cur += ceil(100 / self.step / self.total)
            if self.counter == self.total:
                sys.stdout.write('\r{} {}%\n'.format(self.prefix, 100))
                sys.stdout.flush()


class ProgressBarCallback:
    def __init__(self, prefix='process'):
        self.prefix = prefix
        self.cur = 0
        self.step = 10

    def bar(self, counter, total, msg=''):
        if counter * 100 / self.step / total >= self.cur:
            if counter == total:
                sys.stdout.write('{}{}: {}%\n'.format(self.prefix, msg, 100))
                sys.stdout.flush()
            else:
                sys.stdout.write('{}{}: {}%\n'.format(self.prefix, msg, self.cur * self.step))
                sys.stdout.flush()
                self.cur += ceil(100 / self.step / total)


if __name__ == '__main__':
    import time

    total = 101
    p = ProgressBar(total)
    for i in range(total):
        time.sleep(1 / total)
        p.bar()
    p = ProgressBarCallback()
    for i in range(total):
        time.sleep(1 / total)
        p.bar(i, total - 1)


