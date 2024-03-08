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
@time: 2024/2/1
@file: print_spin.py
@desc:
"""
import threading
import time


class Spin():
    def __init__(self, info):
        self.run_thread = None
        self.run_tag = True
        self.info = info

    def run(self):
        self.run_thread = threading.Thread(target=self.__run)
        self.run_thread.start()

    def __run(self):
        while self.run_tag:
            print('\r{0}|'.format(self.info), end='')
            time.sleep(0.5)
            print('\r{0}/'.format(self.info), end='')
            time.sleep(0.5)
            print('\r{0}-'.format(self.info), end='')
            time.sleep(0.5)
            print('\r{0}\\'.format(self.info), end='')
            time.sleep(0.5)
        print("\r", end='')

    def stop(self):
        self.run_tag = False
        self.run_thread.join()
