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
@time: 2022/6/20
@file: ob_connector.py
@desc:
"""
import mysql.connector
from prettytable import from_db_cursor

from common.logger import logger


class OBConnector(object):
    def __init__(self, ip, port, username, password=None, timeout=10):
        self.ip = str(ip)
        self.port = int(port)
        self.username = str(username)
        self.password = str(password)
        self.timeout = timeout
        self.conn = None
        self.init()

    def init(self):
        try:
            self._connect_db()
        except Exception as e:
            logger.exception(e)

    def _connect_db(self):
        logger.debug("connect OB: {0}:{1} with user {2}".format(self.ip, self.port, self.username))
        self.conn = mysql.connector.connect(
            host=self.ip,
            port=self.port,
            user=self.username,
            passwd=self.password,
            connect_timeout=30,
        )
        logger.debug("connect databse ...")

    def execute_sql(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor(buffered=True)
        cursor.execute(sql)
        ret = cursor.fetchall()
        cursor.close()
        return ret

    def execute_sql_return_cursor_dictionary(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor(buffered=True, dictionary=True)
        cursor.execute(sql)
        return cursor

    def execute_sql_return_cursor(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor(buffered=True)
        cursor.execute(sql)
        return cursor

    def execute_sql_pretty(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor(buffered=True)
        cursor.execute(sql)
        ret = from_db_cursor(cursor)
        cursor.close()
        return ret
