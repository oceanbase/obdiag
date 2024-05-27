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
from prettytable import from_db_cursor
import pymysql as mysql


class OBConnector(object):
    def __init__(
        self,
        ip,
        port,
        username,
        password=None,
        database=None,
        stdio=None,
        timeout=30,
    ):
        self.ip = str(ip)
        self.port = int(port)
        self.username = str(username)
        self.password = str(password)
        self.timeout = timeout
        self.conn = None
        self.stdio = stdio
        self.database = database
        self.init()

    def init(self):
        try:
            self._connect_db()
        except Exception as e:
            self.stdio.verbose(e)

    def _connect_db(self):
        try:
            self.conn = mysql.connect(
                host=self.ip,
                port=self.port,
                user=self.username,
                passwd=self.password,
                db=self.database,
                connect_timeout=30,
            )
            self.stdio.verbose("connect databse ...")
        except mysql.Error as e:
            self.stdio.error("connect OB: {0}:{1} with user {2} failed, error:{3}".format(self.ip, self.port, self.username, e))
            return
        try:
            ob_trx_timeout = self.timeout * 1000000
            self.execute_sql("SET SESSION ob_trx_timeout={0};".format(ob_trx_timeout))
        except Exception as e:
            self.stdio.warn("set ob_trx_timeout failed, error:{0}".format(e))
        try:
            ob_query_timeout = self.timeout * 1000000
            self.execute_sql("SET SESSION ob_query_timeout={0};".format(ob_query_timeout))
        except Exception as e:
            self.stdio.warn("set ob_query_timeout failed, error:{0}".format(e))

    def execute_sql(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        ret = cursor.fetchall()
        cursor.close()
        return ret

    def execute_sql_return_columns_and_data(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        column_names = [col[0] for col in cursor.description]
        ret = cursor.fetchall()
        cursor.close()
        return column_names, ret

    def execute_sql_return_cursor_dictionary(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor(mysql.cursors.DictCursor)
        cursor.execute(sql)
        return cursor

    def execute_sql_return_cursor(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor

    def execute_sql_pretty(self, sql):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor()
        cursor.execute(sql)
        ret = from_db_cursor(cursor)
        cursor.close()
        return ret

    def callproc(self, procname, args=()):
        if self.conn is None:
            self._connect_db()
        else:
            self.conn.ping(reconnect=True)
        cursor = self.conn.cursor()
        cursor.callproc(procname, args)
        ret = cursor.fetchall()
        return ret
