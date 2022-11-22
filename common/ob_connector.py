#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/20
@file: ob_connector.py
@desc:
"""
import mysql.connector
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
