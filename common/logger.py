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
@file: logger.py
@desc:
"""
import logging
import os
import sys
import uuid
from logging.handlers import TimedRotatingFileHandler

from common.constant import const
from utils.file_utils import mkdir_if_not_exist
from utils.yaml_utils import read_yaml_data

if getattr(sys, 'frozen', False):
    absPath = os.path.dirname(os.path.abspath(sys.executable))
else:
    absPath = os.path.dirname(os.path.abspath(__file__))
INNER_CONF_FILE = os.path.join(absPath, "conf/inner_config.yml")

class Logger(object):
    def __init__(self, log_config_dict):
        self.logger = logging.getLogger()
        try:
            self.logger.setLevel(log_config_dict["obdiag"]["logger"]["log_level"].upper())
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/config.yml".format(e))
        log_dir = os.path.expanduser(log_config_dict["obdiag"]["logger"]["log_dir"])
        mkdir_if_not_exist(log_dir)
        log_filename = log_config_dict["obdiag"]["logger"]["log_filename"]
        log_format = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        file_format = logging.Formatter(
            '[%%(asctime)s] [%%(levelname)s] [%s] [%%(filename)s->line:%%(lineno)d] %%(message)s' % uuid.uuid3(
                uuid.NAMESPACE_DNS, str(os.getpid())))
        self.file_handler = TimedRotatingFileHandler(os.path.join(log_dir, log_filename),
                                                     when='D', interval=1, backupCount=30, encoding='utf-8')
        self.file_handler.setFormatter(file_format)
        try:
            self.file_handler.setLevel(log_config_dict["obdiag"]["logger"]["file_handler_log_level"].upper())
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/config.yml".format(e))
        self.stdout_handler = logging.StreamHandler(sys.stdout)
        try:
            self.stdout_handler.setLevel(log_config_dict["obdiag"]["logger"]["stdout_handler_log_level"].upper())
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/config.yml".format(e))
        self.stdout_handler.setFormatter(log_format)
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.stdout_handler)

    def get_logger(self):
        return self.logger


inner_config = const.OBDIAG_BASE_DEFAULT_CONFIG
if os.path.exists(INNER_CONF_FILE):
    inner_config = read_yaml_data(INNER_CONF_FILE)
logger = Logger(inner_config).get_logger()

if __name__ == "__main__":
    logger.debug("tests debug")
    logger.info("tests 1")
    logger.error("test2")
    logger.warning("tests 3")
