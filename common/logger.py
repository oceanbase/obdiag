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

from common.obdiag_exception import OBDIAGConfNotFoundException
from utils.file_utils import mkdir_if_not_exist
from utils.yaml_utils import read_yaml_data

tried_conf_path = []
CONF_FILE = os.environ.get("LOG_CONF", os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                                                    "conf/config.yml"))
if not os.path.exists(CONF_FILE):
    tried_conf_path.append(CONF_FILE)
    LOG_CONF_FILE = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), CONF_FILE)
    if not os.path.exists(LOG_CONF_FILE):
        tried_conf_path.append(LOG_CONF_FILE)
        raise OBDIAGConfNotFoundException("Log Conf file not found:\n{0}".format("\n".join(tried_conf_path)))


class Logger(object):
    def __init__(self, log_config_dict):
        self.logger = logging.getLogger()
        try:
            self.logger.setLevel(log_config_dict["OBDIAG"]["LOGGER"]["log_level"].upper())
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/config.yml".format(e))
        log_dir = os.path.abspath(log_config_dict["OBDIAG"]["LOGGER"]["log_dir"])
        mkdir_if_not_exist(log_dir)
        log_filename = log_config_dict["OBDIAG"]["LOGGER"]["log_filename"]
        log_format = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        file_format = logging.Formatter(
            '[%%(asctime)s] [%%(levelname)s] [%s] [%%(filename)s->line:%%(lineno)d] %%(message)s' % uuid.uuid3(
                uuid.NAMESPACE_DNS, str(os.getpid())))
        self.file_handler = TimedRotatingFileHandler(os.path.join(log_dir, log_filename),
                                                     when='D', interval=1, backupCount=30, encoding='utf-8')
        self.file_handler.setFormatter(file_format)
        try:
            self.file_handler.setLevel(log_config_dict["OBDIAG"]["LOGGER"]["file_handler_log_level"].upper())
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/config.yml".format(e))
        self.stdout_handler = logging.StreamHandler(sys.stdout)
        try:
            self.stdout_handler.setLevel(log_config_dict["OBDIAG"]["LOGGER"]["stdout_handler_log_level"].upper())
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/config.yml".format(e))
        self.stdout_handler.setFormatter(log_format)
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.stdout_handler)

    def get_logger(self):
        return self.logger


logger = Logger(read_yaml_data(CONF_FILE)).get_logger()

if __name__ == "__main__":
    logger.debug("tests debug")
    logger.info("tests 1")
    logger.error("test2")
    logger.warning("tests 3")
