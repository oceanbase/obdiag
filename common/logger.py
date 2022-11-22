#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/20
@file: logger.py
@desc:
"""
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from common.odg_exception import ODGConfNotFoundException
from utils.file_utils import mkdir_if_not_exist

tried_conf_path = []
LOG_CONF_FILE = os.environ.get("LOG_CONF", os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                                                        "conf/odg_conf.json"))
if not os.path.exists(LOG_CONF_FILE):
    tried_conf_path.append(LOG_CONF_FILE)
    LOG_CONF_FILE = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))), LOG_CONF_FILE)
    if not os.path.exists(LOG_CONF_FILE):
        tried_conf_path.append(LOG_CONF_FILE)
        raise ODGConfNotFoundException("Log Conf file not found:\n{0}".format("\n".join(tried_conf_path)))


class Logger(object):
    def __init__(self, log_config_dict):
        self.logger = logging.getLogger()
        try:
            self.logger.setLevel(log_config_dict["logger_conf"]["log_level"])
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/odg_config.json".format(e))
        log_dir = os.path.abspath(log_config_dict["logger_conf"]["log_dir"])
        mkdir_if_not_exist(log_dir)
        log_filename = log_config_dict["logger_conf"]["log_filename"]
        log_format = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
        self.file_handler = TimedRotatingFileHandler(os.path.join(log_dir, log_filename),
                                                     when='D', interval=1, backupCount=30, encoding='utf-8')
        self.file_handler.setFormatter(log_format)
        try:
            self.file_handler.setLevel(log_config_dict["logger_conf"]["file_handler_log_level"])
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/odg_config.json".format(e))
        self.stdout_handler = logging.StreamHandler(sys.stdout)
        try:
            self.stdout_handler.setLevel(log_config_dict["logger_conf"]["stdout_handler_log_level"])
        except Exception as e:
            raise ValueError("Invalid log level setting, error:{0} only supported set ['DEBUG','INFO','WARN','ERROR'], "
                             "Please modify conf/odg_config.json".format(e))
        self.stdout_handler.setFormatter(log_format)
        self.logger.addHandler(self.file_handler)
        self.logger.addHandler(self.stdout_handler)

    def get_logger(self):
        return self.logger


logger = Logger(json.load(open(LOG_CONF_FILE))).get_logger()

if __name__ == "__main__":
    logger.debug("tests debug")
    logger.info("tests 1")
    logger.error("test2")
    logger.warning("tests 3")
