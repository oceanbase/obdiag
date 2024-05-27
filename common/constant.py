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
@time: 2022/8/2
@file: constant.py
@desc:
"""
import os


class _const:
    class ConstError(TypeError):
        pass

    class ConstCaseError(ConstError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("can't change const %s" % name)
        if not name.isupper():
            raise self.ConstCaseError('const name "%s" is not all uppercase' % name)
        self.__dict__[name] = value


const = _const()

const.OB_INSTALL_DIR_DEFAULT = "/root/observer"
const.OB_LOG_DIR_DEFAULT = "/root/observer/log"
const.OBPROXY_INSTALL_DIR_DEFAULT = "/root/obproxy"
const.OBPROXY_LOG_DIR_DEFAULT = "/root/obproxy/log"

const.GATHER_LOG_TEMPORARY_DIR_DEFAULT = "/tmp"

const.OBSTACK2_DEFAULT_INSTALL_PATH = '/tmp/obstack_x86_64_7'
const.OBSTACK2_LOCAL_STORED_PATH = "./dependencies/bin/obstack_x86_64_7"

# 限制收集任务的并发线程数量 10
const.GATHER_THREADS_LIMIT = 10
# 限制收集任务的并发线程单个线程的执行超时时间 15分钟
const.GATHER_THREAD_TIMEOUT = 15 * 60
# obstack2收集堆栈信息支持的最小版本
const.MIN_OB_VERSION_SUPPORT_GATHER_OBSTACK = "2.0.0"

const.MAX_OB_VERSION_SUPPORT_GATHER_OBADMIN = "4.0.0"

const.DEFAULT_CONFIG_PATH = os.path.join(os.path.expanduser('~'), ".obdiag/config.yml")

const.FLT_TRACE_TREE_MAX_RECURSION = 5
const.FLT_TRACE_TREE_TOP_LEAF = 5
const.FLT_TRACE_WORKER = 4
const.FLT_TRACE_OUTPUT = 50

const.OBDIAG_BASE_DEFAULT_CONFIG = {
    "obdiag": {
        "basic": {"config_path": "~/.obdiag/config.yml", "config_backup_dir": "~/.obdiag/backup_conf", "file_number_limit": 20, "file_size_limit": "2G"},
        "logger": {"file_handler_log_level": "DEBUG", "log_dir": "~/.obdiag/log", "log_filename": "obdiag.log", "log_level": "INFO", "mode": "obdiag", "stdout_handler_log_level": "INFO"},
    }
}

const.OBDIAG_CHECK_DEFAULT_CONFIG = {"check": {"ignore_version": "false", "report": {"report_path": "./check_report/", "export_type": "table"}, "package_file": "~/.obdiag/check/check_package.yaml", "tasks_base_path": "~/.obdiag/check/tasks/"}}

const.OBDIAG_GATHER_DEFAULT_CONFIG = {"gather": {"cases_base_path": "~/.obdiag/gather/tasks"}}

const.OBDIAG_RCA_DEFAULT_CONFIG = {
    "rca": {
        "result_path": "./rca/",
    }
}
const.OBDIAG_TELEMETRY_FILE_NAME = os.path.expanduser("~/.obdiag/.obdiag_telemetry.txt")
const.TELEMETRY_CONTENT_REPORTER = "obdiag"
const.TELEMETRY_URL = "openwebapi.oceanbase.com"
const.TELEMETRY_PATH = "/api/web/oceanbase/report"
const.UPDATE_REMOTE_SERVER = 'https://obbusiness-private.oss-cn-shanghai.aliyuncs.com'
const.UPDATE_REMOTE_VERSION_FILE_NAME = 'https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/obdiag/version.yaml'
const.UPDATE_REMOTE_UPDATE_FILE_NAME = 'https://obbusiness-private.oss-cn-shanghai.aliyuncs.com/download-center/opensource/obdiag/data.tar'
const.RCA_WORK_PATH = '~/.obdiag/rca'
