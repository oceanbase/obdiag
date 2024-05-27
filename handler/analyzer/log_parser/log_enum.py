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
@time: 2023/11/07
@file: log_enum.py
@desc:
"""
import os


class OBLogRoleConst(object):
    OBLOG_ROLE_ROOTSERVICE = "RS"
    OBLOG_ROLE_OBSERVER = "OBS"
    OBLOG_ROLE_SUPPORT_LIST = [OBLOG_ROLE_OBSERVER, OBLOG_ROLE_ROOTSERVICE]


class LogTypeConst(object):
    LOG_TYPE_REGULAR = 1
    LOG_TYPE_WF = 2


class AttrRuleTypeConst(object):
    RULE_TYPE_EQUAL = 1
    RULE_TYPE_RANGE = 2
    VALID_RULE_TYPE = [RULE_TYPE_EQUAL, RULE_TYPE_RANGE]


class AttrRuleBracOrBareConst(object):
    IN_BRAC_ATTR = 1  # 日志中在括号内的attr
    BARE_ATTR = 2  # 日志中在括号外的attr


AttrBracOrBareDict = {
    "log_level": AttrRuleBracOrBareConst.BARE_ATTR,
    "component": AttrRuleBracOrBareConst.IN_BRAC_ATTR,
    "th_id": AttrRuleBracOrBareConst.IN_BRAC_ATTR,
    "co_id": AttrRuleBracOrBareConst.IN_BRAC_ATTR,
    "trace_id": AttrRuleBracOrBareConst.IN_BRAC_ATTR,
}


class AgentRuntimeGlobalVars(object):
    AgentRuntimeDir = os.getcwd()


# dir name in pack
class DirNameInPackConst(object):
    TRIMMED_LOG_DIR_NAME_IN_PACK = "trimmed_log"
    OS_STATE_LOG_DIR_NAME_IN_PACK = "os_state"


class PackCompressConst(object):
    COMPRESS_ZIP = "zip"
    COMPRESS_ZSTD = "zstd"
    CompressSuffixDict = {
        COMPRESS_ZIP: ".zip",
        COMPRESS_ZSTD: ".tar.zst",
    }
    COMPRESS_VALID_LIST = [COMPRESS_ZIP, COMPRESS_ZSTD]
    # 当前使用的压缩类型
    PACK_COMPRESS_TYPE = COMPRESS_ZIP


LOG_LEVEL_LIST = ["DEBUG", "INFO", "WARN", "ERROR", "FATAL", "TRACE"]

LOG_TYPE = ["observer", "election", "rootservice"]
