#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
@time: 2025/01/08
@file: obproxy.py
@desc: OBProxy log gathering
"""

import os

from src.handler.gather.gather_log.base import BaseGatherLogOnNode


class ObproxyGatherLogOnNode(BaseGatherLogOnNode):
    """OBProxy log gathering handler"""

    TARGET_NAME = "obproxy"
    LOG_SCOPES = {
        "obproxy": {"key": "*obproxy*"},
        "obproxy_diagnosis": {"key": "*obproxy_diagnosis*"},
        "obproxy_digest": {"key": "*obproxy_digest*"},
        "obproxy_stat": {"key": "*obproxy_stat*"},
        "obproxy_slow": {"key": "*obproxy_slow*"},
        "obproxy_limit": {"key": "*obproxy_limit*"},
    }

    def _get_log_path(self) -> str:
        """Get OBProxy log path"""
        return os.path.join(self.node.get("home_path"), "log")

    # _get_logfile_names_by_time is inherited from BaseGatherLogOnNode
    # Same timestamp format as Observer
