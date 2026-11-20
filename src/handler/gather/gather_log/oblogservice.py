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

"""OBLogService 日志采集，复用公共连接、区间筛选和归档流程。"""

import datetime
import hashlib
import os
import re

from src.common.path_utils import quote_shell_path
from src.handler.gather.gather_log.base import BaseGatherLogOnNode


class OblogserviceGatherLogOnNode(BaseGatherLogOnNode):
    TARGET_NAME = 'oblogservice'
    LOG_SCOPES = {'oblogservice': {'key': 'oblogservice.log*'}}

    def _get_log_path(self):
        return os.path.join(self.node['home_path'], 'log')

    def _build_tmp_dir_name(self, from_ts, to_ts):
        # 同机多实例具有相同的连接名称，用安装目录标识区分各自的采集包。
        instance = hashlib.sha256(self.node['home_path'].encode('utf-8')).hexdigest()[:8]
        return super()._build_tmp_dir_name(from_ts, to_ts) + '_' + instance

    def _parse_timestamp_from_filename(self, file_name):
        # OBLogService 轮转后缀为秒级时间，与 Observer 的 17 位后缀不同。
        match = re.fullmatch(r'oblogservice\.log(?:\.wf)?\.(\d{14})(?:\.(?:gz|zst))?', file_name)
        if match:
            try:
                return datetime.datetime.strptime(match.group(1), '%Y%m%d%H%M%S')
            except ValueError:
                pass
        return None

    def _is_current_log_file(self, file_name):
        return file_name in ('oblogservice.log', 'oblogservice.log.wf')

    def _get_log_type(self, file_name):
        file_name = re.sub(r'\.(gz|zst)$', '', file_name)
        return re.sub(r'\.\d{14}$', '', file_name)

    def _get_all_logfile_names(self, log_files):
        # 最近文件选择也排除无效后缀、临时文件，避免它们被当作当前日志。
        return [name for name in super()._get_all_logfile_names(log_files) if self._is_current_log_file(name) or self._parse_timestamp_from_filename(name) is not None]

    def _grep_log_to_tmp(self, logs_name, tmp_log_dir):
        super()._grep_log_to_tmp(logs_name, tmp_log_dir)
        if self._build_grep_options():
            for name in logs_name:
                if not name.endswith(('.gz', '.zst')):
                    # 过滤会重新生成文件，归档前恢复源日志的修改时间。
                    self.ssh_client.exec_cmd('touch -m -r {0} -- {1}'.format(quote_shell_path(os.path.join(self.log_path, name)), quote_shell_path(os.path.join(tmp_log_dir, name))))
