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

"""Shared limits for agent tool return strings so prompts stay within model context."""

# ~12k tokens UTF-8 upper bound; aligns with common coding-agent CLIs that cap tool output
# (bounded return + spill to file for huge shell/read streams).
DEFAULT_TOOL_OUTPUT_CHARS = 48 * 1024


def truncate_for_agent(
    text: str,
    limit: int = DEFAULT_TOOL_OUTPUT_CHARS,
    *,
    label: str = "output",
) -> str:
    if limit <= 0 or len(text) <= limit:
        return text
    head = text[:limit]
    return (
        f"{head}\n\n"
        f"... [{label} truncated: {len(text)} characters total, showing first {limit}] "
        "After gather_log / gather_obproxy_log / gather_oms_log, use file_list on the pack directory from tool output, "
        "then file_read(start_line=…, max_lines=…). Else use run_shell (head/tail/grep) with approval or read files on disk. "
        "中文：采集完成后先到打包输出目录执行 file_list，再用 file_read 分段读取；或经 run_shell 用 grep/head/tail。"
    )
