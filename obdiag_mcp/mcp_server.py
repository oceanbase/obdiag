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

from fastmcp import FastMCP
from pathlib import Path
import importlib
import sys
import subprocess


# 自动检测项目根目录
current_path = Path(__file__).resolve()
root_path = current_path.parent.parent
if str(root_path) not in sys.path:
    sys.path.insert(0, str(root_path))

mcp = FastMCP("OBDiag MCP Server")

# 动态加载tools目录下的所有模块
for tool_file in Path(__file__).parent.glob("tools/*.py"):
    if tool_file.name == "__init__.py":
        continue
    module = importlib.import_module(f"obdiag_mcp.tools.{tool_file.stem}", package="obdiag_mcp")
    if hasattr(module, 'register_tools'):
        module.register_tools(mcp)

# 启动 MCP 服务
if __name__ == "__main__":
    # 检查是否还存在其他的 MCP 实例
    check_old_mcp = subprocess.run("ps -ef | grep obdiag_mcpserver.py | grep -v grep", shell=True, text=True, capture_output=True)
    if check_old_mcp.returncode == 0:
        print("MCP server is already running. Please stop it before starting a new one.")
        exit(1)
    mcp.run(transport="streamable-http", host="0.0.0.0", port=8000, path="/mcp")
