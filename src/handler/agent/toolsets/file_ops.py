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
@time: 2026/03/10
@file: file_ops.py
@desc: File operation toolset for obdiag agent
"""

import glob as glob_module
import os
from typing import Literal

from pydantic_ai import FunctionToolset, RunContext

from src.handler.agent.models import AgentDependencies

file_toolset: FunctionToolset[AgentDependencies] = FunctionToolset()

MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB


@file_toolset.tool
def file_read(
    ctx: RunContext[AgentDependencies],
    file_path: str,
    encoding: str = "utf-8",
) -> str:
    """
    Read content from a local file.

    Args:
        file_path: Path to the file (can be relative or absolute)
        encoding: File encoding (default: utf-8)

    Returns:
        File content as string, or error message if read fails
    """
    deps = ctx.deps
    try:
        abs_path = os.path.abspath(file_path)

        if not os.path.exists(abs_path):
            return f"Error: File not found: {abs_path}"
        if not os.path.isfile(abs_path):
            return f"Error: Path is not a file: {abs_path}"

        file_size = os.path.getsize(abs_path)
        if file_size > MAX_FILE_SIZE:
            return f"Error: File too large ({file_size} bytes). Maximum allowed size is {MAX_FILE_SIZE} bytes."

        with open(abs_path, "r", encoding=encoding) as f:
            content = f.read()

        if deps.stdio:
            deps.stdio.verbose(f"File read successfully: {abs_path} ({len(content)} characters)")

        return f"File read successfully: {abs_path}\nFile size: {len(content)} characters\n\nContent:\n{content}"

    except PermissionError as e:
        return f"Permission denied: {e}"
    except UnicodeDecodeError as e:
        return f"Encoding error (try a different encoding): {e}"
    except Exception as e:
        return f"File read failed: {e}"


@file_toolset.tool
def file_write(
    ctx: RunContext[AgentDependencies],
    file_path: str,
    content: str,
    mode: Literal["w", "a"] = "w",
    encoding: str = "utf-8",
) -> str:
    """
    Create or write to a local file. Automatically creates parent directories.

    Args:
        file_path: Path to the file (can be relative or absolute)
        content: Content to write to the file
        mode: 'w' for write (overwrite), 'a' for append
        encoding: File encoding (default: utf-8)

    Returns:
        Success message with file path and size, or error message if write fails
    """
    deps = ctx.deps
    try:
        abs_path = os.path.abspath(file_path)
        dir_path = os.path.dirname(abs_path)
        if dir_path and not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)

        with open(abs_path, mode, encoding=encoding) as f:
            f.write(content)

        file_size = os.path.getsize(abs_path)
        action = "created" if mode == "w" else "appended to"
        msg = f"File {action} successfully: {abs_path}\nFile size: {file_size} bytes"

        if deps.stdio:
            deps.stdio.verbose(msg)
        return msg

    except PermissionError as e:
        return f"Permission denied: {e}"
    except Exception as e:
        return f"File write failed: {e}"


@file_toolset.tool
def file_list(
    ctx: RunContext[AgentDependencies],
    directory_path: str,
    pattern: str = "*",
) -> str:
    """
    List files in a directory with sizes. Use directory_path="." for current working directory.

    When the user asks "这些文件有多大" or "文件大小" without specifying a path, use "." (current dir).

    Args:
        directory_path: Path to the directory (use "." for current working directory)
        pattern: Optional glob pattern to filter files (default: *)

    Returns:
        List of files with sizes, or error message if listing fails
    """
    deps = ctx.deps
    try:
        abs_path = os.path.abspath(directory_path)

        if not os.path.exists(abs_path):
            return f"Error: Directory not found: {abs_path}"
        if not os.path.isdir(abs_path):
            return f"Error: Path is not a directory: {abs_path}"

        search_pattern = os.path.join(abs_path, pattern)
        files = sorted(glob_module.glob(search_pattern))

        if not files:
            return f"No files found matching pattern '{pattern}' in {abs_path}"

        file_info = []
        for f in files:
            basename = os.path.basename(f)
            if os.path.isdir(f):
                file_info.append(f"  [DIR]  {basename}/")
            else:
                size = os.path.getsize(f)
                file_info.append(f"  [FILE] {basename} ({size} bytes)")

        if deps.stdio:
            deps.stdio.verbose(f"Listed {len(files)} items in {abs_path}")

        return f"Directory: {abs_path}\nFound {len(files)} item(s):\n" + "\n".join(file_info)

    except PermissionError as e:
        return f"Permission denied: {e}"
    except Exception as e:
        return f"Directory listing failed: {e}"
