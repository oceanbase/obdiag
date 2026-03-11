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
@time: 2026/03/09
@file: file_ops.py
@desc: File operation tools for obdiag agent
"""

import os
from typing import Literal

from pydantic_ai import Agent, RunContext

from src.handler.agent.models import AgentDependencies


def register_file_tools(agent: Agent[AgentDependencies, str]):
    """
    Register file operation tools with the agent

    Args:
        agent: The Pydantic-AI agent to register tools with
    """

    @agent.tool
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

            # Check file size to prevent reading very large files
            file_size = os.path.getsize(abs_path)
            max_size = 10 * 1024 * 1024  # 10 MB limit
            if file_size > max_size:
                return f"Error: File too large ({file_size} bytes). Maximum allowed size is {max_size} bytes."

            with open(abs_path, "r", encoding=encoding) as f:
                content = f.read()

            result_text = f"File read successfully: {abs_path}\nFile size: {len(content)} characters\n\nContent:\n{content}"

            if deps.stdio:
                deps.stdio.verbose(f"File read successfully: {abs_path} ({len(content)} characters)")

            return result_text

        except PermissionError as e:
            error_msg = f"Permission denied: {str(e)}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg
        except UnicodeDecodeError as e:
            error_msg = f"Encoding error (try a different encoding): {str(e)}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"File read failed: {str(e)}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg

    @agent.tool
    def file_write(
        ctx: RunContext[AgentDependencies],
        file_path: str,
        content: str,
        mode: Literal["w", "a"] = "w",
        encoding: str = "utf-8",
    ) -> str:
        """
        Create or write to a local file.

        Automatically creates parent directories if they don't exist.

        Args:
            file_path: Path to the file (can be relative or absolute)
            content: Content to write to the file
            mode: File mode - 'w' for write (overwrite), 'a' for append
            encoding: File encoding (default: utf-8)

        Returns:
            Success message with file path and size, or error message if write fails
        """
        deps = ctx.deps

        try:
            abs_path = os.path.abspath(file_path)

            # Create directory if it doesn't exist
            dir_path = os.path.dirname(abs_path)
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
                if deps.stdio:
                    deps.stdio.verbose(f"Created directory: {dir_path}")

            # Write file
            with open(abs_path, mode, encoding=encoding) as f:
                f.write(content)

            file_size = os.path.getsize(abs_path)
            action = "created" if mode == "w" else "appended to"
            success_msg = f"File {action} successfully: {abs_path}\nFile size: {file_size} bytes"

            if deps.stdio:
                deps.stdio.verbose(success_msg)

            return success_msg

        except PermissionError as e:
            error_msg = f"Permission denied: {str(e)}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"File write failed: {str(e)}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg

    @agent.tool
    def file_list(
        ctx: RunContext[AgentDependencies],
        directory_path: str,
        pattern: str = "*",
    ) -> str:
        """
        List files in a directory.

        Args:
            directory_path: Path to the directory
            pattern: Optional glob pattern to filter files (default: *)

        Returns:
            List of files in the directory, or error message if listing fails
        """
        deps = ctx.deps

        try:
            import glob

            abs_path = os.path.abspath(directory_path)

            if not os.path.exists(abs_path):
                return f"Error: Directory not found: {abs_path}"

            if not os.path.isdir(abs_path):
                return f"Error: Path is not a directory: {abs_path}"

            # List files matching pattern
            search_pattern = os.path.join(abs_path, pattern)
            files = glob.glob(search_pattern)

            if not files:
                return f"No files found matching pattern '{pattern}' in {abs_path}"

            # Sort and format output
            files.sort()
            file_info = []
            for f in files:
                basename = os.path.basename(f)
                if os.path.isdir(f):
                    file_info.append(f"  [DIR]  {basename}/")
                else:
                    size = os.path.getsize(f)
                    file_info.append(f"  [FILE] {basename} ({size} bytes)")

            result = f"Directory: {abs_path}\nFound {len(files)} item(s):\n" + "\n".join(file_info)

            if deps.stdio:
                deps.stdio.verbose(f"Listed {len(files)} items in {abs_path}")

            return result

        except PermissionError as e:
            error_msg = f"Permission denied: {str(e)}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg
        except Exception as e:
            error_msg = f"Directory listing failed: {str(e)}"
            if deps.stdio:
                deps.stdio.verbose(error_msg)
            return error_msg
