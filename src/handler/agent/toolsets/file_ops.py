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
import subprocess
from typing import Literal, Optional

from pydantic_ai import FunctionToolset, RunContext

from src.handler.agent.models import AgentDependencies
from src.handler.agent.tool_output_limits import DEFAULT_TOOL_OUTPUT_CHARS, truncate_for_agent

file_toolset: FunctionToolset[AgentDependencies] = FunctionToolset()

# Directories the agent is allowed to read/write.  Paths under any of these
# prefixes are permitted; everything else is blocked.  The list is intentionally
# broad (home dir + /tmp) so normal diagnostic workflows are not impaired, while
# protecting system-critical paths (/etc, /proc, /sys, /boot, …).
_ALLOWED_PREFIXES: tuple[str, ...] = (
    os.path.expanduser("~"),
    "/tmp",
    "/var/tmp",
    # Also allow cwd at import time so relative paths in test / CI work.
    os.path.abspath("."),
)

# Always-blocked prefixes that must never be accessible even when they fall
# inside an allowed prefix (e.g. ~/.ssh).
_BLOCKED_PREFIXES: tuple[str, ...] = (
    os.path.expanduser("~/.ssh"),
    os.path.expanduser("~/.gnupg"),
)


def _check_path_allowed(abs_path: str, *, write: bool = False) -> Optional[str]:
    """Return an error string if abs_path is not allowed, else None."""
    for blocked in _BLOCKED_PREFIXES:
        if abs_path == blocked or abs_path.startswith(blocked + os.sep):
            return f"Access denied: path is in a protected directory ({blocked})"
    for allowed in _ALLOWED_PREFIXES:
        if abs_path == allowed or abs_path.startswith(allowed + os.sep):
            return None  # permitted
    action = "write to" if write else "read"
    return f"Access denied: cannot {action} '{abs_path}'. " f"Allowed locations: home directory, /tmp, and /var/tmp."


MAX_FILE_SIZE = 10 * 1024 * 1024  # 10 MB
# Files at or below this size are read fully into memory (still subject to max_output_chars).
SMALL_FILE_BYTES = 64 * 1024
# For larger text files, stream only this many lines unless max_lines is set.
DEFAULT_LARGE_FILE_MAX_LINES = 500


def _read_line_window(
    abs_path: str,
    encoding: str,
    start_line: int,
    max_lines: int,
) -> tuple[str, int, int, bool]:
    """
    Read up to max_lines lines starting at start_line (1-based). Returns
    (joined content, first_line_no, last_line_no, has_more_after_window).
    """
    skip = max(0, start_line - 1)
    buf: list[str] = []
    first_no = start_line
    last_no = start_line - 1
    with open(abs_path, "r", encoding=encoding) as f:
        idx = 0
        for line in f:
            if idx < skip:
                idx += 1
                continue
            buf.append(line.rstrip("\n\r"))
            last_no = idx + 1
            idx += 1
            if len(buf) >= max_lines:
                break
        has_more = False
        if len(buf) >= max_lines:
            has_more = next(f, None) is not None
    return "\n".join(buf), first_no, last_no, has_more


@file_toolset.tool
def file_read(
    ctx: RunContext[AgentDependencies],
    file_path: str,
    encoding: str = "utf-8",
    start_line: int = 1,
    max_lines: Optional[int] = None,
    max_output_chars: int = DEFAULT_TOOL_OUTPUT_CHARS,
) -> str:
    """
    Read content from a local text file.

    Small files (on-disk size ≤ 64KB) are read in full. Larger files are **line-sampled** so the
    agent does not load multi-megabyte logs into context: only ``max_lines`` lines from
    ``start_line`` are returned unless you raise ``max_lines``. The full return is also capped by
    ``max_output_chars`` (UTF-8 length of the tool string).

    Args:
        file_path: Path to the file (can be relative or absolute)
        encoding: File encoding (default: utf-8)
        start_line: First line to include (1-based); use for paging large logs
        max_lines: Max lines to return for large files; default 500 when file > 64KB. Ignored when
            the whole file is read (small files).
        max_output_chars: Hard cap on the returned string (default follows tool output budget)

    Returns:
        File content as string, or error message if read fails
    """
    deps = ctx.deps
    try:
        abs_path = os.path.abspath(file_path)

        deny = _check_path_allowed(abs_path)
        if deny:
            return f"Error: {deny}"

        if not os.path.exists(abs_path):
            return f"Error: File not found: {abs_path}"
        if not os.path.isfile(abs_path):
            return f"Error: Path is not a file: {abs_path}"

        file_size = os.path.getsize(abs_path)
        if file_size > MAX_FILE_SIZE:
            return f"Error: File too large ({file_size} bytes). Maximum allowed size is {MAX_FILE_SIZE} bytes."

        meta: list[str] = [
            f"File read successfully: {abs_path}",
            f"File size on disk: {file_size} bytes",
        ]

        if file_size <= SMALL_FILE_BYTES:
            with open(abs_path, "r", encoding=encoding) as f:
                content = f.read()
            meta.append(f"Characters read: {len(content)} (full file; on-disk ≤ {SMALL_FILE_BYTES} bytes)")
            body = content
        else:
            nlines = max_lines if max_lines is not None else DEFAULT_LARGE_FILE_MAX_LINES
            nlines = max(1, int(nlines))
            body, first_no, last_no, has_more = _read_line_window(abs_path, encoding, start_line, nlines)
            if not body:
                meta.append(f"No lines in range starting at line {start_line} " f"(file > {SMALL_FILE_BYTES} bytes; max_lines={nlines}).")
            else:
                meta.append(f"Lines returned: {first_no}–{last_no} (sampled; file > {SMALL_FILE_BYTES} bytes; " f"max_lines={nlines}).")
                if has_more:
                    meta.append(f"More lines exist after line {last_no}. " f"Call again with start_line={last_no + 1} or use run_shell with tail/grep.")

        out = "\n".join(meta) + "\n\nContent:\n" + (body if body else "(empty)")

        if deps.stdio:
            deps.stdio.verbose(f"File read: {abs_path} return_len={len(out)}")

        return truncate_for_agent(out, limit=max_output_chars, label="file_read")

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

        deny = _check_path_allowed(abs_path, write=True)
        if deny:
            return f"Error: {deny}"

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


@file_toolset.tool(requires_approval=True, retries=1)
def run_shell(
    ctx: RunContext[AgentDependencies],
    command: str,
    cwd: Optional[str] = None,
    timeout_sec: int = 180,
) -> str:
    """
    Run a **local** shell command (non-interactive). Requires user approval before execution.

    Typical uses after ``gather_obproxy_log`` / ``gather_log``:
    - Inspect or unpack archives: ``tar -tzf pack.tar.gz | head``, ``tar -xzf pack.tar.gz -C ./out``
    - Zip: ``unzip -l x.zip``, ``unzip -q x.zip -d ./out``
    - Sample large logs: ``head -n 200 path``, ``grep -E 'ERROR|WARN' path | head``

    Prefer ``file_list`` / ``file_read`` for small plain text files. Do not run destructive or unrelated commands.

    Args:
        command: Shell command string (executed with ``shell=True``)
        cwd: Working directory; default is the process current directory
        timeout_sec: Timeout in seconds (default 180)
    """
    deps = ctx.deps
    work = os.path.abspath(cwd) if cwd else None
    if work and not os.path.isdir(work):
        return f"Error: cwd is not a directory: {work}"
    try:
        if deps.stdio:
            deps.stdio.verbose(f"run_shell: {command!r} cwd={work!r}")
        proc = subprocess.run(
            command,
            shell=True,
            cwd=work,
            capture_output=True,
            text=True,
            timeout=max(1, int(timeout_sec)),
            env=os.environ.copy(),
        )
        out_parts = [f"exit_code={proc.returncode}"]
        if proc.stdout:
            out_parts.append(f"stdout:\n{proc.stdout}")
        if proc.stderr:
            out_parts.append(f"stderr:\n{proc.stderr}")
        if not proc.stdout and not proc.stderr:
            out_parts.append("(no stdout/stderr)")
        return truncate_for_agent("\n\n".join(out_parts), label="run_shell")
    except subprocess.TimeoutExpired:
        return f"Error: command timed out after {timeout_sec}s"
    except Exception as e:
        return f"Error: run_shell failed: {e}"
