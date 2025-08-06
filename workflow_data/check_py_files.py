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
@time: 2025/04/14
@file: check_py_files.py
@desc: for check python files header
"""

import os

EXPECTED_LINES = ["#!/usr/bin/env python", "# -*- coding: UTF-8 -*-", "# Copyright (c) 2022 OceanBase"]


def check_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [f.readline().rstrip('\n') for _ in range(3)]
            return lines == EXPECTED_LINES
    except Exception:
        return False


def main():
    import sys

    target_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    failed = []
    for root, _, files in os.walk(target_dir):
        for file in files:
            if file.endswith('.py'):
                path = os.path.join(root, file)
                if not check_file(path):
                    failed.append(path)

    if failed:
        print(f"❌ The following files do not comply with the specifications. Please add copyright related information：{failed}")
        sys.exit(1)
    else:
        print("✅ all pass！")


if __name__ == "__main__":
    main()
