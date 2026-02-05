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
@file: main.py
@desc:
"""

import sys
import os

# Setup sys.path for packaged environment
# Dynamic plugins use "from src.xxx import yyy", need to find src module
if getattr(sys, 'frozen', False):
    # PyInstaller packaged environment
    _base_path = os.path.dirname(sys.executable)
    _site_packages = os.path.join(_base_path, 'lib', 'site-packages')
    if os.path.exists(_site_packages) and _site_packages not in sys.path:
        sys.path.insert(0, _site_packages)

from src.common.diag_cmd import MainCommand
from src.common.stdio import IO

ROOT_IO = IO(1)


def main():
    """Main entry point for obdiag command."""
    defaultencoding = 'utf-8'
    if sys.getdefaultencoding() != defaultencoding:
        try:
            # Python 2 compatibility (deprecated in Python 3)
            from imp import reload

            reload(sys)
            sys.setdefaultencoding(defaultencoding)
        except (ImportError, AttributeError):
            # Python 3 doesn't support setdefaultencoding
            # UTF-8 is already the default in Python 3
            pass
    ROOT_IO.track_limit += 2
    if MainCommand().init(sys.argv[0], sys.argv[1:]).do_command():
        ROOT_IO.exit(0)
        return 0
    else:
        ROOT_IO.exit(1)
        return 1


if __name__ == '__main__':
    sys.exit(main())
