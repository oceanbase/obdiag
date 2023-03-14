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
@time: 2020/7/20
@file: retry_utils.py
@desc:
"""

import time
from common.logger import logger


def retry(retry_count=3, retry_interval=2):
    """
    retry decorator
    Example:
    @retry(3, 2) or @retry()
    def test():
       pass
    """

    def real_decorator(decor_method):
        def wrapper(*args, **kwargs):
            for count in range(retry_count):
                try:
                    return_values = decor_method(*args, **kwargs)
                    return return_values
                except Exception as error:
                    logger.error("Function execution %s retry: %s " %
                                 (decor_method.__name__, count + 1))
                    time.sleep(retry_interval)
                    if count == retry_count - 1:
                        raise error

        return wrapper

    return real_decorator
