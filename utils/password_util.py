#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/28
@file: parser_utils.py
@desc:
"""

from random import choice
import string


def gen_password(length=8, chars=string.ascii_letters + string.digits):
    """
    简短地生成随机密码，包括大小写字母、数字，可以指定密码长度
    :param length:
    :param chars:
    :return:
    """
    return ''.join([choice(chars) for i in range(length)])
