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
@time: 2024/07/02
@file: test_level.py
@desc:
"""
import unittest
from handler.analyzer.sql.rules.level import Level


class TestLevelEnum(unittest.TestCase):

    def test_enum_creation_and_access(self):
        self.assertEqual(Level.OK.name, 'OK')
        self.assertEqual(Level.OK.value, (1, 'ok'))
        self.assertEqual(Level.CRITICAL.string, 'critical')

    def test_comparison_operators(self):
        self.assertTrue(Level.OK < Level.NOTICE)
        self.assertTrue(Level.NOTICE <= Level.NOTICE)
        self.assertFalse(Level.WARN <= Level.OK)
        self.assertTrue(Level.CRITICAL > Level.WARN)
        self.assertTrue(Level.CRITICAL >= Level.CRITICAL)

    def test_from_string(self):
        self.assertEqual(Level.from_string('ok'), Level.OK)
        self.assertEqual(Level.from_string('warn'), Level.WARN)

        with self.assertRaises(ValueError) as context:
            Level.from_string('error')
        self.assertEqual(str(context.exception), "No such level: error")

    def test_invalid_string(self):
        with self.assertRaises(ValueError) as context:
            Level.from_string('unknown')
        self.assertEqual(str(context.exception), "No such level: unknown")


if __name__ == '__main__':
    unittest.main()
