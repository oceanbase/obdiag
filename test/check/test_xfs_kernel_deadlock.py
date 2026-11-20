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

import unittest

from plugins.check.tasks.observer.system.xfs_kernel_deadlock import (
    has_xfs_mount,
    is_affected_xfs_kernel,
    parse_el7_310_kernel_release,
)


class TestXfsKernelDeadlock(unittest.TestCase):

    def test_parse_el7_310_kernel_release(self):
        self.assertEqual((957, 5, 1), parse_el7_310_kernel_release("3.10.0-957.5.1.el7.x86_64"))
        self.assertEqual((1062,), parse_el7_310_kernel_release("3.10.0-1062.el7.x86_64"))
        self.assertIsNone(parse_el7_310_kernel_release("4.18.0-553.el8.x86_64"))

    def test_has_xfs_mount(self):
        self.assertTrue(has_xfs_mount("/data xfs\n"))
        self.assertTrue(has_xfs_mount("/dev/sda1 on /data type xfs (rw,relatime)\n"))
        self.assertFalse(has_xfs_mount("/dev/sda1 on /data type ext4 (rw,relatime)\n"))

    def test_affected_centos7_xfs_old_kernel(self):
        self.assertTrue(is_affected_xfs_kernel("CentOS Linux release 7.5.1804 (Core)", "3.10.0-957.5.1.el7.x86_64", "/data xfs\n"))

    def test_fixed_kernel_is_not_affected(self):
        self.assertFalse(is_affected_xfs_kernel("CentOS Linux release 7.7.1908 (Core)", "3.10.0-1062.el7.x86_64", "/data xfs\n"))

    def test_non_xfs_is_not_affected(self):
        self.assertFalse(is_affected_xfs_kernel("CentOS Linux release 7.5.1804 (Core)", "3.10.0-957.5.1.el7.x86_64", "/data ext4\n"))

    def test_non_redhat_7_is_not_affected(self):
        self.assertFalse(is_affected_xfs_kernel("Ubuntu 22.04", "3.10.0-957.5.1.el7.x86_64", "/data xfs\n"))


if __name__ == '__main__':
    unittest.main()
