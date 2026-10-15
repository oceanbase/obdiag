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

import os
from pathlib import Path
import re
import shlex
import subprocess
import tempfile
import unittest


SPEC_PATH = Path(__file__).resolve().parents[2] / 'rpm' / 'oceanbase-diagnostic-tool.spec'
# 已发布旧包的卸载脚本不会随新包修复，必须保留其原始行为来验证首次升级。
LEGACY_PREUN = 'rm -f /usr/bin/obdiag 2>/dev/null || true\nrm -f /bin/obdiag 2>/dev/null || true\n'


class TestRpmLifecycle(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix='obdiag-rpm-')
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name).resolve()
        self.install_dir = self.root / 'opt/oceanbase-diagnostic-tool'
        self.install_dir.mkdir(parents=True)
        self.command_link = self.root / 'usr/bin/obdiag'
        self.command_link.parent.mkdir(parents=True)
        (self.root / 'bin').mkdir()
        (self.root / 'etc/profile.d').mkdir(parents=True)
        self.binary = self.install_dir / 'obdiag'
        self.binary.write_text('#!/bin/sh\necho "OceanBase Diagnostic Tool: 5.0.0"\n')
        for name in ('init.sh', 'obdiag_backup.sh'):
            (self.install_dir / name).write_text('#!/bin/sh\nexit 0\n')
        (self.install_dir / 'init_obdiag_cmd.sh').write_text('# 补全脚本测试桩\n')
        self.fake_bin = self.root / 'test-bin'
        self.fake_bin.mkdir()
        # 仅替代需要 root 的属主设置，其余命令在隔离目录中实际执行。
        chown = self.fake_bin / 'chown'
        chown.write_text('#!/bin/sh\nexit 0\n')
        chown.chmod(0o755)

    def scriptlet(self, name):
        match = re.search(r'^%' + name + r'\n(.*?)(?=^%[a-z]|\Z)', SPEC_PATH.read_text(), re.MULTILINE | re.DOTALL)
        self.assertIsNotNone(match, 'Missing RPM scriptlet: %' + name)
        return match.group(1)

    def run_script(self, script, count):
        # 一次替换所有安装路径，避免 /bin 与 /usr/bin 重叠而改写真实系统目录。
        script = re.sub(r'/opt/oceanbase-diagnostic-tool|/usr/bin/obdiag|/bin/obdiag|/etc/profile\.d/obdiag\.sh', lambda match: shlex.quote(str(self.root) + match.group()), script)
        result = subprocess.run(['/bin/sh', '-c', script, 'rpm-scriptlet', str(count)], env={**os.environ, 'PATH': str(self.fake_bin) + os.pathsep + os.environ['PATH']}, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return result.stdout

    def run_scriptlet(self, name, count):
        return self.run_script(self.scriptlet(name), count)

    def assert_command_works(self):
        self.assertTrue(self.command_link.is_symlink())
        self.assertEqual(self.command_link.resolve(), self.binary)
        result = subprocess.run([str(self.command_link), '--version'], capture_output=True, text=True, check=True)
        self.assertIn('OceanBase Diagnostic Tool: 5.0.0', result.stdout)

    def test_fresh_install(self):
        self.run_scriptlet('post', 1)
        self.run_scriptlet('posttrans', 1)
        self.assert_command_works()
        self.assertTrue((self.root / 'etc/profile.d/obdiag.sh').is_file())

    def test_converted_package_install_without_posttrans(self):
        self.run_scriptlet('post', 'configure')
        self.assert_command_works()

    def check_legacy_upgrade(self):
        self.command_link.symlink_to(self.binary)
        self.run_scriptlet('post', 2)
        self.run_script(LEGACY_PREUN, 1)
        self.assertFalse(self.command_link.is_symlink())
        self.run_scriptlet('posttrans', 2)
        self.assert_command_works()

    def test_upgrade_from_legacy_package(self):
        self.check_legacy_upgrade()

    def test_upgrade_from_legacy_package_with_merged_usr(self):
        (self.root / 'bin').rmdir()
        (self.root / 'bin').symlink_to('usr/bin')
        self.check_legacy_upgrade()

    def test_upgrade_preserves_command_while_packages_remain(self):
        self.run_scriptlet('post', 2)
        self.run_scriptlet('posttrans', 2)
        for count in (1, 2, 3, 'upgrade'):
            with self.subTest(count=count):
                self.run_scriptlet('preun', count)
                self.assert_command_works()
        self.run_scriptlet('posttrans', 2)
        self.assert_command_works()

    def test_final_uninstall_removes_both_command_links(self):
        self.command_link.symlink_to(self.binary)
        legacy_link = self.root / 'bin/obdiag'
        legacy_link.symlink_to(self.binary)
        self.run_scriptlet('preun', 0)
        self.assertFalse(self.command_link.is_symlink())
        self.assertFalse(legacy_link.is_symlink())

    def test_final_uninstall_tolerates_missing_links(self):
        self.run_scriptlet('preun', 0)
        self.run_scriptlet('preun', 0)

    def test_converted_package_final_uninstall(self):
        self.run_scriptlet('post', 'configure')
        self.run_scriptlet('preun', 'remove')
        self.assertFalse(self.command_link.is_symlink())

    def test_final_uninstall_with_merged_usr(self):
        (self.root / 'bin').rmdir()
        (self.root / 'bin').symlink_to('usr/bin')
        self.command_link.symlink_to(self.binary)
        self.run_scriptlet('preun', 0)
        self.assertFalse(self.command_link.is_symlink())

    def test_posttrans_replaces_stale_link_and_is_repeatable(self):
        self.command_link.symlink_to(self.root / 'missing-obdiag')
        self.run_scriptlet('post', 1)
        self.run_scriptlet('posttrans', 1)
        self.run_scriptlet('posttrans', 1)
        self.assert_command_works()

    def test_installation_guidance_follows_transaction_cleanup(self):
        output = self.run_scriptlet('post', 1)
        self.assertIn('source ', output)
        self.assertNotIn('obdiag --version', output)
        output += self.run_scriptlet('posttrans', 1)
        self.assertLess(output.index('source '), output.index('obdiag --version'))
        self.assert_command_works()


if __name__ == '__main__':
    unittest.main()
