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

"""OceanSeek 日志采集、原修改时间和共用入口兼容性测试。"""

import datetime
import gzip
import os
from pathlib import Path
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

import yaml

from src.common.config import ConfigManager
from src.common.core import ObdiagHome
from src.common.diag_cmd import ObdiagGatherLogCommand
from src.common.result_type import ObdiagResult
from src.common.ssh_client.local_client import LocalClient
from src.handler.analyzer.analyze_log import AnalyzeLogHandler
from src.handler.gather.gather_component_log import GatherComponentLogHandler
from src.handler.gather.gather_log.observer import ObserverGatherLogOnNode
from src.handler.gather.plugins.redact import Redact
from src.handler.gather.step.base import Base
from src.handler.rca.plugins.gather import Gather_log


class TestOceanSeekLogCollection(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='obdiag-oceanseek-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.log_dir = self.root / 'log'
        self.log_dir.mkdir()
        self.node = {'ip': '127.0.0.1', 'home_path': str(self.root), 'ssh_type': 'local', 'host_type': 'OBSERVER'}
        self.context = SimpleNamespace(
            stdio=Mock(),
            options=SimpleNamespace(),
            inner_config={},
            cluster_config={'ob_startup_mode': 'Shared-Storage', 'servers': [self.node]},
            get_variable=lambda name, default=None: str(self.root) if name == 'store_dir' else default,
        )

    def collector(self, **options):
        collector = GatherComponentLogHandler()
        collector.init(self.context, target='observer', store_dir=str(self.root), is_scene=True, **options)
        return collector

    def node_handler(self, **options):
        config = {
            'scope': ObserverGatherLogOnNode.get_log_scopes('Shared-Storage'),
            'from_time': '2026-11-17 16:50:00',
            'to_time': '2026-11-17 16:55:00',
            **options,
        }
        handler = ObserverGatherLogOnNode(self.context, self.node, config)
        handler.ssh_client = LocalClient(self.context, self.node)
        return handler

    def test_yaml_and_command_config_preserve_mode_and_existing_fields(self):
        raw = {'obcluster': {'db_host': '192.0.2.1', 'db_port': 2881, 'servers': {'nodes': [self.node]}}}
        path = self.root / 'config.yml'
        for mode in (None, 'normal', 'NORMAL', 'Shared-Storage', 'sHaReD-sToRaGe'):
            with self.subTest(mode=mode):
                if mode is None:
                    raw['obcluster'].pop('ob_startup_mode', None)
                else:
                    raw['obcluster']['ob_startup_mode'] = mode
                path.write_text(yaml.safe_dump(raw))
                manager = ConfigManager(str(path), self.context.stdio)
                config = manager.get_ob_cluster_config
                self.assertEqual(config['ob_startup_mode'], mode or 'normal')
                self.assertEqual(config['db_host'], '192.0.2.1')
                self.assertEqual(config['db_port'], 2881)
                self.assertEqual(config['servers'][0]['home_path'], str(self.root))
        manager = ConfigManager(stdio=self.context.stdio, config_env_list=['obcluster.ob_startup_mode=SHARED-STORAGE'])
        self.assertIn('libpalf', self.collector(ob_startup_mode=manager.get_ob_cluster_config['ob_startup_mode']).scope)

    def test_mode_specific_scopes_are_case_insensitive_and_do_not_leak(self):
        for mode in ('Shared-Storage', 'SHARED-STORAGE', 'shared-storage', ' sHaReD-sToRaGe '):
            with self.subTest(mode=mode):
                collector = self.collector(ob_startup_mode=mode, scope='all')
                self.assertTrue(collector.result.is_success())
                self.assertEqual(set(collector.scope), {'observer', 'rootservice', 'election', 'alert', 'libpalf'})
                self.assertEqual(set(self.collector(ob_startup_mode=mode, scope='libpalf').scope), {'libpalf'})
        for mode in ('normal', 'NORMAL', ' Normal '):
            with self.subTest(mode=mode):
                self.assertNotIn('libpalf', self.collector(ob_startup_mode=mode).scope)
                self.assertFalse(self.collector(ob_startup_mode=mode, scope='libpalf').result.is_success())
        self.assertNotIn('libpalf', self.collector().scope)
        self.assertNotIn('libpalf', ObserverGatherLogOnNode.LOG_SCOPES)

    def test_invalid_mode_is_rejected_by_log_collection(self):
        for mode in ('', 'shared_storage', 'unknown', None, 1, True, [], {}):
            with self.subTest(mode=mode):
                collector = self.collector(ob_startup_mode=mode)
                self.assertFalse(collector.result.is_success())
                self.assertIn('obcluster.ob_startup_mode', collector.result.error_data)

    def test_gather_log_entry_passes_the_mode_and_enables_mtime_preservation(self):
        home = object.__new__(ObdiagHome)
        home.config_manager = Mock()
        home.stdio = self.context.stdio
        home.context = self.context
        self.context.set_variable = Mock()
        home.set_context_stdio = Mock()
        home.update_obcluster_nodes = Mock()
        home.set_context = Mock()
        with patch('src.common.core.GatherComponentLogHandler') as handler_class:
            home.gather_function('gather_log', None)
        passed = handler_class.return_value.init.call_args.kwargs
        self.assertEqual(passed['ob_startup_mode'], 'Shared-Storage')
        self.assertTrue(passed['preserve_mtime'])

    def test_shared_storage_files_are_discovered_without_changing_normal_scopes(self):
        names = ['observer.log', 'libpalf.log', 'libpalf.log.20261117165611', 'libpalf.log.20261117164000.gz', 'obproxy.log', 'unrelated.log']
        for name in names:
            (self.log_dir / name).write_text('')
        handler = self.node_handler(recent_count=10)
        self.assertEqual(set(handler._find_logs_name()), set(names[:4]))
        handler = self.node_handler(scope=ObserverGatherLogOnNode.LOG_SCOPES, recent_count=10)
        self.assertEqual(handler._find_logs_name(), ['observer.log'])

    def test_absent_libpalf_does_not_prevent_other_logs_from_being_collected(self):
        (self.log_dir / 'observer.log').write_text('')
        self.assertEqual(self.node_handler(recent_count=1)._find_logs_name(), ['observer.log'])

    def test_libpalf_rotation_timestamps_and_types(self):
        handler = self.node_handler()
        for suffix in ('', '.gz', '.zst'):
            with self.subTest(suffix=suffix):
                name = 'libpalf.log.20261117165611' + suffix
                self.assertEqual(handler._parse_timestamp_from_filename(name), datetime.datetime(2026, 11, 17, 16, 56, 11))
                self.assertEqual(handler._get_log_type(name), 'libpalf.log')
        self.assertEqual(handler._parse_timestamp_from_filename('observer.log.20261117165611123'), datetime.datetime(2026, 11, 17, 16, 56, 11, 123000))
        self.assertIsNone(handler._parse_timestamp_from_filename('libpalf.log.20261317165611'))
        self.assertIsNone(handler._parse_timestamp_from_filename('observer.log.20261117165611'))

    def test_time_range_selects_overlapping_libpalf_rotations(self):
        names = ['libpalf.log.20261117164000', 'libpalf.log.20261117165611.gz', 'libpalf.log.20261117171000', 'libpalf.log']
        logs = [(name, str(self.log_dir)) for name in names]
        self.assertEqual(self.node_handler()._filter_logs_by_time(logs), [logs[1]])

    def test_recent_count_groups_compressed_and_uncompressed_libpalf_rotations(self):
        names = ['libpalf.log', 'libpalf.log.20261117164000', 'libpalf.log.20261117165611.gz', 'libpalf.log.20261117171000.zst']
        logs = [(name, str(self.log_dir)) for name in names]
        self.assertEqual(self.node_handler(recent_count=2)._filter_logs_by_recent_count(logs), [logs[0], logs[3]])

    def test_archived_logs_keep_source_mtime_with_and_without_filters(self):
        names = ['observer.log', 'libpalf.log.20261117165611', 'libpalf.log.20261117164000.gz']
        original_mtime = 1700000000
        for name in names:
            content = b'ERROR tenant=1001\nINFO tenant=1001\n'
            (self.log_dir / name).write_bytes(gzip.compress(content) if name.endswith('.gz') else content)
            os.utime(self.log_dir / name, (original_mtime, original_mtime))
        for index, patterns in enumerate(([], ['ERROR'], ['not-found'])):
            with self.subTest(patterns=patterns):
                remote_temp = self.root / ('remote-' + str(index))
                output = remote_temp / 'logs'
                output.mkdir(parents=True)
                local_output = self.root / ('download-' + str(index))
                local_output.mkdir()
                handler = self.node_handler(grep=patterns, preserve_mtime=True, tmp_dir=str(remote_temp), store_dir=str(local_output))
                handler._grep_log_to_tmp(names, str(output))
                handler._package_and_download(str(output), 'logs')
                self.assertEqual(handler.get_result()['success'], 'Success')
                with tarfile.open(handler.get_result()['file_path'], 'r:gz') as archive:
                    for name in names:
                        member = archive.getmember('logs/' + name)
                        self.assertEqual(int(member.mtime), original_mtime)
                    if patterns:
                        expected = b'ERROR tenant=1001\n' if patterns == ['ERROR'] else b''
                        self.assertEqual(archive.extractfile('logs/libpalf.log.20261117165611').read(), expected)
                for name in names:
                    self.assertEqual(int((self.log_dir / name).stat().st_mtime), original_mtime)

    def test_redaction_preserves_mtime_when_requested(self):
        source = self.log_dir / 'libpalf.log'
        source.write_text('secret')
        os.utime(source, (1700000000, 1700000000))
        redact = object.__new__(Redact)
        redact.stdio = self.context.stdio
        redact.preserve_mtime = True
        redact.redacts = {'test': SimpleNamespace(redact=lambda content, **kwargs: content.replace('secret', 'redacted'))}
        target = self.root / 'redacted' / 'libpalf.log'
        semaphore = Mock()
        redact.redact_file(str(source), str(target), semaphore)
        self.assertEqual(target.read_text(), 'redacted')
        self.assertEqual(int(target.stat().st_mtime), 1700000000)
        semaphore.release.assert_called_once()

    def test_analyze_scene_and_rca_keep_their_scopes_in_shared_storage_mode(self):
        for entry in ('analyze', 'scene', 'rca'):
            with self.subTest(entry=entry):
                collector = GatherComponentLogHandler()
                collector.handle = Mock(return_value=ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='stop after initialization'))
                collector.open_all_file = Mock(return_value={})
                if entry == 'analyze':
                    with patch('src.handler.analyzer.analyze_log.GatherComponentLogHandler', return_value=collector):
                        AnalyzeLogHandler(self.context)._AnalyzeLogHandler__handle_with_gather(str(self.root))
                elif entry == 'scene':
                    with patch('src.handler.gather.step.base.GatherComponentLogHandler', return_value=collector):
                        Base(self.context, {'type': 'log'}, self.node, self.context.cluster_config, str(self.root)).execute()
                else:
                    with patch('src.handler.rca.plugins.gather.GatherComponentLogHandler', return_value=collector):
                        Gather_log(self.context).execute()
                self.assertTrue(collector.result.is_success())
                collector.handle.assert_called_once()
                self.assertNotIn('libpalf', collector.scope)
                self.assertFalse(collector.gather_log_conf_dict['preserve_mtime'])

    def test_help_describes_configuration_before_collection_and_mtime(self):
        help_text = ObdiagGatherLogCommand().parser.get_option('--scope').help
        for text in ('obcluster.ob_startup_mode', 'Shared-Storage', 'case-insensitive', 'libpalf', 'home_path/log', 'original modification time'):
            self.assertIn(text, help_text)
        usage = Path('plugins/agent/skills/obdiag-usage/SKILL.md').read_text()
        self.assertIn('ob_startup_mode: Shared-Storage', usage)
        self.assertIn('scope="libpalf"', usage)


if __name__ == '__main__':
    unittest.main()
