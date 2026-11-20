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

"""Regression tests for independent OBLogService configuration, log collection, and release versions."""

import datetime
import gzip
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile
from types import SimpleNamespace
import tomllib
import unittest
from unittest.mock import Mock, patch

import yaml

from src.common.config import ConfigManager
from src.common.context import HandlerContext
from src.common.core import ObdiagHome
from src.common.diag_cmd import ObdiagGatherOblogserviceLogCommand
from src.common.ssh_client.local_client import LocalClient
from src.common.ssh_client.ssh import SshClient
from src.handler.agent.toolsets.obdiag import create_obdiag_tools
from src.handler.gather.gather_component_log import GatherComponentLogHandler
from src.handler.gather.gather_log.oblogservice import OblogserviceGatherLogOnNode


REPO = Path(__file__).resolve().parents[2]


def config_manager(data):
    manager = object.__new__(ConfigManager)
    manager.config_data = data
    return manager


class TestOblogserviceConfig(unittest.TestCase):
    def test_merges_global_settings_and_node_overrides(self):
        manager = config_manager(
            {
                'oblogservice': {
                    'oblogservice_cluster_name': 'test',
                    'servers': {'global': {'ssh_username': 'admin', 'ssh_port': 2222, 'home_path': '/data/logservice'}, 'nodes': [{'ip': '192.0.2.1'}, {'ip': '192.0.2.2', 'ssh_username': 'other', 'home_path': '/data/instance2'}]},
                }
            }
        )
        config = manager.get_oblogservice_config
        first, second = config['servers']
        self.assertEqual(config['oblogservice_cluster_name'], 'test')
        self.assertEqual(first['ssh_username'], 'admin')
        self.assertEqual(first['ssh_port'], 2222)
        self.assertEqual(first['home_path'], '/data/logservice')
        self.assertEqual(first['host_type'], 'OBLOGSERVICE')
        self.assertEqual(second['ssh_username'], 'other')
        self.assertEqual(second['home_path'], '/data/instance2')
        self.assertEqual(first['ssh_type'], 'remote')
        self.assertEqual(config_manager({}).get_oblogservice_config['servers'], [])

    def test_obd_component_alias_is_not_an_obdiag_section(self):
        manager = config_manager({'oblogservice-ce': {'servers': {'nodes': [{'ip': '192.0.2.1'}]}}})
        self.assertEqual(manager.get_oblogservice_config['servers'], [])

    def test_rejects_unsafe_paths(self):
        for path in ('', '/tmp/logs;bad', 'relative/path', None):
            with self.subTest(path=path):
                manager = config_manager({'oblogservice': {'servers': {'nodes': [{'home_path': path}]}}})
                with self.assertRaisesRegex(ValueError, r'oblogservice\.servers\.nodes\[0\]\.home_path'):
                    manager.get_oblogservice_config

    def test_retains_container_settings_and_selects_the_existing_clients(self):
        for kind in ('docker', 'kubernetes'):
            with self.subTest(kind=kind):
                raw = {'oblogservice': {'servers': {'global': {'ssh_type': kind, 'container_name': 'logservice', 'namespace': 'test', 'kubernetes_config_file': '/tmp/kubeconfig'}, 'nodes': [{'ip': '192.0.2.1', 'pod_name': 'logservice-0'}]}}}
                node = config_manager(raw).get_oblogservice_config['servers'][0]
                self.assertEqual(node['container_name'], 'logservice')
                self.assertEqual(node['pod_name'], 'logservice-0')
                self.assertEqual(node['kubernetes_config_file'], '/tmp/kubeconfig')
                with patch('src.common.ssh_client.ssh.DockerClient') as docker, patch('src.common.ssh_client.ssh.KubernetesClient') as kube:
                    SshClient(SimpleNamespace(stdio=Mock(), inner_config={}), node)
                    (docker if kind == 'docker' else kube).assert_called_once()
                    (kube if kind == 'docker' else docker).assert_not_called()


class TestOblogserviceCollection(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory(prefix='obdiag-oblogservice-')
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.log_dir = self.root / 'log'
        self.log_dir.mkdir()
        self.node = {'ip': '127.0.0.1', 'home_path': str(self.root), 'ssh_type': 'local'}
        self.context = HandlerContext(stdio=Mock(), inner_config={}, oblogservice_config={'servers': [self.node]}, cluster_config={'servers': [{'ip': '192.0.2.2'}]}, obproxy_config={'servers': [{'ip': '192.0.2.3'}]})

    def collector(self, **kwargs):
        handler = GatherComponentLogHandler()
        handler.init(self.context, target='oblogservice', store_dir=str(self.root), is_scene=True, **kwargs)
        return handler

    def node_handler(self, **kwargs):
        handler = OblogserviceGatherLogOnNode(self.context, self.node, {'scope': OblogserviceGatherLogOnNode.LOG_SCOPES, 'from_time': '2026-11-20 12:30:00', 'to_time': '2026-11-20 12:40:00', **kwargs})
        handler.ssh_client = LocalClient(self.context, self.node)
        return handler

    def test_oblogservice_target_and_scope_are_independent(self):
        for scope in ('all', 'oblogservice'):
            with self.subTest(scope=scope):
                handler = self.collector(scope=scope)
                self.assertTrue(handler.result.is_success())
                self.assertEqual(handler.nodes, [self.node])
                self.assertEqual(set(handler.scope), {'oblogservice'})
        observer = GatherComponentLogHandler()
        observer.init(self.context, target='observer', store_dir=str(self.root), is_scene=True)
        self.assertTrue(observer.result.is_success())
        self.assertEqual(observer.nodes, self.context.cluster_config['servers'])
        self.assertNotIn('oblogservice', observer.scope)
        proxy = GatherComponentLogHandler()
        proxy.init(self.context, target='obproxy', store_dir=str(self.root), is_scene=True)
        self.assertEqual(proxy.nodes, self.context.obproxy_config['servers'])
        self.assertNotIn('oblogservice', proxy.scope)

    def test_missing_nodes_invalid_scope_and_invalid_time_return_errors(self):
        self.context.oblogservice_config = None
        handler = self.collector()
        self.assertFalse(handler.result.is_success())
        self.assertIn('oblogservice.servers.nodes', handler.result.error_data)
        self.context.oblogservice_config = {'servers': [self.node]}
        self.assertFalse(self.collector(scope='observer').result.is_success())
        self.assertFalse(self.collector(from_option='2026-11-20 13:00:00', to_option='2026-11-20 12:00:00').result.is_success())

    def test_timestamp_formats_and_compressed_type_grouping(self):
        handler = self.node_handler()
        for suffix in ('', '.gz', '.zst'):
            with self.subTest(suffix=suffix):
                name = 'oblogservice.log.20261120124559' + suffix
                self.assertEqual(handler._parse_timestamp_from_filename(name), datetime.datetime(2026, 11, 20, 12, 45, 59))
                self.assertEqual(handler._get_log_type(name), 'oblogservice.log')
        self.assertEqual(handler._get_log_type('oblogservice.log.wf.20261120124559.gz'), 'oblogservice.log.wf')
        self.assertIsNone(handler._parse_timestamp_from_filename('oblogservice.log.20261320124559'))
        self.assertIsNone(handler._parse_timestamp_from_filename('oblogservice.log.20261120124559000'))

    def test_time_range_selects_the_overlapping_rotation(self):
        files = ['oblogservice.log.20261120120000', 'oblogservice.log.20261120124559.gz', 'oblogservice.log.20261120130000', 'oblogservice.log']
        self.assertEqual(self.node_handler()._get_logfile_names_by_time('\n'.join(files)), [files[1]])

    def test_current_file_first_timestamp_prevents_restart_gap_selection(self):
        (self.log_dir / 'oblogservice.log').write_text('[2026-11-20 13:00:00.000000] INFO restarted\n')
        self.assertEqual(self.node_handler()._get_logfile_names_by_time('oblogservice.log'), [])

    def test_discovery_and_recent_count_ignore_other_components_and_temporary_files(self):
        files = [
            'oblogservice.log',
            'oblogservice.log.20261120120000',
            'oblogservice.log.20261120124559.gz',
            'oblogservice.log.20261120130000.zst',
            'oblogservice.log.tmp',
            'observer.log',
            'libpalf.log',
            'obproxy.log',
            'oblogservice.log.backup.log',
            'oblogservice.log.tmp.20261120140000',
        ]
        for name in files:
            (self.log_dir / name).write_text('')
        self.assertEqual(set(self.node_handler(recent_count=2)._find_logs_name()), {files[0], files[3]})

    def test_no_matching_log_is_reported_without_an_archive(self):
        handler = self.node_handler(tmp_dir=str(self.root / 'tmp'), store_dir=str(self.root / 'output'))
        with patch('src.handler.gather.gather_log.base.SshClient', return_value=handler.ssh_client):
            handler.handle()
        self.assertEqual(handler.get_result()['success'], 'Fail')
        self.assertEqual(handler.get_result()['info'], 'no log found')
        self.assertFalse(list(self.root.rglob('*.tar.gz')))

    def test_archives_keep_original_mtime_and_filter_only_uncompressed_logs(self):
        names = ['oblogservice.log', 'oblogservice.log.20261120124559', 'oblogservice.log.20261120120000.gz']
        original = b'ERROR tenant=1001\nWARN tenant=1001\nERROR tenant=1002\n'
        for name in names:
            (self.log_dir / name).write_bytes(gzip.compress(original) if name.endswith('.gz') else original)
            os.utime(self.log_dir / name, (1700000000, 1700000000))
        for index, patterns in enumerate(([], ['ERROR', '1001'], ['not-found'])):
            with self.subTest(patterns=patterns):
                output = self.root / ('output' + str(index))
                output.mkdir()
                handler = self.node_handler(grep=patterns, recent_count=10, tmp_dir=str(self.root / ('tmp' + str(index))), store_dir=str(output))
                with patch('src.handler.gather.gather_log.base.SshClient', return_value=handler.ssh_client):
                    handler.handle()
                self.assertEqual(handler.get_result()['success'], 'Success')
                with tarfile.open(handler.get_result()['file_path'], 'r:gz') as archive:
                    members = {Path(item.name).name: item for item in archive.getmembers() if item.isfile() and Path(item.name).name in names}
                    self.assertEqual(set(members), set(names))
                    for name, member in members.items():
                        self.assertEqual(int(member.mtime), 1700000000)
                        if name.endswith('.gz'):
                            self.assertEqual(archive.extractfile(member).read(), (self.log_dir / name).read_bytes())
                        else:
                            expected = original if not patterns else b'ERROR tenant=1001\n' if patterns == ['ERROR', '1001'] else b''
                            self.assertEqual(archive.extractfile(member).read(), expected)
                for name in names:
                    self.assertEqual(int((self.log_dir / name).stat().st_mtime), 1700000000)

    def test_same_host_instances_have_distinct_installation_identifiers(self):
        first = self.node_handler()._build_tmp_dir_name('from', 'to').rsplit('_', 1)[1]
        other = OblogserviceGatherLogOnNode(self.context, {**self.node, 'home_path': str(self.root / 'instance2')}, {})
        other.ssh_client = LocalClient(self.context, self.node)
        second = other._build_tmp_dir_name('from', 'to').rsplit('_', 1)[1]
        self.assertNotEqual(first, second)

    def test_core_command_needs_no_database_config_or_connection(self):
        home = object.__new__(ObdiagHome)
        home.config_manager = config_manager({'oblogservice': {'servers': {'nodes': [self.node]}}})
        home.options = SimpleNamespace()
        home.cmds = ['gather', 'oblogservice_log']
        home.stdio = self.context.stdio
        home.inner_config_manager = SimpleNamespace(config={})
        home.update_obcluster_nodes = Mock()
        with patch('src.common.core.GatherComponentLogHandler') as handler_class, patch('src.common.core.telemetry.set_cluster_conn') as connect:
            home.gather_oblogservice_log(None)
        connect.assert_not_called()
        home.update_obcluster_nodes.assert_not_called()
        self.assertEqual(handler_class.return_value.init.call_args.kwargs['target'], 'oblogservice')
        self.assertEqual(home.context.oblogservice_config['servers'][0]['home_path'], str(self.root))
        self.assertIsNone(home.context.cluster_config['db_host'])

    def test_cli_help_routing_completion_and_agent_tool(self):
        command = ObdiagGatherOblogserviceLogCommand()
        options, _ = command.parser.parse_args(['--scope', 'oblogservice', '--recent_count', '2', '--grep', 'ERROR'])
        self.assertEqual(options.scope, 'oblogservice')
        self.assertEqual(options.recent_count, 2)
        self.assertEqual(options.grep, ['ERROR'])
        self.assertIn('No obcluster section or SQL credentials required', command.parser.get_option('-c').help)
        self.assertIn('original modification time', command.parser.get_option('--store_dir').help)
        home = Mock()
        command._do_command(home)
        home.gather_oblogservice_log.assert_called_once()
        shell = 'source "$1"; COMP_WORDS=(obdiag gather oblog); COMP_CWORD=2; _obdiag_completion; printf "%s\\n" "${COMPREPLY[@]}"'
        completion = subprocess.run(['bash', '-c', shell, 'bash', str(REPO / 'rpm/init_obdiag_cmd.sh')], check=True, capture_output=True, text=True)
        self.assertIn('oblogservice_log', completion.stdout)
        path = self.root / 'component.yml'
        path.write_text(yaml.safe_dump({'oblogservice': {'servers': {'nodes': [self.node]}}}))
        tool = next(item for item in create_obdiag_tools(lambda: str(path), self.context.stdio) if item.__name__ == 'gather_oblogservice_log')
        with patch('src.handler.agent.toolsets.obdiag.execute_obdiag_command', return_value={'success': True, 'stdout': 'ok', 'stderr': ''}) as execute:
            tool(scope='oblogservice', grep='ERROR', recent_count=2, temp_dir='/tmp/work')
        self.assertEqual(execute.call_args.args[0], 'gather_oblogservice_log')
        self.assertEqual(execute.call_args.args[1]['grep'], ['ERROR'])
        self.assertEqual(execute.call_args.args[1]['recent_count'], 2)

    def test_release_version_and_build_template_match(self):
        version = tomllib.loads((REPO / 'pyproject.toml').read_text())['project']['version']
        self.assertEqual(version, '5.0.3')
        info = subprocess.run(['make', '-s', 'info'], cwd=REPO, check=True, capture_output=True, text=True)
        self.assertIn('OBDIAG_VERSION: ' + version, info.stdout)
        template = (REPO / 'src/common/version.py').read_text()
        self.assertIn("OBDIAG_VERSION = '<VERSION>'", template)
        namespace = {}
        exec(compile(template.replace('<VERSION>', version), 'version.py', 'exec'), namespace)
        self.assertIn('OceanBase Diagnostic Tool: 5.0.3', namespace['get_obdiag_version']())


if __name__ == '__main__':
    unittest.main()
