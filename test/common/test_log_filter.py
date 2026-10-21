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

"""日志关键字筛选的行为回归测试。"""

import os
from pathlib import Path
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from src.common.diag_cmd import ObdiagAnalyzeLogCommand, ObdiagGatherAllCommand, ObdiagGatherLogCommand, ObdiagGatherObproxyLogCommand, ObdiagGatherOmsLogCommand
from src.common.log_filter import build_grep_command
from src.common.ssh_client.local_client import LocalClient
from src.handler.analyzer.analyze_log import AnalyzeLogHandler
from src.handler.gather.gather_log.obproxy import ObproxyGatherLogOnNode
from src.handler.gather.gather_log.observer import ObserverGatherLogOnNode
from src.handler.gather.gather_log.oms import OmsGatherLogOnNode


class TestLogFilterCommand(unittest.TestCase):
    def setUp(self):
        temp_dir = tempfile.TemporaryDirectory(prefix="obdiag-log-filter-")
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)
        self.source = self.root / "input's log"
        self.target = self.root / "output's log"
        self.source.write_text("ERROR tenant=1001\nWARN tenant=1001\nERROR tenant=1002\n", encoding="utf-8")

    def run_command(self, patterns, append=False, **kwargs):
        command = build_grep_command(str(self.source), str(self.target), patterns, append=append)
        return subprocess.run(["/bin/sh", "-c", command], capture_output=True, text=True, timeout=10, **kwargs)

    def test_multiple_regular_expressions_require_all_matches(self):
        result = self.run_command(["^ERROR", "tenant=100[12]", "1001$"])
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.target.read_text(), "ERROR tenant=1001\n")

    def test_no_matches_produce_empty_output(self):
        result = self.run_command(["not-present"])
        self.assertEqual(result.returncode, 1)
        self.assertEqual(self.target.read_text(), "")

    def test_empty_expression_and_no_expressions_keep_all_lines(self):
        for patterns in ([""], []):
            with self.subTest(patterns=patterns):
                result = self.run_command(patterns)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.target.read_bytes(), self.source.read_bytes())

    def test_append_preserves_existing_output(self):
        self.target.write_text("existing\n")
        result = self.run_command(["^WARN"], append=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.target.read_text(), "existing\nWARN tenant=1001\n")

    def test_patterns_reach_grep_as_one_unchanged_argument(self):
        fake_bin = self.root / "bin"
        fake_bin.mkdir()
        argument_file = self.root / "arguments"
        grep = fake_bin / "grep"
        # 只记录参数并透传输入，避免正则语法影响对参数边界的验证。
        grep.write_text('#!/bin/sh\nprintf "%s\\0" "$@" > "$LOG_FILTER_ARGUMENTS"\ncat\n')
        grep.chmod(0o755)
        env = {**os.environ, "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"], "LOG_FILTER_ARGUMENTS": str(argument_file)}
        patterns = ["single'quote", 'double"quote', "$variable", "$(expression)", "`expression`", "semi;colon", "amp&ersand", "left|right", "less<greater>", "#hash", "-option", "", "first\nsecond", r"back\slash", "组合关键字"]
        for pattern in patterns:
            with self.subTest(pattern=pattern):
                result = self.run_command([pattern], env=env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stderr, "")
                self.assertEqual(argument_file.read_bytes().split(b"\0"), [b"-e", pattern.encode("utf-8"), b""])
                self.assertEqual(self.target.read_bytes(), self.source.read_bytes())

    def test_home_relative_paths_use_shell_home(self):
        command = build_grep_command("~/input's log", "~/output's log", ["^WARN"])
        result = subprocess.run(["/bin/sh", "-c", command], env={**os.environ, "HOME": str(self.root)}, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.target.read_text(), "WARN tenant=1001\n")

    def test_leading_dash_in_source_path_is_not_an_option(self):
        source = self.root / "-input"
        source.write_text("sample\n")
        command = build_grep_command(source.name, str(self.target), ["sample"])
        result = subprocess.run(["/bin/sh", "-c", command], cwd=self.root, capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.target.read_text(), "sample\n")


class TestLogFilterHandlers(unittest.TestCase):
    GATHER_HANDLERS = (ObproxyGatherLogOnNode, ObserverGatherLogOnNode, OmsGatherLogOnNode)

    def setUp(self):
        temp_dir = tempfile.TemporaryDirectory(prefix="obdiag-log-handlers-")
        self.addCleanup(temp_dir.cleanup)
        self.root = Path(temp_dir.name)
        self.source_dir = self.root / "source's logs"
        self.source_dir.mkdir()
        self.output_dir = self.root / "output's logs"
        self.output_dir.mkdir()
        self.context = SimpleNamespace(stdio=Mock(), inner_config={})
        self.client = LocalClient(self.context, {"ssh_type": "local"})

    def gather(self, handler_class, patterns, filename="observer's log", content="ERROR owner's tenant=1001\nWARN owner's tenant=1001\nERROR other tenant=1001\n"):
        source = self.source_dir / filename
        source.write_text(content, encoding="utf-8")
        node = {"home_path": str(self.root), "log_path": str(self.source_dir)}
        handler = handler_class(self.context, node, {"grep": patterns})
        handler.log_path = str(self.source_dir)
        handler._log_path_mapping = {filename: str(self.source_dir)}
        handler.ssh_client = self.client
        handler._grep_log_to_tmp([filename], str(self.output_dir))
        return self.output_dir / filename

    def test_all_gather_handlers_apply_each_keyword(self):
        for handler_class in self.GATHER_HANDLERS:
            with self.subTest(handler=handler_class.__name__):
                target = self.gather(handler_class, ["owner's", "^ERROR"])
                self.assertEqual(target.read_text(), "ERROR owner's tenant=1001\n")

    def test_all_gather_handlers_copy_without_keywords(self):
        for handler_class in self.GATHER_HANDLERS:
            for patterns in (None, []):
                with self.subTest(handler=handler_class.__name__, patterns=patterns):
                    target = self.gather(handler_class, patterns)
                    self.assertEqual(target.read_bytes(), (self.source_dir / target.name).read_bytes())

    def test_all_gather_handlers_copy_compressed_files_without_filtering(self):
        for handler_class in self.GATHER_HANDLERS:
            for suffix in (".gz", ".zst"):
                with self.subTest(handler=handler_class.__name__, suffix=suffix):
                    target = self.gather(handler_class, ["not-present"], filename="observer's log" + suffix)
                    self.assertEqual(target.read_bytes(), (self.source_dir / target.name).read_bytes())

    def test_mapped_subdirectory_is_used_for_observer_and_oms(self):
        for handler_class in (ObserverGatherLogOnNode, OmsGatherLogOnNode):
            with self.subTest(handler=handler_class.__name__):
                handler = handler_class(self.context, {"home_path": str(self.root), "log_path": "/unused"}, {"grep": ["^ERROR"]})
                source = self.source_dir / "mapped.log"
                source.write_text("ERROR mapped\nWARN mapped\n")
                handler._log_path_mapping = {source.name: str(self.source_dir)}
                handler.ssh_client = self.client
                handler._grep_log_to_tmp([source.name], str(self.output_dir))
                self.assertEqual((self.output_dir / source.name).read_text(), "ERROR mapped\n")

    def test_offline_analysis_uses_parsed_keywords_and_preserves_append(self):
        command = ObdiagAnalyzeLogCommand()
        source = self.source_dir / "observer's log"
        source.write_text("ERROR owner's tenant=1001\nWARN owner's tenant=1001\nERROR other\n")
        options, _ = command.parser.parse_args(["--files", str(source), "--grep", "owner's", "--grep", "^ERROR"])
        self.context.options = options
        handler = AnalyzeLogHandler(self.context)
        self.assertTrue(handler.init_option())
        self.assertTrue(handler.directly_analyze_files)
        target = self.output_dir / str(source).strip(".").replace("/", "_")
        target.write_text("existing\n")
        handler._AnalyzeLogHandler__pharse_offline_log_file(str(source), str(self.output_dir))
        self.assertEqual(target.read_text(), "existing\nERROR owner's tenant=1001\n")

    def test_offline_analysis_without_keywords_copies_file(self):
        handler = AnalyzeLogHandler(self.context)
        source = self.source_dir / "observer's log"
        source.write_text("ERROR sample\nWARN sample\n")
        handler._AnalyzeLogHandler__pharse_offline_log_file(str(source), str(self.output_dir))
        target = self.output_dir / str(source).strip(".").replace("/", "_")
        self.assertEqual(target.read_bytes(), source.read_bytes())

    def test_analysis_passes_keywords_to_remote_gathering(self):
        self.context.options = SimpleNamespace(since="30m")
        handler = AnalyzeLogHandler(self.context)
        handler.grep_args = ["owner's", "^ERROR"]
        with patch("src.handler.analyzer.analyze_log.GatherComponentLogHandler") as gather_class:
            gather_class.return_value.handle.return_value.is_success.return_value = False
            handler._AnalyzeLogHandler__handle_with_gather(str(self.output_dir))
        self.assertEqual(gather_class.return_value.init.call_args.kwargs["grep"], handler.grep_args)


class TestLogFilterHelp(unittest.TestCase):
    def test_help_and_option_parser_describe_consistent_filtering(self):
        command_classes = (ObdiagAnalyzeLogCommand, ObdiagGatherAllCommand, ObdiagGatherLogCommand, ObdiagGatherObproxyLogCommand, ObdiagGatherOmsLogCommand)
        for command_class in command_classes:
            with self.subTest(command=command_class.__name__):
                command = command_class()
                option = command.parser.get_option("--grep")
                self.assertIn("grep regular expression", option.help)
                self.assertIn("repeat to require all expressions to match", option.help)
                self.assertIn("one argument", option.help)
                if command_class is not ObdiagAnalyzeLogCommand:
                    self.assertIn("uncompressed", option.help)
                options, _ = command.parser.parse_args(["--grep", "owner's", "--grep", "^ERROR"])
                self.assertEqual(options.grep, ["owner's", "^ERROR"])


if __name__ == "__main__":
    unittest.main()
