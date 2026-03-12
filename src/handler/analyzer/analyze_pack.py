#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
"""
@file: analyze_pack.py
@desc: Analyze obdiag gather pack - discover logs, run analyze log, output unified report.
       REQ-OBD-002: obdiag analyze pack --pack_dir=xxx [--output=json]
"""
import json
import os

from src.common.pack_discovery import discover_log_files
from src.common.tool import Util
from src.handler.analyzer.analyze_log import AnalyzeLogHandler
from src.common.result_type import ObdiagResult


class AnalyzePackHandler(object):
    """Parse obdiag gather pack and produce unified diagnostic report."""

    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio

    def handle(self):
        options = self.context.options
        pack_dir = Util.get_option(options, 'pack_dir') or Util.get_option(options, 'log_dir')
        output_format = (Util.get_option(options, 'output') or 'json').lower()
        store_dir = Util.get_option(options, 'store_dir') or './'

        if not pack_dir or not os.path.isdir(pack_dir):
            self.stdio.error("--pack_dir is required and must be an existing directory")
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="pack_dir required")

        discovered = discover_log_files(pack_dir)
        if not discovered:
            self.stdio.warn("No log files found in pack_dir={0}".format(pack_dir))
            report = {
                "health_score": 100,
                "findings": [],
                "action_plan": ["No errors found in pack. Cluster may be healthy."],
            }
            return self._output(report, output_format)

        setattr(options, 'log_dir', pack_dir)
        setattr(options, 'output', 'json')
        setattr(options, 'store_dir', store_dir)
        setattr(options, 'files', None)

        handler = AnalyzeLogHandler(self.context)
        result = handler.handle()
        if not result.is_success():
            return result

        json_data = result.data.get("json") if result.data else None
        if not json_data:
            self.stdio.warn("analyze log did not return json")
            report = {"health_score": 50, "findings": [], "action_plan": ["Parse failed"]}
            return self._output(report, output_format)

        report = self._build_report(json_data)
        return self._output(report, output_format)

    def _build_report(self, log_json):
        """Build health_score, findings, action_plan from analyze log json."""
        findings = log_json.get("findings", [])
        by_ret_code = log_json.get("by_ret_code", {})
        summary = log_json.get("summary", {})

        total_errors = summary.get("total_errors", 0)
        health_score = max(0, 100 - min(total_errors * 2, 80))

        action_plan = []
        for f in findings:
            if f.get("count", 0) > 0 and f.get("summary"):
                action_plan.append("For {0} (count={1}): {2}".format(
                    f.get("ret_code", ""), f.get("count", 0), f.get("summary", "")))

        if not action_plan and total_errors > 0:
            action_plan.append("Review result_details.txt for full error context and trace_ids.")
        elif not action_plan:
            action_plan.append("No issues detected. Cluster appears healthy.")

        return {
            "health_score": health_score,
            "findings": findings,
            "action_plan": action_plan,
            "by_ret_code": by_ret_code,
            "summary": summary,
        }

    def _output(self, report, output_format):
        if output_format == "json":
            self.stdio.print(json.dumps(report, ensure_ascii=False, indent=2))
        else:
            self.stdio.print("Health Score: {0}/100".format(report.get("health_score", 0)))
            self.stdio.print("\nFindings:")
            for f in report.get("findings", []):
                self.stdio.print("  - {0}: {1} (count={2})".format(
                    f.get("ret_code", ""), f.get("summary", ""), f.get("count", 0)))
            self.stdio.print("\nAction Plan:")
            for a in report.get("action_plan", []):
                self.stdio.print("  - {0}".format(a))
        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=report)
