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
@time: 2024/5/20
@file: analyze_sql_review.py
@desc:
"""
import html
import json
import os
import time
import sqlparse
from tabulate import tabulate
from src.common.constant import const
from src.common.tool import StringUtils, Util
from src.common.tool import TimeUtils
from src.common.tool import FileUtil
from src.common.tool import DirectoryUtil
from src.common.ob_connector import OBConnector
from src.handler.analyzer.sql.rule_manager import SQLReviewRuleManager
from src.handler.analyzer.sql.rules.level import Level
from src.handler.meta.html_meta import GlobalHtmlMeta
from src.common.result_type import ObdiagResult


class AnalyzeSQLReviewHandler(object):
    def __init__(self, context):
        super(AnalyzeSQLReviewHandler, self).__init__()
        self.context = context
        self.stdio = context.stdio
        self.from_time_str = None
        self.to_time_str = None
        self.config_path = const.DEFAULT_CONFIG_PATH
        self.analyze_files_list = None
        self.directly_analyze_files = False
        self.level = 'notice'
        self.local_store_path = None
        self.local_stored_parrent_path = os.path.abspath('.')
        self.output_type = 'html'
        self.tenant_db_host = None
        self.tenant_db_port = None

    def init_inner_config(self):
        self.stdio.print("init inner config start")
        self.inner_config = self.context.inner_config
        self.stdio.verbose('inner config: {0}'.format(self.inner_config))
        basic_config = self.inner_config['obdiag']['basic']
        self.config_path = basic_config['config_path']
        self.stdio.print("init inner config complete")
        return True

    def init_config(self):
        self.stdio.print('init cluster config start')
        ob_cluster = self.context.cluster_config
        self.stdio.verbose('cluster config: {0}'.format(StringUtils.mask_passwords(ob_cluster)))
        self.ob_cluster = ob_cluster
        self.sys_connector = OBConnector(context=self.context, ip=ob_cluster.get("db_host"), port=ob_cluster.get("db_port"), username=ob_cluster.get("tenant_sys").get("user"), password=ob_cluster.get("tenant_sys").get("password"), timeout=100)
        self.ob_cluster_name = ob_cluster.get("ob_cluster_name")
        self.stdio.print('init cluster config complete')
        return True

    def init_db_connector(self):
        if self.db_user:
            self.stdio.verbose("init db connector start")
            self.db_connector_provided = True
            host = self.tenant_db_host or self.ob_cluster.get("db_host")
            port = self.tenant_db_port if self.tenant_db_port is not None else self.ob_cluster.get("db_port")
            pwd = self.db_password if self.db_password is not None else ''
            self.db_connector = OBConnector(context=self.context, ip=host, port=port, username=self.db_user, password=pwd, timeout=100)
            self.stdio.print("DB connection: using --user (business tenant) at {0}:{1}, user={2}. (password not logged)".format(host, port, self.db_user))
            self.stdio.verbose("init db connector complete")
        else:
            self.db_connector = self.sys_connector
            tenant_sys = self.ob_cluster.get("tenant_sys") or {}
            summary = StringUtils.mask_passwords(
                {
                    "db_host": self.ob_cluster.get("db_host"),
                    "db_port": self.ob_cluster.get("db_port"),
                    "sys_user": tenant_sys.get("user"),
                    "password": tenant_sys.get("password"),
                }
            )
            self.stdio.print("DB connection: no --user; defaulting to sys tenant from obcluster (host={0}, port={1}, user={2}; config password redacted).".format(summary.get("db_host"), summary.get("db_port"), summary.get("sys_user")))

    def init_option(self):
        self.stdio.print('init option start')
        options = self.context.options
        try:
            self.stdio.verbose('options:[{0}]'.format(json.dumps(StringUtils.mask_passwords(dict(vars(options))), ensure_ascii=False, default=str)))
        except Exception:
            self.stdio.verbose('options:[unavailable]')
        files_option = Util.get_option(options, 'files')
        if files_option:
            self.directly_analyze_files = True
            self.analyze_files_list = files_option
        else:
            self.stdio.error("option --files not found, please provide")
            return False
        db_user_option = Util.get_option(options, 'user')
        db_password_option = Util.get_option(options, 'password')
        host_option = Util.get_option(options, 'host')
        port_option = Util.get_option(options, 'port')
        if host_option:
            self.tenant_db_host = host_option.strip()
        if port_option is not None and str(port_option).strip() != '':
            try:
                self.tenant_db_port = int(port_option)
            except (TypeError, ValueError):
                self.stdio.error('Invalid --port: must be an integer')
                return False
        tenant_name_option = Util.get_option(options, 'tenant_name')
        if tenant_name_option is not None:
            self.tenant_name = tenant_name_option
        level_option = Util.get_option(options, 'level')
        if level_option:
            try:
                Level.from_string(level_option.strip().lower())
                self.level = level_option.strip().lower()
            except ValueError:
                self.stdio.error("Invalid --level: use one of critical, warn, notice, ok")
                return False
        store_dir_option = Util.get_option(options, 'store_dir')
        if store_dir_option is not None:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_parrent_path = os.path.abspath(store_dir_option)
        output_option = Util.get_option(options, 'output')
        if output_option:
            self.output_type = output_option.strip().lower()
        if self.output_type not in ('html', 'json'):
            self.stdio.error('Invalid --output: use html or json')
            return False
        self.db_user = db_user_option
        self.db_password = db_password_option
        if (self.tenant_db_host or self.tenant_db_port is not None) and not self.db_user:
            self.stdio.warn('--host/--port only take effect with --user.')
        self.stdio.print('init option complete')
        return True

    def handle(self):
        self.start_time = time.time()
        if not self.init_option():
            self.stdio.error('init option failed')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="init option failed")
        if not self.init_inner_config():
            self.stdio.error('init inner config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init inner config failed")
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init config failed")
        self.init_db_connector()
        ext = "json" if self.output_type == "json" else "html"
        pack_dir_name = "obdiag_sql_review_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp()))
        self.pack_dir = os.path.join(self.local_stored_parrent_path, pack_dir_name)
        DirectoryUtil.mkdir(path=self.pack_dir, stdio=self.stdio)
        self.local_store_path = os.path.join(self.pack_dir, "result.{0}".format(ext))
        self.stdio.print("use {0} as result store path.".format(self.pack_dir))
        all_results = self.__directly_analyze_files()
        if all_results is None:
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="failed to load sql files from --files")
        results = self.__parse_results(all_results)
        if self.output_type == "html":
            html_result = self.__generate_html_result(results)
            FileUtil.write_append(self.local_store_path, html_result)
        elif self.output_type == "json":
            json_result = self.__generate_json_result(results)
            FileUtil.write_append(self.local_store_path, json_result)
        else:
            self.stdio.error('Unsupported output type: {0}. Use --output html or json.'.format(self.output_type))
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='Unsupported output type: {0}'.format(self.output_type))
        self.__print_result()
        return ObdiagResult(
            ObdiagResult.SUCCESS_CODE,
            data={"store_dir": self.pack_dir, "result_file": os.path.abspath(self.local_store_path)},
        )

    def __directly_analyze_files(self):
        sql_files = self.__get_sql_file_list()
        if len(sql_files) == 0:
            self.stdio.error("failed to find SQL files from the --files option provided")
            return None
        file_results = {}
        for file in sql_files:
            sql_results = {}
            sql_list = self.__parse_sql_file(file)
            for sql in sql_list:
                rules = SQLReviewRuleManager()
                result = rules.manager.analyze_sql_statement(sql, self.stdio, self.level)
                sql_results[sql] = result
            file_results[file] = sql_results
        return file_results

    def __get_sql_file_list(self):
        """
        :param:
        :return: sql_files
        """
        sql_files = []
        if self.analyze_files_list and len(self.analyze_files_list) > 0:
            for path in self.analyze_files_list:
                if os.path.exists(path):
                    if os.path.isfile(path):
                        sql_files.append(path)
                    else:
                        sql_file_list = FileUtil.find_all_file(path)
                        if len(sql_file_list) > 0:
                            sql_files.extend(sql_file_list)
            self.stdio.print("files to be processed: {0}".format(sql_files))
        return sql_files

    def __parse_sql_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8', errors='replace') as file:
            sql_content = file.read()
        statements = sqlparse.split(sql_content)
        sql_list = []
        for stmt in statements:
            s = stmt.strip()
            if not s:
                continue
            if s.startswith('--'):
                continue
            if s.startswith('/*') and s.endswith('*/'):
                continue
            sql_list.append(stmt)
        return sql_list

    def __parse_results(self, results):
        reports = []
        for file_name, file_results in results.items():
            diagnostic_entries = []
            for sql, sql_results in file_results.items():
                diagnostics = []
                for sql_result in sql_results:
                    diagnostic = {"ruleClassName": sql_result.class_name, "ruleName": sql_result.rule_name, "ruleDescription": sql_result.description, "ruleLevel": sql_result.level.value, "suggestion": sql_result.suggestion}
                    diagnostics.append(diagnostic)
                diagnostic_entry = {"sqlText": sql, "diagnostics": diagnostics}
                diagnostic_entries.append(diagnostic_entry)
            report = {"command": "obdiag analyze sql_review", "options": {"files": file_name}, "diagnosticEntries": diagnostic_entries}
            reports.append(report)
        return reports

    def __generate_json_result(self, results):
        """
        Generate JSON output following industry schema (Salesforce Code Analyzer / SQLFluff style).
        Schema: runDir, command, options, violationCounts, reports
        """
        run_dir = os.getcwd()
        total = critical = warn = notice = ok = 0
        reports_json = []
        for report in results:
            entries = []
            for entry in report["diagnosticEntries"]:
                diags = []
                for d in entry["diagnostics"]:
                    level_str = d["ruleLevel"][1] if isinstance(d["ruleLevel"], (list, tuple)) else str(d["ruleLevel"])
                    if level_str == "critical":
                        critical += 1
                    elif level_str == "warn":
                        warn += 1
                    elif level_str == "notice":
                        notice += 1
                    else:
                        ok += 1
                    total += 1
                    diags.append(
                        {
                            "rule": d["ruleName"],
                            "severity": level_str,
                            "message": d["ruleDescription"],
                            "suggestion": d["suggestion"],
                        }
                    )
                entries.append({"sqlText": entry["sqlText"], "violations": diags})
            reports_json.append(
                {
                    "file": report["options"]["files"],
                    "diagnosticEntries": entries,
                }
            )
        output = {
            "runDir": run_dir,
            "command": "obdiag analyze sql_review",
            "options": {"files": [r["options"]["files"] for r in results]},
            "violationCounts": {"total": total, "critical": critical, "warn": warn, "notice": notice, "ok": ok},
            "reports": reports_json,
        }
        return json.dumps(output, ensure_ascii=False, indent=2)

    def __generate_html_table(self, sql_entry):
        diagnostics = sql_entry["diagnostics"]
        sql_text = html.escape(sql_entry["sqlText"])
        if not diagnostics:
            return "<tr><td>{0}</td><td colspan='4'><i>No rule results (SQL may be invalid or unparsable)</i></td></tr>".format(sql_text)
        rows = []
        current_row = [f"<td rowspan={len(diagnostics)}>{sql_text}</td>"]

        for idx, diag in enumerate(diagnostics):
            lvl_raw = diag['ruleLevel'][1] if isinstance(diag['ruleLevel'], (list, tuple)) else str(diag['ruleLevel'])
            lvl = html.escape(lvl_raw)
            cn = html.escape(str(diag['ruleClassName']))
            rd = html.escape(str(diag['ruleDescription']))
            sg = html.escape(str(diag['suggestion']))
            if idx == 0:
                row = current_row + [f"<td>{cn}</td>", f"<td>{rd}</td>", f"<td class='{lvl}'>{lvl}</td>", f"<td>{sg}</td>"]
                rows.append("<tr>" + "".join(row) + "</tr>")
            else:
                rows.append("<tr class='merge'>" + f"<td>{cn}</td>" + f"<td>{rd}</td>" + f"<td class='{lvl}'>{lvl}</td>" + f"<td>{sg}</td>" + "</tr>")
        return "".join(rows)

    def __generate_html_result(self, all_results):
        full_html = ""
        for data in all_results:
            # print(data)
            diagnostic_entries = data["diagnosticEntries"]
            sql_entries_html = "".join([self.__generate_html_table(entry) for entry in diagnostic_entries])
            full_html += (
                GlobalHtmlMeta().get_value(key="sql_review_html_head_template")
                + f"""
            <p>Command: {html.escape(data["command"])}</p>
            <p>Files: {html.escape(str(data["options"]["files"]))}</p>
            <h3>Diagnostic results</h3>
            <table>
                <thead>
                    <tr>
                        <th>SQL text</th>
                        <th>Rule</th>
                        <th>Description</th>
                        <th>Level</th>
                        <th>Suggestion</th>
                    </tr>
                </thead>
                <tbody>
                    {sql_entries_html}
                </tbody>
            </table>
            """
            )
        full_html += GlobalHtmlMeta().get_value(key="html_footer_temple")
        return full_html

    def __print_result(self):
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        data = [["Status", "Result Details", "Time"], ["Completed", self.pack_dir, f"{elapsed_time:.2f} s"]]
        table = tabulate(data, headers="firstrow", tablefmt="grid")
        self.stdio.print("\nAnalyze SQL Review Summary:")
        self.stdio.print(table)
        self.stdio.print("\n")
