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
@time: 2024/5/20
@file: analyze_sql_review.py
@desc:
"""
import os
import sqlparse
import json
from colorama import Fore, Style
from common.constant import const
from common.tool import Util
from common.tool import TimeUtils
from common.tool import FileUtil
from common.ob_connector import OBConnector
from handler.analyzer.sql.rule_manager import SQLReviewRuleManager
from handler.meta.sql_meta import GlobalSqlMeta
from handler.meta.html_meta import GlobalHtmlMeta


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

    def init_inner_config(self):
        self.stdio.verbose("init inner config start")
        self.inner_config = self.context.inner_config
        basic_config = self.inner_config['obdiag']['basic']
        self.config_path = basic_config['config_path']
        self.local_stored_parrent_path = self.inner_config['analyze_sql_review']['result_path']
        if not os.path.exists(os.path.abspath(self.local_stored_parrent_path)):
            self.stdio.warn('No such directory {0}, Now create it'.format(os.path.abspath(self.local_stored_parrent_path)))
            os.makedirs(os.path.abspath(self.local_stored_parrent_path))
        self.stdio.verbose("init inner config success")
        return True

    def init_config(self):
        self.stdio.verbose("Init config start")
        ob_cluster = self.context.cluster_config
        self.ob_cluster = ob_cluster
        self.sys_connector = OBConnector(ip=ob_cluster.get("db_host"), port=ob_cluster.get("db_port"), username=ob_cluster.get("tenant_sys").get("user"), password=ob_cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=100)
        self.ob_cluster_name = ob_cluster.get("ob_cluster_name")
        self.stdio.verbose("Init config success")
        return True

    def __init_db_connector(self):
        if self.db_user:
            self.stdio.verbose("Init db connector start")
            self.db_connector_provided = True
            self.db_connector = OBConnector(ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.db_user, password=self.db_password, stdio=self.stdio, timeout=100)
            self.stdio.verbose("Init db connector end")
        else:
            self.db_connector = self.sys_connector

    def init_option(self):
        options = self.context.options
        files_option = Util.get_option(options, 'files')
        if files_option:
            self.directly_analyze_files = True
            self.analyze_files_list = files_option
        db_user_option = Util.get_option(options, 'user')
        db_password_option = Util.get_option(options, 'password')
        tenant_name_option = Util.get_option(options, 'tenant_name')
        if tenant_name_option is not None:
            self.tenant_name = tenant_name_option
        level_option = Util.get_option(options, 'level')
        if level_option:
            self.level = level_option
        self.db_user = db_user_option
        self.db_password = db_password_option
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_inner_config():
            self.stdio.error('init inner config failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        self.__init_db_connector()
        self.local_store_dir = os.path.join(self.local_stored_parrent_path, "sql_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp())))
        if not os.path.exists(os.path.abspath(self.local_store_dir)):
            os.makedirs(os.path.abspath(self.local_store_dir))
        self.stdio.print("Use {0} as result dir.".format(self.local_store_dir))
        if self.directly_analyze_files:
            all_results = self.__directly_analyze_files()
            results = self.__parse_results(all_results)
            html_result = self.__generate_html_result(results)
            FileUtil.write_append(os.path.join(self.local_store_dir, "sql_review_result.html"), html_result)
        else:
            all_results = self.__analyze_sql_audit()
        self.__print_result()

    def __directly_analyze_files(self):
        sql_files = self.__get_sql_file_list()
        if len(sql_files) == 0:
            self.stdio.warn("Failed to find SQL files from the --files option provided")
            return None
        file_results = {}
        sql_results = {}
        for file in sql_files:
            sql_list = self.__parse_sql_file(file)
            for sql in sql_list:
                rules = SQLReviewRuleManager()
                result = rules.manager.analyze_sql_statement(sql, self.stdio, self.level)
                sql_results[sql] = result
            file_results[file] = sql_results
        return file_results

    def __analyze_sql_audit(self):
        return {}

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
            self.stdio.print("The list of SQL files to be processed is as follows: {0}".format(sql_files))
        return sql_files

    def __parse_sql_file(self, file_path):
        with open(file_path, 'r') as file:
            sql_content = file.read()
        statements = sqlparse.split(sql_content)
        sql_list = [stmt for stmt in statements if stmt.strip()]
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

    def __generate_html_table(self, sql_entry):
        diagnostics = sql_entry["diagnostics"]
        sql_text = sql_entry["sqlText"]
        rows = []
        current_row = [f"<td rowspan={len(diagnostics)}>{sql_text}</td>"]

        for idx, diag in enumerate(diagnostics):
            if idx == 0:
                # Start a new row with SQL text having rowspan equal to the number of diagnostics.
                row = current_row + [f"<td>{diag['ruleClassName']}</td>", f"<td>{diag['ruleDescription']}</td>", f"<td class='{diag['ruleLevel'][1]}'>{diag['ruleLevel'][1]}</td>", f"<td>{diag['suggestion']}</td>"]
                rows.append("<tr>" + "".join(row) + "</tr>")
            else:
                rows.append("<tr class='merge'>" + f"<td>{diag['ruleClassName']}</td>" + f"<td>{diag['ruleDescription']}</td>" + f"<td class='{diag['ruleLevel'][1]}'>{diag['ruleLevel'][1]}</td>" + f"<td>{diag['suggestion']}</td>" + "</tr>")
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
            <p>Command: {data["command"]}</p>
            <p>Files: {data["options"]["files"]}</p>
            <h3>诊断结果</h3>
            <table>
                <thead>
                    <tr>
                        <th>SQL文本</th>
                        <th>诊断规则</th>
                        <th>规则描述</th>
                        <th>规则级别</th>
                        <th>调优建议</th>
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
        self.stdio.print(Fore.YELLOW + "\nAnalyze sql_review results stored in this directory: {0}\n".format(self.local_store_dir) + Style.RESET_ALL)
