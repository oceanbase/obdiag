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
@file: analyze_sql.py
@desc:
"""
import datetime
import html
import json
from decimal import Decimal
import time
import os
from tabulate import tabulate
from src.common.constant import const
from src.common.tool import StringUtils, Util
from src.common.tool import TimeUtils
from src.common.tool import DirectoryUtil
from src.common.ob_connector import OBConnector
from src.handler.meta.sql_meta import GlobalSqlMeta
from src.handler.meta.html_meta import GlobalHtmlMeta
from src.common.tool import FileUtil
from src.handler.analyzer.sql.rule_manager import SQLReviewRuleManager
from src.handler.analyzer.sql.rules.level import Level
from src.handler.analyzer.sql.meta.sys_tenant_meta import SysTenantMeta
from src.handler.gather.gather_scenes import GatherSceneHandler
from src.common.command import get_observer_version
from src.common.result_type import ObdiagResult


class AnalyzeSQLHandler(object):
    def __init__(self, context):
        super(AnalyzeSQLHandler, self).__init__()
        self.context = context
        self.stdio = context.stdio
        self.from_time_str = None
        self.to_time_str = None
        self.from_timestamp = None
        self.to_timestamp = None
        self.config_path = const.DEFAULT_CONFIG_PATH
        self.db_connector_provided = False
        self.tenant_name = None
        self.tenant_name_specified = False
        self.db_user = None
        self.tenant_db_host = None
        self.tenant_db_port = None
        self.local_stored_parrent_path = os.path.abspath('.')
        self.sql_audit_limit = 2000
        self.elapsed_time = 100000
        self.output_type = 'html'
        self.level = 'notice'
        self.ob_version = '4.0.0.0'
        self.sql_audit_keys = [
            'svrIp',
            'svrPort',
            'requestId',
            'clientIp',
            'tenantName',
            'tenantId',
            'dbName',
            'dbId',
            'querySql',
            'planId',
            'sqlId',
            'traceId',
            'requestTime',
            'returnRows',
            'affectedRows',
            'partitionCount',
            'retCode',
            'event0WaitTimeUs',
            'event1WaitTimeUs',
            'event2WaitTimeUs',
            'event3WaitTimeUs',
            'totalWaitTimeMicro',
            'totalWaits',
            'rpcCount',
            'planType',
            'isInnerSql',
            'isExecutorRpc',
            'isHitPlan',
            'elapsedTime',
            'cpuTime',
            'netTime',
            'netWaitTime',
            'queueTime',
            'decodeTime',
            'getPlanTime',
            'executeTime',
            'applicationWaitTime',
            'concurrencyWaitTime',
            'userIoWaitTime',
            'scheduleTime',
            'rowCacheHit',
            'bloomFilterCacheHit',
            'blockCacheHit',
            'blockIndexCacheHit',
            'diskReads',
            'retryCount',
            'tableScan',
            'consistencyLevel',
            'memstoreReadRowCount',
            'ssstoreReadRowCount',
            'planCachePlanExplain',
        ]

    def init_inner_config(self):
        self.stdio.print('init inner config start')
        self.inner_config = self.context.inner_config
        self.stdio.verbose('inner config: {0}'.format(self.inner_config))
        basic_config = self.inner_config['obdiag']['basic']
        self.config_path = basic_config['config_path']
        self.stdio.print('init inner config complete')
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

    def init_ob_version(self):
        self.stdio.print('get observer version start')
        self.ob_version = get_observer_version(self.context)
        self.stdio.print('get observer version complete, version:{0}'.format(self.ob_version))
        return True

    def init_db_connector(self):
        if self.db_user:
            self.db_connector_provided = True
            host = self.tenant_db_host or self.ob_cluster.get("db_host")
            port = self.tenant_db_port if self.tenant_db_port is not None else self.ob_cluster.get("db_port")
            pwd = self.db_password if self.db_password is not None else ''
            self.db_connector = OBConnector(context=self.context, ip=host, port=port, username=self.db_user, password=pwd, timeout=100)
            self.stdio.print(
                "sql_audit: using --user (business tenant) at {0}:{1}, user={2}. (password not logged)".format(host, port, self.db_user)
            )
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
            self.stdio.print(
                "sql_audit: no --user; defaulting to sys tenant from obcluster (host={0}, port={1}, user={2}; config password redacted).".format(
                    summary.get("db_host"), summary.get("db_port"), summary.get("sys_user")
                )
            )

    def init_option(self):
        self._input_error_detail = None
        self.stdio.print('init option start')
        options = self.context.options
        try:
            self.stdio.verbose('options:[{0}]'.format(json.dumps(StringUtils.mask_passwords(dict(vars(options))), ensure_ascii=False, default=str)))
        except Exception:
            self.stdio.verbose('options:[unavailable]')
        from_option = Util.get_option(options, 'from')
        to_option = Util.get_option(options, 'to')
        since_option = Util.get_option(options, 'since')
        db_user_option = Util.get_option(options, 'user')
        if db_user_option:
            tenant_name = self.__extract_tenant_name(db_user_option)
            if not tenant_name:
                self._input_error_detail = (
                    "Invalid --user: cannot derive tenant from username. Expected formats: "
                    "user@tenant (e.g. app@mytenant), user@tenant#cluster, or cluster:tenant:user. "
                    "Omit --user to use sys tenant from obcluster for sql_audit."
                )
                self.stdio.error(self._input_error_detail)
                return False
            self.db_user = db_user_option
            self.tenant_name = tenant_name
        db_password_option = Util.get_option(options, 'password')
        self.db_password = db_password_option
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
        if tenant_name_option:
            self.tenant_name = tenant_name_option
            self.tenant_name_specified = True
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
        limit_option = Util.get_option(options, 'limit')
        if limit_option is not None:
            try:
                self.sql_audit_limit = int(limit_option)
                if self.sql_audit_limit < 1:
                    raise ValueError()
            except (TypeError, ValueError):
                self.stdio.error('Invalid --limit: must be a positive integer')
                return False
        elapsed_time_option = Util.get_option(options, 'elapsed_time')
        if elapsed_time_option is not None:
            try:
                self.elapsed_time = int(elapsed_time_option)
                if self.elapsed_time < 0:
                    raise ValueError()
            except (TypeError, ValueError):
                self.stdio.error('Invalid --elapsed_time: must be a non-negative integer (microseconds)')
                return False
        if from_option is not None and to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(from_option)
                to_timestamp = TimeUtils.parse_time_str(to_option)
                self.from_time_str = from_option
                self.to_time_str = to_option
            except ValueError:
                self.stdio.exception('Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. from_datetime={0}, to_datetime={1}'.format(from_option, to_option))
                return False
            if to_timestamp <= from_timestamp:
                self.stdio.error('Error: from datetime is larger than to datetime, please check.')
                return False
        elif (from_option is None or to_option is None) and since_option is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('analyze sql from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.warn('no time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option is not None:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('analyze sql from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        self.from_timestamp = TimeUtils.datetime_to_timestamp(self.from_time_str, self.stdio)
        self.to_timestamp = TimeUtils.datetime_to_timestamp(self.to_time_str, self.stdio)
        if (self.tenant_db_host or self.tenant_db_port is not None) and not self.db_user:
            self.stdio.warn('--host/--port only take effect with --user (business tenant sql_audit connection).')
        self.stdio.print('init option complete')
        return True

    def handle(self):
        self.start_time = time.time()
        if not self.init_option():
            err = getattr(self, '_input_error_detail', None) or 'init option failed'
            if not getattr(self, '_input_error_detail', None):
                self.stdio.error('init option failed')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data=err)
        if not self.init_inner_config():
            self.stdio.error('init inner config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init inner config failed")
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init config failed")
        if not self.init_ob_version():
            self.stdio.error('init ob version failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init ob version failed")
        self.init_db_connector()
        ext = "json" if self.output_type == "json" else "html"
        run_ts = TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp())
        pack_dir_name = "obdiag_analyze_sql_{0}_{1}_{2}".format(
            TimeUtils.timestamp_to_filename_time(self.from_timestamp),
            TimeUtils.timestamp_to_filename_time(self.to_timestamp),
            run_ts,
        )
        self.pack_dir = os.path.join(self.local_stored_parrent_path, pack_dir_name)
        DirectoryUtil.mkdir(path=self.pack_dir, stdio=self.stdio)
        self.local_store_path = os.path.join(self.pack_dir, "result.{0}".format(ext))
        self.stdio.print("use {0} as result store path.".format(self.pack_dir))
        all_tenant_results = {}
        if self.tenant_name_specified:
            tenant_names = [(self.tenant_name,)]
            self.stdio.print('select sql tenant name list: {0} (specified by --tenant_name)'.format(tenant_names))
        else:
            meta = SysTenantMeta(self.sys_connector, self.stdio, self.ob_version)
            self.stdio.print('select sql tenant name list start')
            tenant_names = meta.get_ob_tenant_name_list()
            self.stdio.print('select sql tenant name list end, result:{0}'.format(tenant_names))
        if not tenant_names:
            self.stdio.error('No tenant available for sql audit. Use --tenant_name or check cluster / sys tenant access.')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='no tenant available for sql audit')
        for tenant_name in tenant_names:
            self.stdio.print('select tenant:{0} sql audit start'.format(tenant_name[0]))
            inner_results = self.__select_sql_audit(tenant_name[0])
            self.stdio.print('select tenant:{0} sql audit complete'.format(tenant_name[0]))
            filter_results = self.__filter_max_elapsed_time_with_same_sql_id(inner_results)
            all_tenant_results[tenant_name] = filter_results
        for tenant_name, results in all_tenant_results.items():
            for item in results:
                item['planCachePlanExplain'] = self.__get_plan_cache_plan_explain(item)
                item['diagnosticEntries'] = self.__parse_sql_review(item["querySql"])
        cluster_data = self.__gather_cluster_info()
        self.__print_cluster_gather_store_dir(cluster_data)
        if self.output_type == "html":
            html_result = self.__generate_html_result(all_tenant_results, cluster_data)
            if html_result:
                FileUtil.write_append(self.local_store_path, html_result)
                self.__print_result()
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=self.__build_success_result_data(cluster_data))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="sql audit result is empty or HTML generation failed")
        elif self.output_type == "json":
            json_result = self.__generate_json_result(all_tenant_results, cluster_data)
            if json_result:
                FileUtil.write_append(self.local_store_path, json_result)
                self.__print_result()
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=self.__build_success_result_data(cluster_data))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="JSON generation failed")
        else:
            self.stdio.error('Unsupported output type: {0}. Use --output html or json.'.format(self.output_type))
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='Unsupported output type: {0}'.format(self.output_type))

    def __extract_tenant_name(self, username):
        """
        Extracts the tenant name from the given OBClient username format.
        Parameters:
            username (str): The username portion of the OBClient connection string, formatted as 'user@tenantName' or 'user@tenantName#clusterName' or 'clusterName:tenantName:user'.
        Returns:
            The tenant name, or None if parsing fails
        """
        # Check for 'user@tenantName' or 'user@tenantName#clusterName' format
        if "@" in username:
            parts = username.split('@')
            if len(parts) == 2:
                return parts[1].split('#')[0] if '#' in parts[1] else parts[1]

        # Check for 'clusterName:tenantName:user' format
        elif ":" in username:
            parts = username.split(':')
            if len(parts) >= 3:
                return parts[1]

        return None

    def __select_sql_audit(self, tenant_name):
        sql = str(GlobalSqlMeta().get_value(key="get_sql_audit_ob4_for_sql_review"))
        replacements = {
            "##REPLACE_TENANT_NAME##": tenant_name,
            "##REPLACE_REQUEST_FROM_TIME##": str(self.from_timestamp),
            "##REPLACE_REQUEST_TO_TIME##": str(self.to_timestamp),
            "##REPLACE_ELAPSED_TIME##": str(self.elapsed_time),
            "##REPLACE_LIMIT##": str(self.sql_audit_limit),
        }
        for old, new in replacements.items():
            sql = sql.replace(old, new)
        self.stdio.verbose("excute SQL: {0}".format(sql))
        columns, rows = self.db_connector.execute_sql_return_columns_and_data(sql)
        result = []
        for row in rows:
            result.append(dict(zip(columns, row)))
        self.stdio.print("excute select sql_audit SQL complete, the length of raw result is {0}".format(len(result)))
        return result

    def __get_plan_cache_plan_explain(self, data):
        try:
            meta = SysTenantMeta(self.sys_connector, self.stdio, self.ob_version)
            column_names, table_data = meta.get_plain_explain_raw(data['tenantId'], data['svrIp'], data['svrPort'], data['planId'])
            return tabulate(table_data, headers=column_names, tablefmt="grid")
        except Exception as e:
            self.stdio.verbose('get plan explain failed: {0}'.format(e))
            return 'plan explain unavailable: {0}'.format(e)

    def __filter_max_elapsed_time_with_same_sql_id(self, data):
        max_elapsed_times = {}
        try:
            for item in data:
                key = (item['tenantId'], item['dbId'], item['sqlId'])
                if key not in max_elapsed_times or item['elapsedTime'] > max_elapsed_times[key]['elapsedTime']:
                    max_elapsed_times[key] = item
        except KeyError as e:
            self.stdio.error('sql_audit row missing expected field (need tenantId, dbId, sqlId, elapsedTime): {0}'.format(e))
            return []
        filtered_data = list(max_elapsed_times.values())
        self.stdio.print("filter filter max elapsed time with same sql_id complete, raw data length:{0}, filter data length:{1}".format(len(data), len(filtered_data)))
        return filtered_data

    def __parse_sql_review(self, sql):
        rules = SQLReviewRuleManager()
        result = rules.manager.analyze_sql_statement(sql, self.stdio, self.level)
        return result

    def __generate_current_row_selected_keys(self, diagnostics, keys, rowspan_length):
        current_row = [f"<td rowspan={rowspan_length}>{html.escape(str(diagnostics.get(key, '')))}</td>" for key in keys]
        return current_row

    def __generate_html_table(self, diagnostics):
        rowspan_length = len(diagnostics['diagnosticEntries'])
        if rowspan_length == 0:
            audit_cells = "".join(f"<td>{html.escape(str(diagnostics.get(k, '')))}</td>" for k in self.sql_audit_keys)
            return "<tr>" + audit_cells + "<td colspan='4'>No rule results (SQL may be invalid or unparsable)</td></tr>"
        rows = []
        current_row = self.__generate_current_row_selected_keys(diagnostics, self.sql_audit_keys, rowspan_length)
        table_head = ''.join(current_row)
        for idx, diag in enumerate(diagnostics['diagnosticEntries']):
            lvl = html.escape(diag.level.string)
            if idx == 0:
                row = table_head + (
                    f"<td>{html.escape(diag.class_name)}</td>",
                    f"<td>{html.escape(diag.description)}</td>",
                    f"<td class='{lvl}'>{lvl}</td>",
                    f"<td>{html.escape(diag.suggestion)}</td>",
                )
                rows.append("<tr>" + "".join(row) + "</tr>")
            else:
                rows.append(
                    "<tr class='merge'>"
                    + f"<td>{html.escape(diag.class_name)}</td>"
                    + f"<td>{html.escape(diag.description)}</td>"
                    + f"<td class='{lvl}'>{lvl}</td>"
                    + f"<td>{html.escape(diag.suggestion)}</td>"
                    + "</tr>"
                )
        return "".join(rows)

    def __generate_table_headers(self):
        headers_html = "".join([f"<th>{item}</th>" for item in self.sql_audit_keys])
        return headers_html

    def __cluster_info_public_dict(self, cluster_data):
        """report_directory / store_dir: run root (result.html), same as ObdiagResult.data.store_dir on success.
        cluster_gather_directory: embedded observer.base pack (GatherSceneHandler report_path)."""
        if not isinstance(cluster_data, ObdiagResult):
            return None
        report_root = os.path.abspath(self.pack_dir)
        gather_dir = cluster_data.data.get("store_dir") if cluster_data.data else None
        return {
            "code": cluster_data.code,
            "report_directory": report_root,
            "store_dir": report_root,
            "cluster_gather_directory": gather_dir,
            "error_data": cluster_data.error_data,
        }

    def __generate_cluster_info_html(self, data):
        if isinstance(data, ObdiagResult):
            display_data = self.__cluster_info_public_dict(data)
            display_str = json.dumps(display_data, ensure_ascii=False, indent=2)
        else:
            display_str = str(data)
        display_str = html.escape(display_str)
        result = f"""
          <div id="collapsibleSection">
            <h3 class="header">Cluster Information</h3>
            <div class="content">
                <pre class="markdown-code-block">{display_str}</pre>
            </div>
        </div>
        """
        result += GlobalHtmlMeta().get_value(key="html_script_templete")
        return result

    def __gather_cluster_info(self):
        handler = GatherSceneHandler(context=self.context, gather_pack_dir=self.pack_dir, is_inner=True)
        return handler.handle()

    def __generate_html_result(self, all_results, cluster_data):
        if len(all_results) == 0:
            self.stdio.error('sql audit result is empty, unable to generate HTML')
            return None
        self.stdio.print('generate html result start')
        full_html = ""
        table_headers = self.__generate_table_headers()
        cluster_info = self.__generate_cluster_info_html(cluster_data)
        all_sql_entries_html = ""
        i = 0
        for key, value in all_results.items():
            tenant_sql_entries_html = ""
            for data in value:
                i += 1
                sql_entries_html = "".join(self.__generate_html_table(data))
                tenant_sql_entries_html += sql_entries_html

            if len(tenant_sql_entries_html) > 0:
                all_sql_entries_html += f"""
                <div id="collapsibleSection">
                    <h3 class="header">Tenant[{key[0]}] SQL Diagnostic Result</h3>
                        <div class="content">
                            <pre class="markdown-code-block">
                <table>
                    <thead>
                        <tr>
                            {table_headers}
                            <th>诊断规则</th>
                            <th>规则描述</th>
                            <th>规则级别</th>
                            <th>诊断建议</th>
                        </tr>
                    </thead>
                    <tbody>
                        {tenant_sql_entries_html}
                    </tbody>
                </table>
                </pre>
                </div>
                </div>
                """

        try:
            opts_raw = vars(self.context.options) if getattr(self.context, "options", None) is not None else {}
            opts_safe = StringUtils.mask_passwords(opts_raw)
            opts_line = html.escape(json.dumps(opts_safe, ensure_ascii=False, default=str))
        except Exception:
            opts_line = html.escape("(options unavailable)")
        full_html += (
            GlobalHtmlMeta().get_value(key="analyze_sql_html_head_template")
            + f"""
            <div id="collapsibleSection">
            <h3 class="header">Command Information</h3>
            <div class="content">
                <pre class="markdown-code-block">
                <p>Command: "obdiag analyze sql"</p>
                <p>Options: {opts_line}</p>
                </pre>
            </div>
            </div>
            {cluster_info}
            """
            + all_sql_entries_html
        )
        full_html += GlobalHtmlMeta().get_value(key="html_footer_temple")
        self.stdio.print('generate html result complete')
        return full_html

    def __json_serializer(self, obj):
        """Custom JSON serializer for datetime, Decimal, etc."""
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError("Object of type %s is not JSON serializable" % type(obj).__name__)

    def __generate_json_result(self, all_results, cluster_data):
        """
        Generate JSON output following industry schema (Salesforce Code Analyzer style).
        Schema: runDir, command, options, clusterInfo, violationCounts, tenants
        """
        run_dir = os.getcwd()
        total = critical = warn = notice = ok = 0
        cluster_info = {}
        if isinstance(cluster_data, ObdiagResult):
            cluster_info = self.__cluster_info_public_dict(cluster_data)
        tenants_json = []
        for tenant_key, rows in all_results.items():
            tenant_name = tenant_key[0] if isinstance(tenant_key, (list, tuple)) else str(tenant_key)
            entries = []
            for row in rows:
                diags = []
                for d in row.get("diagnosticEntries", []):
                    level_str = d.level.string
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
                            "rule": d.rule_name,
                            "severity": level_str,
                            "message": d.description,
                            "suggestion": d.suggestion,
                        }
                    )
                sql_row = {k: v for k, v in row.items() if k != "diagnosticEntries" and k in self.sql_audit_keys}
                sql_row["violations"] = diags
                sql_row["planCachePlanExplain"] = row.get("planCachePlanExplain", "")
                entries.append(sql_row)
            tenants_json.append({"tenant": tenant_name, "diagnosticEntries": entries})
        output = {
            "runDir": run_dir,
            "command": "obdiag analyze sql",
            "options": {"from": self.from_time_str, "to": self.to_time_str, "level": self.level},
            "clusterInfo": cluster_info,
            "violationCounts": {"total": total, "critical": critical, "warn": warn, "notice": notice, "ok": ok},
            "tenants": tenants_json,
        }
        return json.dumps(output, ensure_ascii=False, indent=2, default=self.__json_serializer)

    def __print_cluster_gather_store_dir(self, cluster_data):
        if isinstance(cluster_data, ObdiagResult) and cluster_data.data:
            gather_dir = cluster_data.data.get("store_dir")
            if gather_dir:
                self.stdio.print("cluster gather pack directory: {0}".format(gather_dir))

    def __build_success_result_data(self, cluster_data):
        data = {"store_dir": self.pack_dir, "result_file": os.path.abspath(self.local_store_path)}
        if isinstance(cluster_data, ObdiagResult) and cluster_data.data:
            cg = cluster_data.data.get("store_dir")
            if cg:
                data["cluster_gather_store_dir"] = cg
        return data

    def __print_result(self):
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        data = [["Status", "Result Details", "Time"], ["Completed", self.pack_dir, f"{elapsed_time:.2f} s"]]
        table = tabulate(data, headers="firstrow", tablefmt="grid")
        self.stdio.print("\nAnalyze SQL Summary:")
        self.stdio.print(table)
        self.stdio.print("\n")
