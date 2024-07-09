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
@file: analyze_sql.py
@desc:
"""
import datetime
import time
import os
from tabulate import tabulate
from common.constant import const
from common.tool import StringUtils, Util
from common.tool import TimeUtils
from common.ob_connector import OBConnector
from handler.meta.sql_meta import GlobalSqlMeta
from handler.meta.html_meta import GlobalHtmlMeta
from common.tool import FileUtil
from handler.analyzer.sql.rule_manager import SQLReviewRuleManager
from handler.analyzer.sql.meta.sys_tenant_meta import SysTenantMeta
from handler.gather.gather_scenes import GatherSceneHandler
from common.command import get_observer_version_by_sql


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
        self.tenant_name = 'all'
        self.db_user = None
        self.local_stored_parrent_path = os.path.abspath('./obdiag_analyze/')
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
        self.sys_connector = OBConnector(ip=ob_cluster.get("db_host"), port=ob_cluster.get("db_port"), username=ob_cluster.get("tenant_sys").get("user"), password=ob_cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=100)
        self.ob_cluster_name = ob_cluster.get("ob_cluster_name")
        self.stdio.print('init cluster config complete')
        return True

    def init_ob_version(self):
        self.stdio.print('get observer version start')
        self.ob_version = get_observer_version_by_sql(self.ob_cluster, self.stdio)
        self.stdio.print('get observer version complete, version:{0}'.format(self.ob_version))
        return True

    def init_db_connector(self):
        if self.db_user:
            self.db_connector_provided = True
            self.db_connector = OBConnector(ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.db_user, password=self.db_password, stdio=self.stdio, timeout=100)
        else:
            self.db_connector = self.sys_connector

    def init_option(self):
        self.stdio.print('init option start')
        options = self.context.options
        self.stdio.verbose('options:[{0}]'.format(options))
        from_option = Util.get_option(options, 'from')
        to_option = Util.get_option(options, 'to')
        since_option = Util.get_option(options, 'since')
        db_user_option = Util.get_option(options, 'user')
        if db_user_option:
            tenant_name = self.__extract_tenant_name(db_user_option)
            if tenant_name:
                self.db_user = db_user_option
                self.tenant_name = tenant_name
            else:
                return False
        db_password_option = Util.get_option(options, 'password')
        self.db_password = db_password_option
        level_option = Util.get_option(options, 'level')
        if level_option:
            self.level = level_option
        store_dir_option = Util.get_option(options, 'store_dir')
        if store_dir_option is not None:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('Error: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_parrent_path = os.path.abspath(store_dir_option)
        output_option = Util.get_option(options, 'output')
        if output_option:
            self.output_type = output_option
        limit_option = Util.get_option(options, 'limit')
        if limit_option:
            self.sql_audit_limit = limit_option
        elapsed_time_option = Util.get_option(options, 'elapsed_time')
        if elapsed_time_option:
            self.elapsed_time = elapsed_time_option
        if from_option is not None and to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(from_option)
                to_timestamp = TimeUtils.parse_time_str(to_option)
                self.from_time_str = from_option
                self.to_time_str = to_option
            except:
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
        self.stdio.print('init option complete')
        return True

    def handle(self):
        self.start_time = time.time()
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_inner_config():
            self.stdio.error('init inner config failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        if not self.init_ob_version():
            self.stdio.error('init ob version failed')
            return False
        self.init_db_connector()
        self.local_store_path = os.path.join(self.local_stored_parrent_path, "obdiag_analyze_sql_result_{0}_{1}.html".format(TimeUtils.timestamp_to_filename_time(self.from_timestamp), TimeUtils.timestamp_to_filename_time(self.to_timestamp)))
        self.stdio.print("use {0} as result store path.".format(self.local_store_path))
        self.stdio.print('select sql audit start')
        raw_results = self.__select_sql_audit()
        self.stdio.print('select sql audit complete')
        results = self.__filter_max_elapsed_time_with_same_sql_id(raw_results)
        for item in results:
            item['planCachePlanExplain'] = self.__get_plan_cache_plan_explain(item)
            item['diagnosticEntries'] = self.__parse_sql_review(item["querySql"])
        if self.output_type == "html":
            data = self.__gather_cluster_info()
            html_result = self.__generate_html_result(results, data)
            if html_result:
                FileUtil.write_append(self.local_store_path, html_result)
                self.__print_result()
        else:
            pass

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

        self.stdio.error("unable to recognize the user name format")
        return None

    def __select_sql_audit(self):
        sql = str(GlobalSqlMeta().get_value(key="get_sql_audit_ob4_for_sql_review"))
        replacements = {"##REPLACE_REQUEST_FROM_TIME##": str(self.from_timestamp), "##REPLACE_REQUEST_TO_TIME##": str(self.to_timestamp), "##REPLACE_ELAPSED_TIME##": str(self.elapsed_time), "##REPLACE_LIMIT##": str(self.sql_audit_limit)}
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
        meta = SysTenantMeta(self.sys_connector, self.stdio, self.ob_version)
        column_names, table_data = meta.get_plain_explain_raw(data['tenantId'], data['svrIp'], data['svrPort'], data['planId'])
        formatted_table = tabulate(table_data, headers=column_names, tablefmt="grid")
        return formatted_table

    def __filter_max_elapsed_time_with_same_sql_id(self, data):
        # Create a dictionary to hold the max elapsed time for each db_id, sql_id pair
        max_elapsed_times = {}
        for item in data:
            key = (item['tenantId'], item['dbId'], item['sqlId'])
            if key not in max_elapsed_times or item['elapsedTime'] > max_elapsed_times[key]['elapsedTime']:
                max_elapsed_times[key] = item
        # Extract the values which are the filtered list
        filtered_data = list(max_elapsed_times.values())
        self.stdio.print("filter filter max elapsed time with same sql_id complete, raw data length:{0}, filter data length:{1}".format(len(data), len(filtered_data)))
        return filtered_data

    def __parse_sql_review(self, sql):
        rules = SQLReviewRuleManager()
        result = rules.manager.analyze_sql_statement(sql, self.stdio, self.level)
        return result

    def __generate_current_row_selected_keys(self, diagnostics, keys, rowspan_length):
        current_row = [f"<td rowspan={rowspan_length}>{diagnostics[key]}</td>" for key in keys]
        return current_row

    def __generate_html_table(self, diagnostics):
        rows = []
        rowspan_length = len(diagnostics['diagnosticEntries'])
        current_row = self.__generate_current_row_selected_keys(diagnostics, self.sql_audit_keys, rowspan_length)
        table_head = ''.join(current_row)
        for idx, diag in enumerate(diagnostics['diagnosticEntries']):
            if idx == 0:
                # Start a new row with SQL text having rowspan equal to the number of diagnostics.
                row = table_head + f"<td>{diag.class_name}</td>", f"<td>{diag.description}</td>", f"<td class='{diag.level.string}'>{diag.level.string}</td>", f"<td>{diag.suggestion}</td>"
                rows.append("<tr>" + "".join(row) + "</tr>")
            else:
                rows.append("<tr class='merge'>" + f"<td>{diag.class_name}</td>" + f"<td>{diag.description}</td>" + f"<td class='{diag.level.string}'>{diag.level.string}</td>" + f"<td>{diag.suggestion}</td>" + "</tr>")
        return "".join(rows)

    def __generate_table_headers(self):
        headers_html = "".join([f"<th>{item}</th>" for item in self.sql_audit_keys])
        return headers_html

    def __generate_cluster_info_html(self, data):
        result = f"""
          <div id="collapsibleSection">
            <h3 class="header">Cluster Info</h3>
            <div class="content">
                <pre class="markdown-code-block">{data}</pre>
            </div>
        </div>
        """
        result += GlobalHtmlMeta().get_value(key="html_script_templete")
        return result

    def __gather_cluster_info(self):
        handler = GatherSceneHandler(context=self.context, gather_pack_dir=self.local_stored_parrent_path, is_inner=True)
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
        for data in all_results:
            i += 1
            sql_entries_html = "".join(self.__generate_html_table(data))
            all_sql_entries_html += sql_entries_html
        full_html += (
            GlobalHtmlMeta().get_value(key="analyze_sql_html_head_template")
            + f"""
            <div id="collapsibleSection">
            <h3 class="header">Command Information</h3>
            <div class="content">
                <pre class="markdown-code-block">
                <p>Command: "obdiag analyze sql"</p>
                <p>Options: {self.context.options}</p>
                </pre>
            </div>
            </div>
            """
            + f"""
            {cluster_info}
            <h3>Tenant SQL Diagnostic Result</h3>
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
                    {all_sql_entries_html}
                </tbody>
            </table>
            """
        )
        full_html += GlobalHtmlMeta().get_value(key="html_footer_temple")
        self.stdio.print('generate html result complete')
        return full_html

    def __print_result(self):
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        data = [["Status", "Result Details", "Time"], ["Completed", self.local_store_path, f"{elapsed_time:.2f} s"]]
        table = tabulate(data, headers="firstrow", tablefmt="grid")
        self.stdio.print("\nAnalyze SQL Summary:")
        self.stdio.print(table)
        self.stdio.print("\n")
