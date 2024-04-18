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
@time: 2022/11/29
@file: gather_plan_monitor.py
@desc:
"""
import os
import re
import sys
import shutil
import time
from decimal import Decimal
import pymysql as mysql
import tabulate
from prettytable import from_db_cursor
from common.ob_connector import OBConnector
from handler.meta.html_meta import GlobalHtmlMeta
from handler.meta.sql_meta import GlobalSqlMeta
from common.tool import Util
from common.tool import DirectoryUtil
from common.tool import StringUtils
from common.tool import FileUtil
from common.tool import TimeUtils


class GatherPlanMonitorHandler(object):
    def __init__(self, context, gather_pack_dir='./', is_scene=False):
        self.context = context
        self.stdio = context.stdio
        self.ob_cluster = None
        self.local_stored_path = gather_pack_dir
        self.tenant_mode = None
        self.sys_database = None
        self.database = None
        self.enable_dump_db = True
        self.trace_id = None
        self.env = {}
        self.STAT_NAME = {}
        self.report_file_path = ""
        self.enable_fast_dump = False
        self.ob_major_version = None
        self.sql_audit_name = "gv$sql_audit"
        self.plan_explain_name = "gv$plan_cache_plan_explain"
        self.is_scene = is_scene
        if self.context.get_variable("gather_timestamp", None) :
            self.gather_timestamp=self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        ob_cluster = self.context.cluster_config
        self.ob_cluster = ob_cluster
        self.sys_connector = OBConnector(ip=ob_cluster.get("db_host"), port=ob_cluster.get("db_port"), username=ob_cluster.get("tenant_sys").get("user"), password=ob_cluster.get("tenant_sys").get("password"), stdio=self.stdio, timeout=100)
        self.ob_cluster_name = ob_cluster.get("ob_cluster_name")
        return True

    def init_option(self):
        options = self.context.options
        trace_id_option = Util.get_option(options, 'trace_id')
        store_dir_option = Util.get_option(options, 'store_dir')
        env_option = Util.get_option(options, 'env')
        if self.context.get_variable("gather_plan_monitor_trace_id", None) :
            trace_id_option=self.context.get_variable("gather_plan_monitor_trace_id")
        if trace_id_option is not None:
            self.trace_id = trace_id_option
        else:
            self.stdio.error("option --trace_id not found, please provide")
            return False
        if store_dir_option and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('warn: option --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.local_stored_path = os.path.abspath(store_dir_option)
        if env_option is not None:
            self.__init_db_conn(env_option)
        else:
            self.db_connector = self.sys_connector
        return self.tenant_mode_detected()

    def __init_db_connector(self):
        self.db_connector = OBConnector(ip=self.db_conn.get("host"), port=self.db_conn.get("port"), username=self.db_conn.get("user"), password=self.db_conn.get("password"), database=self.db_conn.get("database"), stdio=self.stdio, timeout=100)

    def handle(self):
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if self.is_scene:
            pack_dir_this_command = self.local_stored_path
        else:
            pack_dir_this_command = os.path.join(self.local_stored_path, "gather_pack_{0}".format(
            TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
        self.report_file_path = os.path.join(pack_dir_this_command, "sql_plan_monitor_report.html")
        self.stdio.verbose("Use {0} as pack dir.".format(pack_dir_this_command))
        DirectoryUtil.mkdir(path=pack_dir_this_command, stdio=self.stdio)
        gather_tuples = []
        gather_pack_path_dict = {}

        def handle_plan_monitor_from_ob(cluster_name):
            """
            handler sql plan monitor from ob
            :return:
            """
            st = time.time()
            resp = self.init_resp()
            result_sql_audit_by_trace_id_limit1 = self.select_sql_audit_by_trace_id_limit1()
            if len(result_sql_audit_by_trace_id_limit1) > 0:
                trace = result_sql_audit_by_trace_id_limit1[0]
                trace_id = trace[0]
                user_sql = trace[1]
                sql = trace[1]
                db_name = trace[8]
                plan_id = trace[9]
                tenant_id = trace[10]
                svr_ip = trace[12]
                svr_port = trace[13]
                self.stdio.verbose("TraceID : %s " % trace_id)
                self.stdio.verbose("SQL : %s " % sql)
                self.stdio.verbose("SVR_IP : %s " % svr_ip)
                self.stdio.verbose("SVR_PORT : %s " % svr_port)
                self.stdio.verbose("DB: %s " % db_name)
                self.stdio.verbose("PLAN_ID: %s " % plan_id)
                self.stdio.verbose("TENANT_ID: %s " % tenant_id)

                sql_plan_monitor_svr_agg_template = self.sql_plan_monitor_svr_agg_template_sql()
                sql_plan_monitor_svr_agg_v1 = str(sql_plan_monitor_svr_agg_template) \
                    .replace("##REPLACE_TRACE_ID##", trace_id) \
                    .replace("##REPLACE_ORDER_BY##", "PLAN_LINE_ID ASC, MAX_CHANGE_TIME ASC, SVR_IP, SVR_PORT")
                sql_plan_monitor_svr_agg_v2 = str(sql_plan_monitor_svr_agg_template) \
                    .replace("##REPLACE_TRACE_ID##", trace_id) \
                    .replace("##REPLACE_ORDER_BY##", "SVR_IP, SVR_PORT, PLAN_LINE_ID")

                sql_plan_monitor_detail_template = self.sql_plan_monitor_detail_template_sql()
                sql_plan_monitor_detail_v1 = str(sql_plan_monitor_detail_template) \
                    .replace("##REPLACE_TRACE_ID##", trace_id) \
                    .replace("##REPLACE_ORDER_BY##", "PLAN_LINE_ID ASC, SVR_IP, SVR_PORT, CHANGE_TS, PROCESS_NAME ASC")
                sql_plan_monitor_detail_v2 = str(sql_plan_monitor_detail_template) \
                    .replace("##REPLACE_TRACE_ID##", trace_id) \
                    .replace("##REPLACE_ORDER_BY##", "PROCESS_NAME ASC, PLAN_LINE_ID ASC, FIRST_REFRESH_TIME ASC")

                sql_plan_monitor_dfo_op = self.sql_plan_monitor_dfo_op_sql(tenant_id, plan_id, trace_id)
                full_audit_sql_by_trace_id_sql = self.full_audit_sql_by_trace_id_sql(trace_id)
                plan_explain_sql = self.plan_explain_sql(tenant_id, plan_id, svr_ip, svr_port)

                # 输出报告头
                self.stdio.verbose("[sql plan monitor report task] report header")
                self.report_header()
                # 输出sql_audit的概要信息
                self.stdio.verbose("[sql plan monitor report task] report sql_audit")
                self.report_sql_audit()
                # 输出sql explain的信息
                self.stdio.verbose("[sql plan monitor report task] report plan explain, sql: [{0}]".format(sql))
                self.report_plan_explain(db_name, sql)
                # 输出plan cache的信息
                self.stdio.verbose("[sql plan monitor report task] report plan cache")
                self.report_plan_cache(plan_explain_sql)
                # 输出表结构的信息
                self.stdio.verbose("[sql plan monitor report task] report table schema")
                self.report_schema(user_sql)
                self.init_monitor_stat()
                # 输出sql_audit的详细信息
                self.stdio.verbose("[sql plan monitor report task] report sql_audit details")
                self.report_sql_audit_details(full_audit_sql_by_trace_id_sql)
                # 输出算子信息 表+图
                self.stdio.verbose("[sql plan monitor report task] report sql plan monitor dfo")
                self.report_sql_plan_monitor_dfo_op(sql_plan_monitor_dfo_op)
                # 输出算子信息按 svr 级汇总 表+图
                self.stdio.verbose("[sql plan monitor report task] report sql plan monitor group by server")
                self.report_sql_plan_monitor_svr_agg(sql_plan_monitor_svr_agg_v1, sql_plan_monitor_svr_agg_v2)
                self.report_fast_preview()
                # 输出算子信息按算子维度聚集
                self.stdio.verbose("[sql plan monitor report task] sql plan monitor detail operator")
                self.report_sql_plan_monitor_detail_operator_priority(sql_plan_monitor_detail_v1)
                # 输出算子信息按线程维度聚集
                self.stdio.verbose("[sql plan monitor report task] sql plan monitor group by priority")
                self.reportsql_plan_monitor_detail_svr_priority(sql_plan_monitor_detail_v2)

                # 输出本报告在租户下使用的 SQL
                self.__report("<h4>本报告在租户下使用的 SQL</h4>")
                self.__report("<div class='help' style='font-size:11px'>DFO 级<hr /><pre>%s</pre></div><br/>" % (
                    sql_plan_monitor_dfo_op))
                self.__report("<div class='help' style='font-size:11px'>机器级<hr /><pre>%s</pre></div><br/>" % (
                    sql_plan_monitor_svr_agg_v1))
                self.__report("<div class='help' style='font-size:11px'>线程级<hr /><pre>%s</pre></div><br/>" % (
                    sql_plan_monitor_detail_v1))

                t = time.localtime(time.time())
                self.__report("报告生成时间： %s" % (time.strftime("%Y-%m-%d %H:%M:%S", t)))
                self.report_footer()
                self.stdio.verbose("report footer complete")

            if resp["skip"]:
                return
            if resp["error"]:
                gather_tuples.append((cluster_name, True, resp["error_msg"], 0, int(time.time() - st),
                                      "Error:{0}".format(resp["error_msg"]), ""))
                return
            gather_pack_path_dict[cluster_name] = resp["gather_pack_path"]
            gather_tuples.append((cluster_name, False, "", int(time.time() - st), pack_dir_this_command))

        if getattr(sys, 'frozen', False):
                absPath = os.path.dirname(sys.executable)
        else:
            absPath = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        cs_resources_path = os.path.join(absPath, "resources")
        self.stdio.verbose("[cs resource path] : {0}".format(cs_resources_path))
        target_resources_path = os.path.join(pack_dir_this_command, "resources")
        self.copy_cs_resource(cs_resources_path, target_resources_path)
        self.stdio.verbose("[sql plan monitor report task] start")
        handle_plan_monitor_from_ob(self.ob_cluster_name)
        self.stdio.verbose("[sql plan monitor report task] end")
        summary_tuples = self.__get_overall_summary(gather_tuples)
        self.stdio.print(summary_tuples)
        # 将汇总结果持久化记录到文件中
        FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)
        return gather_tuples, gather_pack_path_dict


    def __init_db_conn(self, env):
        try:
            env_dict = StringUtils.parse_env(env)
            self.env = env_dict
            cli_connection_string = self.env.get("db_connect")
            self.db_conn = StringUtils.parse_mysql_conn(cli_connection_string)
            if StringUtils.validate_db_info(self.db_conn):
                self.__init_db_connector()
            else:
                self.stdio.error("db connection information requird [db_connect = '-hxx -Pxx -uxx -pxx -Dxx'] but provided {0}, please check the --env {0}".format(env_dict))
                self.db_connector = self.sys_connector
        except Exception as e:
            self.db_connector = self.sys_connector
            self.stdio.exception("init db connector, error: {0}, please check --env option ")

    @staticmethod
    def __get_overall_summary(node_summary_tuple):
        """
        generate overall summary from ob summary tuples
        :param summary_tuple: (cluster, is_err, err_msg, size, consume_time)
        :return: a string indicating the overall summary
        """
        summary_tab = []
        field_names = ["Cluster", "Status", "Time", "PackPath"]
        for tup in node_summary_tuple:
            cluster = tup[0]
            is_err = tup[2]
            consume_time = tup[3]
            pack_path = tup[4]
            summary_tab.append(
                (cluster, "Error" if is_err else "Completed", "{0} s".format(int(consume_time)), pack_path))
        return "\nGather Sql Plan Monitor Summary:\n" + \
               tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)

    def report_schema(self, sql):
        try:
            schemas = ""
            valid_words = []
            if self.enable_dump_db:
                words = [w.strip(',') for w in ("%s" % sql).split() if not (
                        "[" in w or "=" in w or "|" in w or "(" in w or "--" in w or "]" in w or ")" in w or "*" in w or "/" in w or "%" in w or "'" in w or "-" in w or w.isdigit())]
                for t in words:
                    if t in valid_words:
                        continue
                    valid_words.append(t)
                for t in valid_words:
                    try:
                        data = self.db_connector.execute_sql("show create table %s" % t)
                        schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % (data[1])
                        self.stdio.verbose("table schema: {0}".format(schemas))
                    except Exception as e:
                        pass
            cursor = self.sys_connector.execute_sql_return_cursor("show variables like '%parallel%'")
            s = from_db_cursor(cursor)
            s.align = 'l'
            schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % s

            cursor.execute("show variables")
            s = from_db_cursor(cursor)
            s.align = 'l'
            schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % s

            cursor.execute("show parameters")
            s = from_db_cursor(cursor)
            s.align = 'l'
            schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % s
            self.__report(
                "<div><h2 id='schema_anchor'>SCHEMA 信息</h2><div id='schema' style='display: none'>" + schemas + "</div></div>")
            cursor.close()
        except Exception as e:
            self.stdio.exception("report table schema failed %s" % sql)
            self.stdio.exception(repr(e))
            pass

    def report_pre(self, s):
        pre = f'''<pre style='margin:20px;border:1px solid gray;'>{s}</pre>'''
        self.__report(pre)

    def report_header(self):
        header = GlobalHtmlMeta().get_value(key="sql_plan_monitor_report_header")
        with open(self.report_file_path, 'w') as f:
            f.write(header)
        self.stdio.verbose("report header complete")

    def init_monitor_stat(self):
        sql = "select ID,NAME,TYPE from " + (
            "SYS." if self.tenant_mode == "oracle" else "oceanbase.") + "v$sql_monitor_statname order by ID"
        data = self.sys_connector.execute_sql(sql)
        for item in data:
            self.STAT_NAME[item[0]] = {"type": item[2], "name": item[1]}
        self.stdio.verbose("init sql plan monitor stat complete")

    def otherstat_detail_explain_item(self, item, n, v):
        try:
            if 0 == item[n]:
                val = ""
            elif self.STAT_NAME[item[n]]["type"] <= 1:
                val = str(item[v])
            elif self.STAT_NAME[item[n]]["type"] == 2:
                val = "%0.3fMB" % (item[n + 1] / 1024.0 / 1024)
            elif self.STAT_NAME[item[n]]["type"] == 3:
                val = "%s.%06d" % (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(item[v] / 1000000)),
                                   item[v] - (item[v] / 1000000) * 1000000)
            else:
                val = str(item[v])
        except Exception as e:
            val = str(item[v])
        return "" if item[n] == 0 else self.STAT_NAME[item[n]]["name"] + "(" + val + ");<br/>"

    def otherstat_agg_explain_item(self, item, n, v):
        try:
            if 0 == item[n]:
                val = None
            elif self.STAT_NAME[item[n]]["type"] <= 1:
                val = str(item[v])
            elif self.STAT_NAME[item[n]]["type"] == 2:
                val = "%0.3fMB" % (float(item[v]) / 1024.0 / 1024)
            else:
                val = None
        except Exception as e:
            val = str(item[v])
        return "" if val == None else self.STAT_NAME[item[n]]["name"] + "(" + val + ");<br/>"

    def detail_otherstat_explain(self, item):
        otherstat = ""
        otherstat += self.otherstat_detail_explain_item(item, "OTHERSTAT_1_ID", "OTHERSTAT_1_VALUE")
        otherstat += self.otherstat_detail_explain_item(item, "OTHERSTAT_2_ID", "OTHERSTAT_2_VALUE")
        otherstat += self.otherstat_detail_explain_item(item, "OTHERSTAT_3_ID", "OTHERSTAT_3_VALUE")
        otherstat += self.otherstat_detail_explain_item(item, "OTHERSTAT_4_ID", "OTHERSTAT_4_VALUE")
        otherstat += self.otherstat_detail_explain_item(item, "OTHERSTAT_5_ID", "OTHERSTAT_5_VALUE")
        otherstat += self.otherstat_detail_explain_item(item, "OTHERSTAT_6_ID", "OTHERSTAT_6_VALUE")
        return otherstat

    def dfo_otherstat_explain(self, item):
        otherstat = ""
        otherstat += self.otherstat_agg_explain_item(item, "OTHERSTAT_1_ID", "SUM_STAT_1")
        otherstat += self.otherstat_agg_explain_item(item, "OTHERSTAT_2_ID", "SUM_STAT_2")
        otherstat += self.otherstat_agg_explain_item(item, "OTHERSTAT_3_ID", "SUM_STAT_3")
        otherstat += self.otherstat_agg_explain_item(item, "OTHERSTAT_4_ID", "SUM_STAT_4")
        otherstat += self.otherstat_agg_explain_item(item, "OTHERSTAT_5_ID", "SUM_STAT_5")
        otherstat += self.otherstat_agg_explain_item(item, "OTHERSTAT_6_ID", "SUM_STAT_6")
        return otherstat

    def report_detail_graph_data(self, ident, cursor, title=''):
        data = "<script> var %s = [" % ident
        for item in cursor:
            start = (0 if None == item['FIRST_CHANGE_TS'] else item['FIRST_CHANGE_TS'])
            end = (0 if None == item['LAST_CHANGE_TS'] else item['LAST_CHANGE_TS'])
            rows = (0 if None == item['OUTPUT_ROWS'] else item['OUTPUT_ROWS'])
            otherstat = self.detail_otherstat_explain(item)
            data = data + "{start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d, tag:'op', depth:%d, rescan:%d, svr_ip:'%s', otherstat:'%s'}," % (
                start, end, end - start, item['PLAN_LINE_ID'], item['PLAN_OPERATION'], item['PROCESS_NAME'], rows,
                item['PLAN_DEPTH'], item['RESCAN_TIMES'], item['SVR_IP'], otherstat)
        data = data + "{start:0}];</script>"
        data = data + "<p>%s</p><div class='bar' id='%s'></div>" % (title, ident)
        self.__report(data)

    def report_detail_graph_data_obversion4(self, ident, cursor, title=''):
        data = "<script> var %s = [" % ident
        for item in cursor:
            start = (0 if None == item['FIRST_CHANGE_TS'] else item['FIRST_CHANGE_TS'])
            end = (0 if None == item['LAST_CHANGE_TS'] else item['LAST_CHANGE_TS'])
            rows = (0 if None == item['OUTPUT_ROWS'] else item['OUTPUT_ROWS'])
            otherstat = self.detail_otherstat_explain(item)
            data = data + "{cpu:%f, io:%f, start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d, tag:'op', depth:%d, rescan:%d, svr_ip:'%s', otherstat:'%s'}," % (
                item['MY_CPU_TIME'], item['MY_IO_TIME'], start, end, end - start, item['PLAN_LINE_ID'],
                item['PLAN_OPERATION'], item['PROCESS_NAME'], rows, item['PLAN_DEPTH'], item['RESCAN_TIMES'],
                item['SVR_IP'], otherstat)
        data = data + "{start:0}];</script>"
        data = data + "<p>%s</p><div class='bar' id='%s'></div>" % (title, ident)
        self.__report(data)

    # dfo db time
    def report_dfo_agg_db_time_graph_data_obversion4(self, cursor, title=''):
        data = "<script> var db_time_serial = ["
        for item in cursor:
            start = Decimal('0.00001')
            end = item['MY_DB_TIME'] + start
            diff = end - start  # db time diff
            rows = item['TOTAL_OUTPUT_ROWS']
            op_id = item['PLAN_LINE_ID']
            op = item['PLAN_OPERATION']
            depth = item['PLAN_DEPTH']
            est_rows = 0
            threads = item['THREAD_NUM']
            my_cpu_time = item['MY_CPU_TIME']
            my_io_time = item['MY_IO_TIME']
            otherstat = "my_db_time:%f, my_cpu_time:%f, my_io_time:%f" % (
                item['MY_DB_TIME'], item['MY_CPU_TIME'], item['MY_IO_TIME'])
            data = data + "{cpu:%f,io:%f,start:%f, end:%f, diff:%f, my_io_time:%f, my_cpu_time:%f, opid:%s, op:'%s', est_rows:0, rows:%d, tag:'db_time', tid: %d, depth:%d, otherstat:'%s'}," % (
                item['MY_CPU_TIME'], item['MY_IO_TIME'], start, end, diff, my_io_time, my_cpu_time, op_id, op, rows,
                threads, depth, otherstat)
        data = data + "{start:0}];"
        data = data + "</script><p>%s</p><div class='bar' id='db_time_serial'></div>" % (title);
        self.__report(data)

    def report_dfo_agg_graph_data(self, cursor, title=''):
        data = "<script> var agg_serial = ["
        for item in cursor:
            start = (0 if None == item['MIN_FIRST_CHANGE_TS'] else item['MIN_FIRST_CHANGE_TS'])
            end = (0 if None == item['MAX_LAST_CHANGE_TS'] else item['MAX_LAST_CHANGE_TS'])
            rows = (0 if None == item['TOTAL_OUTPUT_ROWS'] else item['TOTAL_OUTPUT_ROWS'])
            est_rows = (0 if None == item['EST_ROWS'] else item['EST_ROWS'])
            otherstat = self.dfo_otherstat_explain(item)
            data = data + "{start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d,est_rows:%d, tag:'dfo', depth:%d, otherstat:'%s'}," % (
                start, end, end - start, item['PLAN_LINE_ID'],
                item['PLAN_OPERATION'], item['PARALLEL'], rows, est_rows, item['PLAN_DEPTH'], otherstat)
        data = data + "{start:0}];"
        data = data + "</script><p>%s</p><div class='bar' id='agg_serial'></div>" % (title)
        self.__report(data)

    def report_dfo_agg_graph_data_obversion4(self, cursor, title=''):
        data = "<script> var agg_serial = ["
        for item in cursor:
            start = (0 if None == item['MIN_FIRST_CHANGE_TS'] else item['MIN_FIRST_CHANGE_TS'])
            end = (0 if None == item['MAX_LAST_CHANGE_TS'] else item['MAX_LAST_CHANGE_TS'])
            rows = (0 if None == item['TOTAL_OUTPUT_ROWS'] else item['TOTAL_OUTPUT_ROWS'])
            skewness = (0 if None == item['SKEWNESS'] else item['SKEWNESS'])
            est_rows = (0 if None == item['EST_ROWS'] else item['EST_ROWS'])
            otherstat = self.dfo_otherstat_explain(item)
            data = data + "{cpu:%f,io:%f,start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d,est_rows:%d, tag:'dfo', depth:%d, otherstat:'%s', skewness:%.2f}," % (
                item['MY_CPU_TIME'], item['MY_IO_TIME'], start, end, end - start, item['PLAN_LINE_ID'],
                item['PLAN_OPERATION'], item['PARALLEL'], rows, est_rows, item['PLAN_DEPTH'], otherstat, skewness)
        data = data + "{start:0}];"
        data = data + "</script><p>%s</p><div class='bar' id='agg_serial'></div>" % (title)
        self.__report(data)

    def report_dfo_sched_agg_graph_data(self, cursor, title=''):
        data = "<script> var agg_sched_serial = ["
        for item in cursor:
            start = (0 if None == item['MIN_FIRST_REFRESH_TS'] else item['MIN_FIRST_REFRESH_TS'])
            end = (0 if None == item['MAX_LAST_REFRESH_TS'] else item['MAX_LAST_REFRESH_TS'])
            rows = (0 if None == item['TOTAL_OUTPUT_ROWS'] else item['TOTAL_OUTPUT_ROWS'])
            est_rows = (0 if None == item['EST_ROWS'] else item['EST_ROWS'])
            otherstat = self.dfo_otherstat_explain(item)
            data = data + "{start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d,est_rows:%d, tag:'dfo', " \
                          "depth:%d, otherstat:'%s'}," % (start, end, end - start, item['PLAN_LINE_ID'],
                                                          item['PLAN_OPERATION'], item['PARALLEL'], rows, est_rows,
                                                          item['PLAN_DEPTH'], otherstat)
        data = data + "{start:0}];"
        data = data + "</script><p>%s</p><div class='bar' id='agg_sched_serial'></div>" % (title)
        self.__report(data)

    def report_dfo_sched_agg_graph_data_obversion4(self, cursor, title=''):
        data = "<script> var agg_sched_serial = ["
        for item in cursor:
            start = (0 if None == item['MIN_FIRST_REFRESH_TS'] else item['MIN_FIRST_REFRESH_TS'])
            end = (0 if None == item['MAX_LAST_REFRESH_TS'] else item['MAX_LAST_REFRESH_TS'])
            rows = (0 if None == item['TOTAL_OUTPUT_ROWS'] else item['TOTAL_OUTPUT_ROWS'])
            skewness = (0 if None == item['SKEWNESS'] else item['SKEWNESS'])
            est_rows = (0 if None == item['EST_ROWS'] else item['EST_ROWS'])
            otherstat = self.dfo_otherstat_explain(item)
            data = data + "{cpu:%f,io:%f,start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',rows:%d,est_rows:%d, " \
                          "tag:'dfo', depth:%d, otherstat:'%s', skewness:%.2f}," % (item['MY_CPU_TIME'],
                                                                                    item['MY_IO_TIME'], start, end,
                                                                                    end - start, item['PLAN_LINE_ID'],
                                                                                    item['PLAN_OPERATION'],
                                                                                    item['PARALLEL'], rows, est_rows,
                                                                                    item['PLAN_DEPTH'], otherstat,
                                                                                    skewness)
        data = data + "{start:0}];"
        data = data + "</script><p>%s</p><div class='bar' id='agg_sched_serial'></div>" % (title)
        self.__report(data)

    # sqc，辅助查询协调者
    def report_svr_agg_graph_data(self, ident, cursor, title=''):
        data = "<script> var %s = [" % ident
        for item in cursor:
            start = (0 if None == item['MIN_FIRST_CHANGE_TS'] else item['MIN_FIRST_CHANGE_TS'])
            end = (0 if None == item['MAX_LAST_CHANGE_TS'] else item['MAX_LAST_CHANGE_TS'])
            rows = (0 if None == item['TOTAL_OUTPUT_ROWS'] else item['TOTAL_OUTPUT_ROWS'])
            data = data + "{start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',svr:'%s',rows:%d, " \
                          "tag:'sqc', depth:%d}," % (start, end, end - start, item['PLAN_LINE_ID'],
                                                     item['PLAN_OPERATION'], item['PARALLEL'],
                                                     item['SVR_IP'] + ':' + str(item['SVR_PORT']), rows,
                                                     item['PLAN_DEPTH'])
        data = data + "{start:0}];</script>"
        data = data + "<p>%s</p><div class='bar' id='%s'></div>" % (title, ident)
        self.stdio.verbose("report SQL_PLAN_MONITOR SQC operator priority start, DATA: %s", data)
        self.__report(data)

    def report_svr_agg_graph_data_obversion4(self, ident, cursor, title=''):
        data = "<script> var %s = [" % ident
        for item in cursor:
            start = (0 if None == item['MIN_FIRST_CHANGE_TS'] else item['MIN_FIRST_CHANGE_TS'])
            end = (0 if None == item['MAX_LAST_CHANGE_TS'] else item['MAX_LAST_CHANGE_TS'])
            rows = (0 if None == item['TOTAL_OUTPUT_ROWS'] else item['TOTAL_OUTPUT_ROWS'])
            skewness = (0 if None == item['SKEWNESS'] else item['SKEWNESS'])
            data = data + "{cpu:%f,io:%f,start:%f, end:%f, diff:%f, opid:%s, op:'%s',tid:'%s',svr:'%s',rows:%d, " \
                          "tag:'sqc', depth:%d, skewness:%.2f}," % (item['MY_CPU_TIME'], item['MY_IO_TIME'], start,
                                                                    end, end - start, item['PLAN_LINE_ID'],
                                                                    item['PLAN_OPERATION'], item['PARALLEL'],
                                                                    item['SVR_IP'] + ':' + str(item['SVR_PORT']), rows,
                                                                    item['PLAN_DEPTH'], skewness)
        data = data + "{start:0}];</script>"
        data = data + "<p>%s</p><div class='bar' id='%s'></div>" % (title, ident)
        self.stdio.verbose("report SQL_PLAN_MONITOR SQC operator priority start, DATA: %s", data)
        self.__report(data)

    def report_fast_preview(self):
        content = '''
        <script>
        generate_graph("dfo", agg_serial, $('#agg_serial'));
        generate_graph("dfo", agg_sched_serial, $('#agg_sched_serial'));
        generate_graph("sqc", svr_agg_serial_v1, $('#svr_agg_serial_v1'));
        generate_graph("sqc", svr_agg_serial_v2, $('#svr_agg_serial_v2'));
        </script>
        '''
        self.__report(content)
        self.stdio.verbose("report SQL_PLAN_MONITOR fast preview complete")

    def report_footer(self):
        footer = GlobalHtmlMeta().get_value(key="sql_plan_monitor_report_footer")
        self.__report(footer)

    def __report(self, s):
        with open(self.report_file_path, 'a') as f:
            f.write(s)

    def tenant_mode_detected(self):
        try:
            # Detect MySQL mode
            data = self.db_connector.execute_sql("show variables like 'version_comment'")
            for row in data:
                ob_version = row[1]

            version_pattern = r'(?:OceanBase(_CE)?\s+)?(\d+\.\d+\.\d+\.\d+)'
            matched_version = re.search(version_pattern, ob_version)

            if matched_version:
                version = matched_version.group(2)
                major_version = int(version.split('.')[0])

                self.sql_audit_name = "gv$ob_sql_audit" if major_version >= 4 else "gv$sql_audit"
                self.plan_explain_name = (
                    "gv$ob_plan_cache_plan_explain" if major_version >= 4 else "gv$plan_cache_plan_explain"
                )
                self.ob_major_version = major_version
                self.tenant_mode = "mysql"
                self.sys_database = "oceanbase"

                self.stdio.verbose(f"Detected MySQL mode successful, Database version: {ob_version}")
                return True
            else:
                raise ValueError("Failed to match MySQL version")
        except Exception as e:
            # Detect Oracle mode
            try:
                data = self.sys_connector.execute_sql("select SUBSTR(BANNER, 11, 100) from V$VERSION;")
                banner = data[0][0]

                version_pattern = r'(\d+\.\d+\.\d+\.\d+)'
                matched_version = re.search(version_pattern, banner)

                if matched_version:
                    version = matched_version.group(1)
                    major_version = int(version.split('.')[0])

                    self.sql_audit_name = f"gv$ob_sql_audit" if major_version >= 4 else "gv$sql_audit"
                    self.ob_major_version = major_version
                    self.tenant_mode = "oracle"
                    self.sys_database = "SYS"

                    self.stdio.verbose(f"Detected Oracle mode successful, Database version: {version}")
                    return True
                else:
                    raise ValueError("Failed to match Oracle version")
            except Exception as oe:
                self.stdio.error(f"Error detecting database mode: {oe}")

    def init_resp(self):
        """
        the handler for one ob cluster
        :param target_ob: the agent object
        :return: a resp dict, indicating the information of the response
        """
        resp = {"skip": False, "error": False, "gather_pack_path": self.local_stored_path}
        if resp["error"]:
            return resp
        return resp

    def copy_cs_resource(self, source_path, target_path):
        shutil.copytree(source_path, target_path)

    def sql_audit_by_trace_id_limit1_sql(self):
        if self.tenant_mode == 'mysql':
            sql = str(GlobalSqlMeta().get_value(key="sql_audit_by_trace_id_limit1_mysql")) \
                .replace("##REPLACE_TRACE_ID##", self.trace_id).replace("##REPLACE_SQL_AUDIT_TABLE_NAME##",
                                                                           self.sql_audit_name)
        else:
            sql = str(GlobalSqlMeta().get_value(key="sql_audit_by_trace_id_limit1_oracle")).replace(
                "##REPLACE_TRACE_ID##", self.trace_id).replace("##REPLACE_SQL_AUDIT_TABLE_NAME##",
                                                                  self.sql_audit_name)
        return sql

    def select_sql_audit_by_trace_id_limit1(self):
        sql = self.sql_audit_by_trace_id_limit1_sql()
        result = self.sys_connector.execute_sql(sql)
        return result

    def plan_explain_sql(self, tenant_id, plan_id, svr_ip, svr_port):
        if self.tenant_mode == 'mysql':
            if self.ob_major_version >= 4:
                sql = "select * from oceanbase.gv$ob_plan_cache_plan_explain where tenant_id = %s and " \
                      "plan_id = %s  and svr_ip = '%s' and svr_port = %s" % (tenant_id, plan_id, svr_ip, svr_port)
            else:
                sql = "select * from oceanbase.gv$plan_cache_plan_explain where tenant_id = %s and " \
                      "plan_id = %s  and ip = '%s' and port = %s" % (tenant_id, plan_id, svr_ip, svr_port)
        else:
            if self.ob_major_version >= 4:
                sql = "select * from sys.gv$ob_plan_cache_plan_explain where tenant_id = %s and plan_id = " \
                      "%s  and svr_ip = '%s' and svr_port = %s" % (tenant_id, plan_id, svr_ip, svr_port)
            else:
                sql = "select * from sys.gv$plan_cache_plan_explain where tenant_id = %s and plan_id = " \
                      "%s  and svr_ip = '%s' and svr_port = %s" % (tenant_id, plan_id, svr_ip, svr_port)
        return sql

    def full_audit_sql_by_trace_id_sql(self, trace_id):
        if self.tenant_mode == 'mysql':
            if self.ob_major_version >=4:
                sql = "select /*+ sql_audit */ %s from oceanbase.%s where trace_id = '%s' " \
                  "AND client_ip IS NOT NULL ORDER BY QUERY_SQL ASC, REQUEST_ID" % (
                      GlobalSqlMeta().get_value(key="sql_audit_item_mysql_obversion4"), self.sql_audit_name, trace_id)
            else:
                sql = "select /*+ sql_audit */ %s from oceanbase.%s where trace_id = '%s' " \
                      "AND client_ip IS NOT NULL ORDER BY QUERY_SQL ASC, REQUEST_ID" % (
                          GlobalSqlMeta().get_value(key="sql_audit_item_mysql"), self.sql_audit_name, trace_id)
        else:
            if self.ob_major_version >=4:
                sql = "select /*+ sql_audit */ %s from sys.%s where trace_id = '%s' AND  " \
                  "length(client_ip) > 4 ORDER BY  REQUEST_ID" % (
                      GlobalSqlMeta().get_value(key="sql_audit_item_oracle_obversion4"), self.sql_audit_name, trace_id)
            else:
                sql = "select /*+ sql_audit */ %s from sys.%s where trace_id = '%s' AND  " \
                      "length(client_ip) > 4 ORDER BY  REQUEST_ID" % (
                          GlobalSqlMeta().get_value(key="sql_audit_item_oracle"), self.sql_audit_name, trace_id)
        return sql

    def sql_plan_monitor_dfo_op_sql(self, tenant_id, plan_id, trace_id):
        if self.tenant_mode == 'mysql':
            if self.ob_major_version >= 4:
                sql = str(GlobalSqlMeta().get_value(key="sql_plan_monitor_dfo_op_mysql_obversion4")) \
                    .replace("##REPLACE_TRACE_ID##", trace_id) \
                    .replace("##REPLACE_PLAN_ID##", str(plan_id)) \
                    .replace("##REPLACE_TENANT_ID##", str(tenant_id)) \
                    .replace("##REPLACE_PLAN_EXPLAIN_TABLE_NAME##", self.plan_explain_name)
            else:
                sql = str(GlobalSqlMeta().get_value(key="sql_plan_monitor_dfo_op_mysql")) \
                    .replace("##REPLACE_TRACE_ID##", trace_id) \
                    .replace("##REPLACE_PLAN_ID##", str(plan_id)) \
                    .replace("##REPLACE_TENANT_ID##", str(tenant_id)) \
                    .replace("##REPLACE_PLAN_EXPLAIN_TABLE_NAME##", self.plan_explain_name)
        else:
            if self.ob_major_version >= 4:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_dfo_op_oracle_obversion4") \
                    .replace("##REPLACE_TRACE_ID##", trace_id) \
                    .replace("##REPLACE_PLAN_ID##", str(plan_id)) \
                    .replace("##REPLACE_TENANT_ID##", str(tenant_id)) \
                    .replace("##REPLACE_PLAN_EXPLAIN_TABLE_NAME##", self.plan_explain_name)
            else:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_dfo_op_oracle") \
                    .replace("##REPLACE_TRACE_ID##", trace_id) \
                    .replace("##REPLACE_PLAN_ID##", str(plan_id)) \
                    .replace("##REPLACE_TENANT_ID##", str(tenant_id)) \
                    .replace("##REPLACE_PLAN_EXPLAIN_TABLE_NAME##", self.plan_explain_name)

        return sql

    def sql_plan_monitor_svr_agg_template_sql(self):
        if self.tenant_mode == 'mysql':
            if self.ob_major_version >= 4:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_svr_agg_template_mysql_obversion4")
            else:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_svr_agg_template_mysql")
        else:
            if self.ob_major_version >= 4:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_svr_agg_template_oracle_obversion4")
            else:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_svr_agg_template_oracle")
        return sql

    def sql_plan_monitor_detail_template_sql(self):
        if self.tenant_mode == 'mysql':
            if self.ob_major_version >= 4:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_detail_template_mysql_obversion4")
            else:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_detail_template_mysql")
        else:
            if self.ob_major_version >= 4:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_detail_template_oracle_obversion4")
            else:
                sql = GlobalSqlMeta().get_value(key="sql_plan_monitor_detail_template_oracle")
        return sql

    # sql audit 细节
    def report_sql_audit_details(self, sql):
        if self.enable_dump_db:
            full_audit_sql_result = self.sys_connector.execute_sql_pretty(sql)
            self.__report(
                "<div><h2 id='sql_audit_table_anchor'>SQL_AUDIT 信息</h2><div class='v' id='sql_audit_table' style='display: none'>" + full_audit_sql_result.get_html_string() + "</div></div>")
        self.stdio.verbose("report full sql audit complete")

    # plan cache
    def report_plan_cache(self, sql):
        try:
            cursor_plan_explain = self.sys_connector.execute_sql_return_cursor(sql)
            self.stdio.verbose("select plan_explain from ob complete")
            self.report_pre(sql)
            self.stdio.verbose("report plan_explain_sql complete")

            data_plan_explain = from_db_cursor(cursor_plan_explain)
            data_plan_explain.align = 'l'
            self.report_pre(data_plan_explain)
            self.stdio.verbose("report plan_explain complete")
        except Exception as e:
            self.stdio.exception("plan cache> %s" % sql)
            self.stdio.exception(repr(e))
            pass

    # sql_audit 概要
    def report_sql_audit(self):
        sql = self.sql_audit_by_trace_id_limit1_sql()
        self.stdio.verbose("select sql_audit from ob with SQL: %s", sql)
        try:
            sql_audit_result = self.sys_connector.execute_sql_pretty(sql)
            self.stdio.verbose("sql_audit_result: %s", sql_audit_result)
            self.stdio.verbose("report sql_audit_result to file start ...")
            self.__report(sql_audit_result.get_html_string())
            self.stdio.verbose("report sql_audit_result end")
        except Exception as e:
            self.stdio.exception("sql_audit> %s" % sql)
            self.stdio.exception(repr(e))

    def report_plan_explain(self, db_name, raw_sql):
        explain_sql = "explain %s" % raw_sql
        try:
            sql_explain_cursor = self.db_connector.execute_sql_return_cursor(explain_sql)
            self.stdio.verbose("execute SQL: %s", explain_sql)
            sql_explain_result_sql = "%s" % explain_sql
            sql_explain_result = from_db_cursor(sql_explain_cursor)

            # output explain result
            self.stdio.verbose("report sql_explain_result_sql complete")
            self.report_pre(sql_explain_result_sql)
            self.stdio.verbose("report sql_explain_result_sql complete")
            sql_explain_result.align = 'l'
            self.report_pre(sql_explain_result)
            self.stdio.verbose("report sql_explain_result complete")
        except Exception as e:
            self.stdio.exception("plan explain> %s" % explain_sql)
            self.stdio.exception(repr(e))
            pass

    def report_sql_plan_monitor_dfo_op(self, sql):
        data_sql_plan_monitor_dfo_op = self.sys_connector.execute_sql_pretty(sql)
        self.__report(
            "<div><h2 id='agg_table_anchor'>SQL_PLAN_MONITOR DFO 级调度时序汇总</h2><div class='v' id='agg_table' style='display: none'>" + data_sql_plan_monitor_dfo_op.get_html_string() + "</div></div>")
        self.stdio.verbose("report SQL_PLAN_MONITOR DFO complete")
        cursor_sql_plan_monitor_dfo_op = self.sys_connector.execute_sql_return_cursor_dictionary(sql)
        if self.ob_major_version >= 4:
            self.report_dfo_sched_agg_graph_data_obversion4(cursor_sql_plan_monitor_dfo_op, '调度时序图')
        else:
            self.report_dfo_sched_agg_graph_data(cursor_sql_plan_monitor_dfo_op, '调度时序图')
        self.stdio.verbose("report SQL_PLAN_MONITOR DFO SCHED complete")
        cursor_sql_plan_monitor_dfo_op = self.sys_connector.execute_sql_return_cursor_dictionary(sql)
        if self.ob_major_version >= 4:
            self.report_dfo_agg_graph_data_obversion4(cursor_sql_plan_monitor_dfo_op, '数据时序图')
        else:
            self.report_dfo_agg_graph_data(cursor_sql_plan_monitor_dfo_op, '数据时序图')
        self.stdio.verbose("report SQL_PLAN_MONITOR DFO graph data complete")

    def report_sql_plan_monitor_svr_agg(self, sql_plan_monitor_svr_agg_v1, sql_plan_monitor_svr_agg_v2):
        cursor_sql_plan_monitor_svr_agg = self.sys_connector.execute_sql_return_cursor(sql_plan_monitor_svr_agg_v1)
        self.__report(
            "<div><h2 id='svr_agg_table_anchor'>SQL_PLAN_MONITOR SQC 级汇总</h2><div class='v' id='svr_agg_table' style='display: none'>" + from_db_cursor(
                cursor_sql_plan_monitor_svr_agg).get_html_string() + "</div><div class='shortcut'><a href='#svr_agg_serial_v1'>Goto 算子优先</a> <a href='#svr_agg_serial_v2'>Goto 机器优先</a></div></div>")
        self.stdio.verbose("report SQL_PLAN_MONITOR SQC complete")
        cursor_sql_plan_monitor_svr_agg_v1 = self.sys_connector.execute_sql_return_cursor_dictionary(
            sql_plan_monitor_svr_agg_v2)
        if self.ob_major_version >= 4:
            self.report_svr_agg_graph_data_obversion4('svr_agg_serial_v1', cursor_sql_plan_monitor_svr_agg_v1, '算子优先视图')
        else:
            self.report_svr_agg_graph_data('svr_agg_serial_v1', cursor_sql_plan_monitor_svr_agg_v1, '算子优先视图')
        self.stdio.verbose("report SQL_PLAN_MONITOR SQC operator priority complete")
        cursor_data_sql_plan_monitor_svr_agg_v2 = self.sys_connector.execute_sql_return_cursor_dictionary(
            sql_plan_monitor_svr_agg_v2)
        if self.ob_major_version >= 4:
            self.report_svr_agg_graph_data('svr_agg_serial_v2', cursor_data_sql_plan_monitor_svr_agg_v2, '机器优先视图')
        else:
            self.report_svr_agg_graph_data('svr_agg_serial_v2', cursor_data_sql_plan_monitor_svr_agg_v2, '机器优先视图')
        self.stdio.verbose("report SQL_PLAN_MONITOR SQC server priority complete")

    def report_sql_plan_monitor_detail_operator_priority(self, sql):
        cursor_sql_plan_monitor_detail = self.sys_connector.execute_sql_return_cursor(sql)
        self.__report(
            "<div><h2 id='detail_table_anchor'>SQL_PLAN_MONITOR 详情</h2><div class='v' id='detail_table' style='display: none'>" + (
                "no result in --fast mode" if self.enable_fast_dump else from_db_cursor(
                    cursor_sql_plan_monitor_detail).get_html_string()) + "</div><div class='shortcut'><a href='#detail_serial_v1'>Goto 算子优先</a> <a href='#detail_serial_v2'>Goto 线程优先</a></div></div>")
        self.stdio.verbose("report SQL_PLAN_MONITOR details complete")
        cursor_sql_plan_monitor_detail_v1 = self.sys_connector.execute_sql_return_cursor_dictionary(sql)
        if self.ob_major_version >= 4:
            self.report_detail_graph_data_obversion4("detail_serial_v1",
                                                     cursor_sql_plan_monitor_detail_v1,
                                                     '算子优先视图')
        else:
            self.report_detail_graph_data("detail_serial_v1", cursor_sql_plan_monitor_detail_v1, '算子优先视图')
        self.stdio.verbose("report SQL_PLAN_MONITOR details operator priority complete")

    def reportsql_plan_monitor_detail_svr_priority(self, sql):
        cursor_sql_plan_monitor_detail_v2 = self.sys_connector.execute_sql_return_cursor_dictionary(sql)
        if self.ob_major_version >= 4:
            self.report_detail_graph_data_obversion4("detail_serial_v2",
                                                     cursor_sql_plan_monitor_detail_v2,
                                                     '线程优先视图')
        else:
            self.report_detail_graph_data("detail_serial_v2", cursor_sql_plan_monitor_detail_v2, '线程优先视图')
        self.stdio.verbose("report SQL_PLAN_MONITOR details server priority complete")
