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
@file: gather_plan_monitor_handler.py
@desc:
"""
import os
import shutil
import time
from decimal import Decimal

import MySQLdb
import tabulate
from prettytable import from_db_cursor

from common.logger import logger
from common.ob_connector import OBConnector
from common.obdiag_exception import OBDIAGInvalidArgs, OBDIAGArgsNotFoundException
from handler.base_sql_handler import BaseSQLHandler
from handler.sql_handler.html_meta import GlobalHtmlMeta
from handler.sql_handler.sql_meta import GlobalSqlMeta
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file
from utils.time_utils import timestamp_to_filename_time


class GatherPlanMonitorHandler(BaseSQLHandler):
    def __init__(self, ob_cluster, gather_pack_dir, gather_timestamp):
        super(GatherPlanMonitorHandler, self).__init__()
        self.ob_cluster = ob_cluster
        self.local_stored_path = gather_pack_dir
        self.gather_timestamp = gather_timestamp
        self.ob_cluster_name = ob_cluster["cluster_name"]
        self.tenant_mode = None
        self.sys_database = None
        self.database = None
        self.ob_connector = OBConnector(ip=ob_cluster["host"],
                                        port=ob_cluster["port"],
                                        username=ob_cluster["user"],
                                        password=ob_cluster["password"],
                                        timeout=100)
        self.enable_dump_db = False
        self.trace_id = None
        self.STAT_NAME = {}
        self.report_file_path = ""
        self.enable_fast_dump = False
        self.ob_major_version = None
        self.sql_audit_name = "gv$sql_audit"
        self.plan_explain_name = "gv$plan_cache_plan_explain"

    def handle(self, args):
        """
        the overall handler for the gather command
        :param args: command args
        :return: the summary should be displayed
        """
        if not self.__check_valid_and_parse_args(args):
            raise OBDIAGInvalidArgs("Invalid args, args={0}".format(args))
        pack_dir_this_command = os.path.join(self.local_stored_path, "gather_pack_{0}".format(
            timestamp_to_filename_time(self.gather_timestamp)))
        self.report_file_path = os.path.join(pack_dir_this_command, "sql_plan_monitor_report.html")
        logger.info("Use {0} as pack dir.".format(pack_dir_this_command))
        mkdir_if_not_exist(pack_dir_this_command)
        gather_tuples = []
        gather_pack_path_dict = {}

        def handle_plan_monitor_from_ob(cluster_name, args):
            """
            handler sql plan monitor from ob
            :param args: cluster_name, command args
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
                logger.info("TraceID : %s " % trace_id)
                logger.info("SQL : %s " % sql)
                logger.info("SVR_IP : %s " % svr_ip)
                logger.info("SVR_PORT : %s " % svr_port)
                logger.info("DB: %s " % db_name)
                logger.info("PLAN_ID: %s " % plan_id)
                logger.info("TENANT_ID: %s " % tenant_id)

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
                self.report_header()
                # 输出sql_audit的概要信息
                self.report_sql_audit()
                # 输出sql explain的信息
                self.report_plan_explain(db_name, sql)
                # 输出plan cache的信息
                self.report_plan_cache(plan_explain_sql)
                # 输出表结构的信息
                self.report_schema(user_sql)
                self.init_monitor_stat()
                # 输出sql_audit的详细信息
                self.report_sql_audit_details(full_audit_sql_by_trace_id_sql)
                # 输出算子信息 表+图
                self.report_sql_plan_monitor_dfo_op(sql_plan_monitor_dfo_op)
                # 输出算子信息按 svr 级汇总 表+图
                self.report_sql_plan_monitor_svr_agg(sql_plan_monitor_svr_agg_v1, sql_plan_monitor_svr_agg_v2)
                self.report_fast_preview()
                # 输出算子信息按算子维度聚集
                self.report_sql_plan_monitor_detail_operator_priority(sql_plan_monitor_detail_v1)
                # 输出算子信息按线程维度聚集
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
                logger.info("report footer complete")

            if resp["skip"]:
                return
            if resp["error"]:
                gather_tuples.append((cluster_name, True, resp["error_msg"], 0, int(time.time() - st),
                                      "Error:{0}".format(resp["error_msg"]), ""))
                return
            gather_pack_path_dict[cluster_name] = resp["gather_pack_path"]
            gather_tuples.append((cluster_name, False, "", int(time.time() - st), pack_dir_this_command))

        cs_resources_path = os.path.join(self.local_stored_path, "resources")
        target_resources_path = os.path.join(pack_dir_this_command, "resources")
        self.copy_cs_resource(cs_resources_path, target_resources_path)
        handle_plan_monitor_from_ob(self.ob_cluster_name, args)
        summary_tuples = self.__get_overall_summary(gather_tuples)
        print(summary_tuples)
        # 将汇总结果持久化记录到文件中
        write_result_append_to_file(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)
        return gather_tuples, gather_pack_path_dict

    def __check_valid_and_parse_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        if getattr(args, "trace_id") is not None:
            # 1: trace_id must be must be provided, if not be valid
            try:
                self.trace_id = getattr(args, "trace_id")
            except OBDIAGArgsNotFoundException:
                logger.error("Error: trace_id must be must be provided")
                return False
        else:
            return False
        self.tenant_mode_detected()
        return True

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
                        data = self.ob_connector.execute_sql("show create table %s" % t)
                        schemas = schemas + "<pre style='margin:20px;border:1px solid gray;'>%s</pre>" % (data[1])
                    except Exception as e:
                        pass
            cursor = self.ob_connector.execute_sql_return_cursor("show variables like '%parallel%'")
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
            logger.error("report table schema failed %s" % sql)
            logger.error(repr(e))
            pass

    def report_pre(self, s):
        pre = '''<pre style='margin:20px;border:1px solid gray;'>%s</pre>''' % s
        self.__report(pre)

    def report_header(self):
        header = GlobalHtmlMeta().get_value(key="sql_plan_monitor_report_header")
        with open(self.report_file_path, 'w') as f:
            f.write(header)
        logger.info("report header complete")

    def init_monitor_stat(self):
        sql = "select ID,NAME,TYPE from " + (
            "SYS." if self.tenant_mode == "oracle" else "oceanbase.") + "v$sql_monitor_statname order by ID"
        data = self.ob_connector.execute_sql(sql)
        for item in data:
            self.STAT_NAME[item[0]] = {"type": item[2], "name": item[1]}
        logger.info("init sql plan monitor stat complete")

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
        logger.debug("report SQL_PLAN_MONITOR SQC operator priority start, DATA: %s", data)
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
        logger.debug("report SQL_PLAN_MONITOR SQC operator priority start, DATA: %s", data)
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
        logger.info("report SQL_PLAN_MONITOR fast preview complete")

    def report_footer(self):
        footer = GlobalHtmlMeta().get_value(key="sql_plan_monitor_report_footer")
        self.__report(footer)

    def __report(self, s):
        with open(self.report_file_path, 'a') as f:
            f.write(s)

    def tenant_mode_detected(self):
        try:
            data = self.ob_connector.execute_sql("select version();")
            logger.info("Detected mySQL mode successful,  Database version : %s " % ("%s" % data[0]))
            v = "%s" % data
            if len(v.split('-')) > 1:
                v = v.split('-')[2][1:]
                version = v[0]
            else:
                version = v
            if int(version[0]) >= 4:
                self.sql_audit_name = "gv$ob_sql_audit"
                self.plan_explain_name = "gv$ob_plan_cache_plan_explain"
            else:
                self.sql_audit_name = "gv$sql_audit"
                self.plan_explain_name = "gv$plan_cache_plan_explain"
            self.ob_major_version = int(version[0])
            self.tenant_mode = "mysql"
            self.sys_database = "oceanbase"
        except:
            data = self.ob_connector.execute_sql("select SUBSTR(BANNER, 11, 100) from V$VERSION;")
            logger.info("Detectedo oracle mode successful,  Database version : %s " % ("%s" % data[0]))
            version = ("%s" % data[0])
            if int(version[0]) >= 4:
                self.sql_audit_name = "gv$ob_sql_audit"
            else:
                self.sql_audit_name = "gv$sql_audit"
            self.ob_major_version = int(version[0])
            self.tenant_mode = "oracle"
            self.sys_database = "SYS"

    def init_resp(self):
        """
        the handler for one ob cluster
        :param args: command args
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
                .replace("##REPLACE_TRACE_ID##", self.trace_id[0]).replace("##REPLACE_SQL_AUDIT_TABLE_NAME##",
                                                                           self.sql_audit_name)
        else:
            sql = str(GlobalSqlMeta().get_value(key="sql_audit_by_trace_id_limit1_oracle")).replace(
                "##REPLACE_TRACE_ID##", self.trace_id[0]).replace("##REPLACE_SQL_AUDIT_TABLE_NAME##",
                                                                  self.sql_audit_name)
        return sql

    def select_sql_audit_by_trace_id_limit1(self):
        sql = self.sql_audit_by_trace_id_limit1_sql()
        result = self.ob_connector.execute_sql(sql)
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
        if not self.enable_dump_db:
            full_audit_sql_result = self.ob_connector.execute_sql_pretty(sql)
            self.__report(
                "<div><h2 id='sql_audit_table_anchor'>SQL_AUDIT 信息</h2><div class='v' id='sql_audit_table' style='display: none'>" + full_audit_sql_result.get_html_string() + "</div></div>")
        logger.info("report full sql audit complete")

    # plan cache
    def report_plan_cache(self, sql):
        try:
            cursor_plan_explain = self.ob_connector.execute_sql_return_cursor(sql)
            logger.info("select plan_explain from ob complete")
            self.report_pre(sql)
            logger.info("report plan_explain_sql complete")

            data_plan_explain = from_db_cursor(cursor_plan_explain)
            data_plan_explain.align = 'l'
            self.report_pre(data_plan_explain)
            logger.info("report plan_explain complete")
        except Exception as e:
            logger.error("plan cache> %s" % sql)
            logger.error(repr(e))
            pass

    # sql_audit 概要
    def report_sql_audit(self):
        sql = self.sql_audit_by_trace_id_limit1_sql()
        logger.debug("select sql_audit from ob with SQL: %s", sql)
        try:
            sql_audit_result = self.ob_connector.execute_sql_pretty(sql)
            logger.debug("sql_audit_result: %s", sql_audit_result)
            logger.info("report sql_audit_result to file start ...")
            self.__report(sql_audit_result.get_html_string())
            logger.info("report sql_audit_result end")
        except Exception as e:
            logger.error("sql_audit> %s" % sql)
            logger.error(repr(e))

    def report_plan_explain(self, db_name, raw_sql):
        explain_sql = "explain %s" % raw_sql
        try:
            db = MySQLdb.connect(host=self.ob_cluster["host"], port=self.ob_cluster["port"],
                                 user=self.ob_cluster["user"], passwd=self.ob_cluster["password"], db=db_name)
            cursor = db.cursor()
            logger.debug("execute SQL: %s", explain_sql)
            cursor.execute(explain_sql)
            sql_explain_result_sql = "%s" % explain_sql
            sql_explain_result = cursor.fetchone()

            # output explain result
            logger.info("report sql_explain_result_sql complete")
            self.report_pre(sql_explain_result_sql)
            logger.info("report sql_explain_result_sql complete")
            self.report_pre(sql_explain_result)
            logger.info("report sql_explain_result complete")
        except Exception as e:
            logger.error("plan explain> %s" % explain_sql)
            logger.error(repr(e))
            pass

    def report_sql_plan_monitor_dfo_op(self, sql):
        data_sql_plan_monitor_dfo_op = self.ob_connector.execute_sql_pretty(sql)
        self.__report(
            "<div><h2 id='agg_table_anchor'>SQL_PLAN_MONITOR DFO 级调度时序汇总</h2><div class='v' id='agg_table' style='display: none'>" + data_sql_plan_monitor_dfo_op.get_html_string() + "</div></div>")
        logger.info("report SQL_PLAN_MONITOR DFO complete")
        cursor_sql_plan_monitor_dfo_op = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
        if self.ob_major_version >= 4:
            self.report_dfo_sched_agg_graph_data_obversion4(cursor_sql_plan_monitor_dfo_op, '调度时序图')
        else:
            self.report_dfo_sched_agg_graph_data(cursor_sql_plan_monitor_dfo_op, '调度时序图')
        logger.info("report SQL_PLAN_MONITOR DFO SCHED complete")
        cursor_sql_plan_monitor_dfo_op = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
        if self.ob_major_version >= 4:
            self.report_dfo_agg_graph_data_obversion4(cursor_sql_plan_monitor_dfo_op, '数据时序图')
        else:
            self.report_dfo_agg_graph_data(cursor_sql_plan_monitor_dfo_op, '数据时序图')
        logger.info("report SQL_PLAN_MONITOR DFO graph data complete")

    def report_sql_plan_monitor_svr_agg(self, sql_plan_monitor_svr_agg_v1, sql_plan_monitor_svr_agg_v2):
        cursor_sql_plan_monitor_svr_agg = self.ob_connector.execute_sql_return_cursor(sql_plan_monitor_svr_agg_v1)
        self.__report(
            "<div><h2 id='svr_agg_table_anchor'>SQL_PLAN_MONITOR SQC 级汇总</h2><div class='v' id='svr_agg_table' style='display: none'>" + from_db_cursor(
                cursor_sql_plan_monitor_svr_agg).get_html_string() + "</div><div class='shortcut'><a href='#svr_agg_serial_v1'>Goto 算子优先</a> <a href='#svr_agg_serial_v2'>Goto 机器优先</a></div></div>")
        logger.info("report SQL_PLAN_MONITOR SQC complete")
        cursor_sql_plan_monitor_svr_agg_v1 = self.ob_connector.execute_sql_return_cursor_dictionary(
            sql_plan_monitor_svr_agg_v2)
        if self.ob_major_version >= 4:
            self.report_svr_agg_graph_data_obversion4('svr_agg_serial_v1', cursor_sql_plan_monitor_svr_agg_v1, '算子优先视图')
        else:
            self.report_svr_agg_graph_data('svr_agg_serial_v1', cursor_sql_plan_monitor_svr_agg_v1, '算子优先视图')
        logger.info("report SQL_PLAN_MONITOR SQC operator priority complete")
        cursor_data_sql_plan_monitor_svr_agg_v2 = self.ob_connector.execute_sql_return_cursor_dictionary(
            sql_plan_monitor_svr_agg_v2)
        if self.ob_major_version >= 4:
            self.report_svr_agg_graph_data('svr_agg_serial_v2', cursor_data_sql_plan_monitor_svr_agg_v2, '机器优先视图')
        else:
            self.report_svr_agg_graph_data('svr_agg_serial_v2', cursor_data_sql_plan_monitor_svr_agg_v2, '机器优先视图')
        logger.info("report SQL_PLAN_MONITOR SQC server priority complete")

    def report_sql_plan_monitor_detail_operator_priority(self, sql):
        cursor_sql_plan_monitor_detail = self.ob_connector.execute_sql_return_cursor(sql)
        self.__report(
            "<div><h2 id='detail_table_anchor'>SQL_PLAN_MONITOR 详情</h2><div class='v' id='detail_table' style='display: none'>" + (
                "no result in --fast mode" if self.enable_fast_dump else from_db_cursor(
                    cursor_sql_plan_monitor_detail).get_html_string()) + "</div><div class='shortcut'><a href='#detail_serial_v1'>Goto 算子优先</a> <a href='#detail_serial_v2'>Goto 线程优先</a></div></div>")
        logger.info("report SQL_PLAN_MONITOR details complete")
        cursor_sql_plan_monitor_detail_v1 = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
        if self.ob_major_version >= 4:
            self.report_detail_graph_data_obversion4("detail_serial_v1",
                                                     cursor_sql_plan_monitor_detail_v1,
                                                     '算子优先视图')
        else:
            self.report_detail_graph_data("detail_serial_v1", cursor_sql_plan_monitor_detail_v1, '算子优先视图')
        logger.info("report SQL_PLAN_MONITOR details operator priority complete")

    def reportsql_plan_monitor_detail_svr_priority(self, sql):
        cursor_sql_plan_monitor_detail_v2 = self.ob_connector.execute_sql_return_cursor_dictionary(sql)
        if self.ob_major_version >= 4:
            self.report_detail_graph_data_obversion4("detail_serial_v2",
                                                     cursor_sql_plan_monitor_detail_v2,
                                                     '线程优先视图')
        else:
            self.report_detail_graph_data("detail_serial_v2", cursor_sql_plan_monitor_detail_v2, '线程优先视图')
        logger.info("report SQL_PLAN_MONITOR details server priority complete")
