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
@time: 2025/04/28
@file: gather_dbms_xplan.py
@desc:
"""

import os
import time

from src.common.result_type import ObdiagResult
from src.common.stdio import SafeStdio
from src.common.ob_connector import OBConnector
from src.common.tool import StringUtils
from src.common.command import get_observer_version
from src.common.tool import Util
from pathlib import Path
from src.common.tool import TimeUtils
from tabulate import tabulate
from src.common.command import get_file_size, download_file, delete_file_force
from src.common.command import SshClient
from src.common.tool import DirectoryUtil
from src.common.tool import NetUtils
from src.common.result_type import ObdiagResult
from src.common.tool import FileUtil
from src.handler.meta.sql_meta import GlobalSqlMeta


class GatherDBMSXPLANHandler(SafeStdio):

    def __init__(self, context, store_dir="./", is_scene=False, is_inner=False):
        self.context = context
        self.stdio = context.stdio
        self.is_ssh = True
        self.ob_cluster = {}
        self.ob_connector = None
        self.tenant_connector = None
        self.store_dir = store_dir
        self.raw_query_sql = None
        self.plan_id = None
        self.tenant_id = None
        self.svr_ip = None
        self.svr_port = None
        self.is_innner = is_inner
        self.is_scene = is_scene
        self.tenant_user = None
        self.tenant_password = None
        self.db_name = None
        self.scope = "all"
        self.env = None
        self.skip_gather = False
        self.retry_opt_trace = True
        self.opt_trace_file_suffix = "obdiag_" + StringUtils.generate_alphanum_code(6)
        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init(self):
        try:
            self.store_dir = os.path.join(self.store_dir, "obdiag_gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
            options = self.context.options
            self.trace_id = Util.get_option(options, 'trace_id')
            user = Util.get_option(options, 'user')
            password = Util.get_option(options, 'password') or ""
            store_dir_option = Util.get_option(options, 'store_dir')
            self.scope_option = Util.get_option(options, 'scope')
            valid_scopes = ['all', 'opt_trace', 'display_cursor']
            if self.scope_option:
                if self.scope_option not in valid_scopes:
                    error_msg = f"invalid --scope option: '{self.scope_option}'. Valid options are: {', '.join(valid_scopes)}. Setting default --scope=all."
                    self.stdio.warn(error_msg)
                    self.scope = "all"
                else:
                    self.scope = self.scope_option
            if store_dir_option is not None and store_dir_option != './':
                if not os.path.exists(os.path.abspath(store_dir_option)):
                    self.stdio.warn('args --store_dir [{0}]: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                    os.makedirs(os.path.abspath(store_dir_option))
                    self.store_dir = os.path.abspath(store_dir_option)
                else:
                    self.store_dir = os.path.abspath(store_dir_option)
            if self.context.get_variable("gather_trace_id", None):
                self.trace_id = self.context.get_variable("gather_trace_id")
                if not self.context.get_variable("gather_user"):
                    self.stdio.error("The data queried with the specified trace_id {0} from gv$ob_sql_audit is empty. Please verify if this trace_id has expired.".format(self.trace_id))
                    return False
                user = self.context.get_variable("gather_user")
            if self.context.get_variable("gather_password", None):
                password = self.context.get_variable("gather_password")
            if self.context.get_variable("store_dir", None):
                self.store_dir = self.context.get_variable("store_dir")
            if self.context.get_variable("dbms_xplan_scope", None):
                self.scope = self.context.get_variable("dbms_xplan_scope")
            if not self.trace_id:
                self.stdio.error("option --trace_id not found, please provide")
                return False
            if not user:
                self.stdio.error("option --user not found, please provide")
                return False
            env_option = Util.get_option(options, 'env')
            if env_option:
                self.env = env_option
            self.tenant_password = password
            self.tenant_user = user
            return True
        except Exception as e:
            self.stdio.error(e)
            return False

    def init_config(self):
        self.ob_cluster = self.context.cluster_config
        self.ob_nodes = self.context.cluster_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.ob_nodes, self.stdio)
        if new_nodes:
            self.ob_nodes = new_nodes
        self.inner_config = self.context.inner_config
        if self.inner_config is None:
            self.file_number_limit = 20
            self.file_size_limit = 2 * 1024 * 1024 * 1024
        else:
            basic_config = self.inner_config['obdiag']['basic']
            self.file_number_limit = int(basic_config["file_number_limit"])
            self.file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
            self.config_path = basic_config['config_path']
        self.ob_connector = OBConnector(
            context=self.context, ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.ob_cluster.get("tenant_sys").get("user"), password=self.ob_cluster.get("tenant_sys").get("password"), timeout=100
        )
        if self.is_scene:
            self.__init_db_conn(self.env)
        else:
            self.tenant_connector = OBConnector(context=self.context, ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.tenant_user, password=self.tenant_password, timeout=100, database=self.db_name)
        self.file_name = "{0}/obdiag_dbms_xplan_display_cursor.txt".format(self.store_dir)
        return True

    def handle(self):
        self.start_time = time.time()
        if not self.init():
            self.stdio.error('init failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init failed")
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init config failed")
        excute_status = self.execute()
        # recycle *_obdiag_*.trac in observer log dir
        for node in self.ob_nodes:
            try:
                log_path = os.path.join(node.get("home_path"), "log")
                ssh_client = SshClient(self.context, node)
                self.stdio.verbose("node: {}. recycle *_obdiag_*.trac in observer log dir. obdiag will clean all '*_obdiag_*.trac'".format(ssh_client.get_name()))
                ssh_client.exec_cmd(f"find {log_path} -type f -name '*_obdiag_*.trac' -exec rm -f {{}} +")
            except Exception as e:
                self.stdio.warn("node: {}. recycle *_obdiag_*.trac in observer log dir failed: {}".format(node.get("ip"), e))
                pass

        if not self.is_innner and excute_status:
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.store_dir})
        return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="execute failed")

    def execute(self):
        try:
            self.version = get_observer_version(self.context)
            raw_query_sql = self.__get_sql_audit_from_trace_id()
            if raw_query_sql:
                self.tenant_connector = OBConnector(context=self.context, ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.tenant_user, password=self.tenant_password, timeout=100, database=self.db_name)
                if self.scope in ['all', 'opt_trace']:
                    self.get_opt_trace()
                if self.scope in ['all', 'display_cursor']:
                    self.get_display_cursor()
                return True
            else:
                self.stdio.error("The data queried with the specified trace_id {0} from gv$ob_sql_audit is empty. Please verify if this trace_id has expired.".format(self.trace_id))
                return False
        except Exception as e:
            self.stdio.error("get dbms_xplan result failed, error: {0}".format(e))
            return False

    def get_display_cursor(self):
        result = ''
        display_cursor_sql = "SELECT DBMS_XPLAN.DISPLAY_CURSOR({plan_id}, 'all', '{svr_ip}',  {svr_port}, {tenant_id}) FROM DUAL".format(plan_id=self.plan_id, svr_ip=self.svr_ip, svr_port=self.svr_port, tenant_id=self.tenant_id)
        try:
            if not StringUtils.compare_versions_lower(self.version, "4.2.5.0"):
                self.stdio.verbose("execute SQL: %s", display_cursor_sql)
                plan_result = self.tenant_connector.execute_sql_pretty(display_cursor_sql)
                if plan_result:
                    plan_result.align = 'l'
                    result = 'obclient> ' + display_cursor_sql + '\n' + str(plan_result)
                    self.stdio.verbose("dbms_xplan.display_cursor report complete")
                    self.__report(result)
                    self.__print_display_cursor_result()
                else:
                    self.stdio.warn("the result of dbms_xplan.display_cursor is None")
            else:
                self.stdio.warn("dbms_xplan.display_cursor report requires the OB version to be greater than 4.2.5.0 Your version: {0} does not meet this requirement.".format(self.version))
        except Exception as e:
            self.stdio.exception("dbms_xplan.display_cursor report> %s" % sql)
            self.stdio.exception(repr(e))
            pass

    def get_opt_trace(self):
        self.stdio.verbose("Use {0} as pack dir.".format(self.store_dir))
        self.gather_tuples = []

        def handle_from_node(node):
            st = time.time()
            resp = self.__handle_from_node(node, self.store_dir)
            file_size = ""
            if len(resp["error"]) == 0:
                file_size = os.path.getsize(resp["gather_pack_path"])
            self.gather_tuples.append((node.get("ip"), False, resp["error"], file_size, int(time.time() - st), resp["gather_pack_path"]))

        @Util.retry(8, 2)
        def is_ready():
            try:
                for node in self.ob_nodes:
                    if node.get("ssh_type") == "docker" or node.get("ssh_type") == "kubernetes":
                        self.stdio.warn("Skip gather from node {0} because it is a docker or kubernetes node".format(node.get("ip")))
                        continue
                    handle_from_node(node)
                    if self.retry_opt_trace:
                        self.skip_gather = False
                        self.gather_tuples = []
                        self.stdio.warn("failed to gather dbms_xplan.enable_opt_trace, wait to retry")
                        raise
            except Exception as e:
                raise e

        is_ready()
        summary_tuples = self.__get_overall_summary(self.gather_tuples)
        self.stdio.print(summary_tuples)

    def __get_sql_audit_from_trace_id(self):
        sql = str(GlobalSqlMeta().get_value(key="sql_audit_by_trace_id_limit1_mysql")).replace("##REPLACE_TRACE_ID##", self.trace_id).replace("##REPLACE_SQL_AUDIT_TABLE_NAME##", "gv$ob_sql_audit").replace("##OB_VERSION_PARAMS_VALUE##", "params_value")
        audit_result = self.ob_connector.execute_sql(sql)
        if len(audit_result) > 0:
            trace = audit_result[0]
            raw_sql = trace[1]
            db_name = trace[8]
            plan_id = trace[9]
            tenant_id = trace[10]
            svr_ip = trace[12]
            svr_port = trace[13]
            self.raw_query_sql = raw_sql
            self.db_name = db_name
            self.plan_id = plan_id
            self.tenant_id = tenant_id
            self.svr_ip = svr_ip
            self.svr_port = svr_port
            return raw_sql
        else:
            return

    def __handle_from_node(self, node, local_stored_path):
        resp = {"skip": False, "error": "", "gather_pack_path": ""}
        error_info = ''
        if not self.skip_gather:
            error_info = self.__generate_opt_trace(self.raw_query_sql)
            self.skip_gather = True
        if len(error_info) == 0:
            remote_ip = node.get("ip") if self.is_ssh else NetUtils.get_inner_ip(self.stdio)
            remote_user = node.get("ssh_username")
            self.stdio.verbose("Sending Collect Shell Command to node {0} ...".format(remote_ip))
            DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)
            ssh_failed = False
            ssh_client = None
            try:
                ssh_client = SshClient(self.context, node)
            except Exception as e:
                self.stdio.exception("ssh {0}@{1}: failed, Please check the node conf.".format(remote_user, remote_ip))
                ssh_failed = True
                resp["skip"] = True
                resp["error"] = "Please check the node conf."
                return resp
            if not ssh_failed:
                home_path = node.get("home_path")
                log_path = os.path.join(home_path, "log")
                get_remote_file_full_path_cmd = self.__build_find_latest_log_cmd(log_path, self.opt_trace_file_suffix)
                remote_file_full_path_res = ssh_client.exec_cmd(get_remote_file_full_path_cmd)
                remote_file_full_path = next((line for line in remote_file_full_path_res.splitlines() if line.strip()), None)
                if remote_file_full_path:
                    self.retry_opt_trace = False
                    file_size = get_file_size(ssh_client, remote_file_full_path, self.stdio)
                    if int(file_size) < self.file_size_limit:
                        local_file_path = "{0}/{1}".format(local_stored_path, remote_ip.replace('.', '_') + '_' + Path(remote_file_full_path).name)
                        download_file(ssh_client, remote_file_full_path, local_file_path, self.stdio)
                        resp["error"] = ""
                    else:
                        resp["error"] = "File too large"
                    delete_file_force(ssh_client, remote_file_full_path, self.stdio)
                    resp["gather_pack_path"] = "{0}".format(local_file_path)
                else:
                    resp["error"] = "command: {0} execution on Node {1} failed: file not found.".format(get_remote_file_full_path_cmd, remote_ip)

        else:
            resp["skip"] = True
            resp["error"] = error_info
        return resp

    def __generate_opt_trace(self, sql):
        try:
            error_info = ""
            if not StringUtils.compare_versions_lower(self.version, "4.2.5.0"):
                if sql:

                    self.tenant_connector.execute_enable_opt_trace(self.opt_trace_file_suffix, sql)
                    self.stdio.verbose("execute SQL: %s", sql)
                    step = (
                        "obclient> SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\ncall dbms_xplan.enable_opt_trace();\ncall dbms_xplan.set_opt_trace_parameter(identifier=>'{0}', `level`=>3);\nexplain {1}\ncall dbms_xplan.disable_opt_trace();\n".format(
                            self.opt_trace_file_suffix, sql
                        )
                    )
                    self.__report(step)
                    self.stdio.verbose("get dbms_xplan.enable_opt_trace complete")
                    return error_info
                else:
                    error_info = "failed generat opt_trace: the input SQL is empty"
                    self.stdio.error(error_info)
                    return error_info
            else:
                error_info = "dbms_xplan.enable_opt_trace requires the OB version to be greater than 4.2.5.0 Your version: {0} does not meet this requirement.".format(self.version)
                self.stdio.warn(error_info)
                return error_info
        except Exception as e:
            error_info = "dbms_xplan.enable_opt_trace generate failed > %s" % e
            self.stdio.exception(error_info)
            return error_info

    def __report(self, data):
        try:
            if not isinstance(data, str):
                data = str(data)
            os.makedirs(os.path.dirname(self.file_name), exist_ok=True)

            with open(self.file_name, 'a', encoding='utf-8') as f:
                f.write(data + '\n')
        except Exception as e:
            self.stdio.error("report result to file: {0} failed, error: {1}".format(self.file_name, e))

    def __print_display_cursor_result(self):
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        data = [["Status", "Result Details", "Time"], ["Completed", self.file_name, f"{elapsed_time:.2f} s"]]
        table = tabulate(data, headers="firstrow", tablefmt="grid")
        self.stdio.print("\nGather dbms_xplan.display_cursor:")
        self.stdio.print(table)
        self.stdio.print("\n")
        return

    def __init_db_conn(self, env):
        try:
            env_dict = StringUtils.parse_env(env)
            self.env = env_dict
            cli_connection_string = self.env.get("db_connect")
            self.db_conn = StringUtils.parse_mysql_conn(cli_connection_string)
            if StringUtils.validate_db_info(self.db_conn):
                self.__init_tenant_connector()
                return True
            else:
                self.stdio.error("db connection information requird [db_connect = '-hxx -Pxx -uxx -pxx -Dxx'],  but provided {0}, please check the --env option".format(env_dict))
                return False
        except Exception as e:
            self.stdio.exception("init db connector, error: {0}, please check --env option ".format(e))

    def __init_tenant_connector(self):
        self.tenant_connector = OBConnector(
            context=self.context, ip=self.db_conn.get("host"), port=self.db_conn.get("port"), username=self.db_conn.get("user"), password=self.db_conn.get("password") or "", database=self.db_conn.get("database"), timeout=100
        )

    def __build_find_latest_log_cmd(self, log_path, suffix):
        return f"find \"{log_path}\" -type f -name \"*{suffix}.trac\" "

    @staticmethod
    def __get_overall_summary(node_summary_tuple):
        summary_tab = []
        field_names = ["Node", "Status", "Size", "Time", "PackPath"]
        for tup in node_summary_tuple:
            node = tup[0]
            is_err = tup[1]
            file_size = tup[3]
            consume_time = tup[4]
            pack_path = tup[5]
            try:
                format_file_size = FileUtil.size_format(num=file_size, output_str=True)
            except:
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather dbms_xplan.enable_opt_trace:\n" + tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
