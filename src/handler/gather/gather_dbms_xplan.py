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
@desc: Gather DBMS_XPLAN handler (Migrated to BaseHandler)
"""

import os
import time

from src.common.base_handler import BaseHandler
from src.common.result_type import ObdiagResult
from src.common.ob_connector import OBConnector
from src.common.tool import StringUtils
from src.common.command import get_observer_version
from src.common.tool import Util
from pathlib import Path
from src.common.tool import TimeUtils

# Removed tabulate import - now using BaseHandler._generate_summary_table
from src.common.command import get_file_size, download_file, delete_file_force
from src.common.ssh_client.ssh import SshClient
from src.common.tool import DirectoryUtil
from src.common.tool import NetUtils
from src.common.tool import FileUtil
from src.handler.meta.sql_meta import GlobalSqlMeta


class GatherDBMSXPLANHandler(BaseHandler):

    def _init(self, store_dir="./", is_scene=False, is_inner=False, **kwargs):
        """Subclass initialization"""
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

        # Initialize options and config in _init
        self.store_dir = os.path.join(self.store_dir, "obdiag_gather_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))

        trace_id = self._get_option('trace_id')
        user = self._get_option('user')
        password = self._get_option('password') or ""
        store_dir_option = self._get_option('store_dir')
        scope_option = self._get_option('scope')

        valid_scopes = ['all', 'opt_trace', 'display_cursor']
        if scope_option:
            if scope_option not in valid_scopes:
                error_msg = f"invalid --scope option: '{scope_option}'. Valid options are: {', '.join(valid_scopes)}. Setting default --scope=all."
                self._log_warn(error_msg)
                self.scope = "all"
            else:
                self.scope = scope_option

        if store_dir_option is not None and store_dir_option != './':
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self._log_warn(f'args --store_dir [{os.path.abspath(store_dir_option)}]: No such directory, Now create it')
                os.makedirs(os.path.abspath(store_dir_option))
                self.store_dir = os.path.abspath(store_dir_option)
            else:
                self.store_dir = os.path.abspath(store_dir_option)

        if self.context.get_variable("gather_trace_id", None):
            trace_id = self.context.get_variable("gather_trace_id")
            if not self.context.get_variable("gather_user"):
                raise ValueError("gather_user not found in context variables")
            user = self.context.get_variable("gather_user")

        if self.context.get_variable("gather_password", None):
            password = self.context.get_variable("gather_password")

        if self.context.get_variable("store_dir", None):
            self.store_dir = self.context.get_variable("store_dir")

        if self.context.get_variable("dbms_xplan_scope", None):
            self.scope = self.context.get_variable("dbms_xplan_scope")

        if not trace_id:
            raise ValueError("option --trace_id not found, please provide")

        if not user:
            raise ValueError("option --user not found, please provide")

        env_option = self._get_option('env')
        if env_option:
            self.env = env_option

        self.tenant_password = password
        self.tenant_user = user
        self.trace_id = trace_id

        # Initialize config
        self.ob_cluster = self.context.cluster_config
        self.ob_nodes = self.context.cluster_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.ob_nodes, self.stdio)
        if new_nodes:
            self.ob_nodes = new_nodes

        # Use ConfigAccessor if available (file_number_limit is from BaseHandler @property)
        if self.config:
            self._file_size_limit = self.config.gather_file_size_limit
            self.config_path = self.config.basic_config_path
        else:
            # Fallback to direct config access
            if self.context.inner_config is None:
                self._file_size_limit = 2 * 1024 * 1024 * 1024
            else:
                basic_config = self.context.inner_config['obdiag']['basic']
                self._file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
                self.config_path = basic_config['config_path']

        self.ob_connector = OBConnector(
            context=self.context, ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.ob_cluster.get("tenant_sys").get("user"), password=self.ob_cluster.get("tenant_sys").get("password"), timeout=100
        )

        if self.is_scene:
            self.__init_db_conn(self.env)
        else:
            self.tenant_connector = OBConnector(context=self.context, ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.tenant_user, password=self.tenant_password, timeout=100, database=self.db_name)

        self.file_name = f"{self.store_dir}/obdiag_dbms_xplan_display_cursor.txt"

    def handle(self) -> ObdiagResult:
        """Main handle logic"""
        self._validate_initialized()

        try:
            self.start_time = time.time()
            excute_status = self.execute()

            # recycle *_obdiag_*.trac in observer log dir
            for node in self.ob_nodes:
                try:
                    log_path = os.path.join(node.get("home_path"), "log")
                    ssh_client = SshClient(self.context, node)
                    self._log_verbose(f"node: {ssh_client.get_name()}. recycle *_obdiag_*.trac in observer log dir. obdiag will clean all '*_obdiag_*.trac'")
                    ssh_client.exec_cmd(f"find {log_path} -type f -name '*_obdiag_*.trac' -exec rm -f {{}} +")
                except Exception as e:
                    self._log_warn(f"node: {node.get('ip')}. recycle *_obdiag_*.trac in observer log dir failed: {e}")

            if not self.is_innner and excute_status:
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": self.store_dir})
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="execute failed")

        except Exception as e:
            return self._handle_error(e)

    def execute(self):
        try:
            self.version = get_observer_version(self.context)
            if StringUtils.compare_versions_lower(self.version, "4.2.5.0"):
                self._log_error(f"DBMS_XPLAN feature requires OceanBase version >= 4.2.5.0. Current version: {self.version} is not supported.")
                return False
            raw_query_sql = self.__get_sql_audit_from_trace_id()
            if raw_query_sql:
                self.tenant_connector = OBConnector(context=self.context, ip=self.ob_cluster.get("db_host"), port=self.ob_cluster.get("db_port"), username=self.tenant_user, password=self.tenant_password, timeout=100, database=self.db_name)
                if self.scope in ['all', 'opt_trace']:
                    self.get_opt_trace()
                if self.scope in ['all', 'display_cursor']:
                    self.get_display_cursor()
                return True
            else:
                self._log_error(f"The data queried with the specified trace_id {self.trace_id} from gv$ob_sql_audit is empty. Please verify if this trace_id has expired.")
                return False
        except Exception as e:
            self._log_error(f"get dbms_xplan result failed, error: {e}")
            return False

    def get_display_cursor(self):
        result = ''
        display_cursor_sql = "SELECT DBMS_XPLAN.DISPLAY_CURSOR({plan_id}, 'all', '{svr_ip}',  {svr_port}, {tenant_id}) FROM DUAL".format(plan_id=self.plan_id, svr_ip=self.svr_ip, svr_port=self.svr_port, tenant_id=self.tenant_id)
        try:
            if not StringUtils.compare_versions_lower(self.version, "4.2.5.0"):
                self._log_verbose(f"execute SQL: {display_cursor_sql}")
                plan_result = self.tenant_connector.execute_sql_pretty(display_cursor_sql)
                if plan_result:
                    plan_result.align = 'l'
                    result = f'obclient> {display_cursor_sql}\n{str(plan_result)}'
                    self._log_verbose("dbms_xplan.display_cursor report complete")
                    self.__report(result)
                    self.__print_display_cursor_result()
                else:
                    self._log_warn("the result of dbms_xplan.display_cursor is None")
            else:
                self._log_warn(f"dbms_xplan.display_cursor report requires the OB version to be greater than 4.2.5.0 Your version: {self.version} does not meet this requirement.")
        except Exception as e:
            self.stdio.exception(f"dbms_xplan.display_cursor report> {display_cursor_sql}")
            self.stdio.exception(repr(e))
            pass

    def get_opt_trace(self):
        self._log_verbose(f"Use {self.store_dir} as pack dir.")
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
                        self._log_warn(f"Skip gather from node {node.get('ip')} because it is a docker or kubernetes node")
                        continue
                    handle_from_node(node)
                    if self.retry_opt_trace:
                        self.skip_gather = False
                        self.gather_tuples = []
                        self._log_warn("failed to gather dbms_xplan.enable_opt_trace, wait to retry")
                        raise
            except Exception as e:
                raise e

        is_ready()
        summary_tuples = self.__get_overall_summary(self.gather_tuples)
        self._log_info(summary_tuples)

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
            self._log_verbose(f"Sending Collect Shell Command to node {remote_ip} ...")
            DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)
            ssh_failed = False
            ssh_client = None
            try:
                ssh_client = SshClient(self.context, node)
            except Exception as e:
                self.stdio.exception(f"ssh {remote_user}@{remote_ip}: failed, Please check the node conf.")
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
                    if int(file_size) < self._file_size_limit:
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
                    self._log_verbose(f"execute SQL: {sql}")
                    step = f"obclient> SET TRANSACTION ISOLATION LEVEL READ COMMITTED;\ncall dbms_xplan.enable_opt_trace();\ncall dbms_xplan.set_opt_trace_parameter(identifier=>'{self.opt_trace_file_suffix}', `level`=>3);\nexplain {sql}\ncall dbms_xplan.disable_opt_trace();\n"
                    self.__report(step)
                    self._log_verbose("get dbms_xplan.enable_opt_trace complete")
                    return error_info
                else:
                    error_info = "failed generat opt_trace: the input SQL is empty"
                    self._log_error(error_info)
                    return error_info
            else:
                error_info = f"dbms_xplan.enable_opt_trace requires the OB version to be greater than 4.2.5.0 Your version: {self.version} does not meet this requirement."
                self._log_warn(error_info)
                return error_info
        except Exception as e:
            error_info = f"dbms_xplan.enable_opt_trace generate failed > {e}"
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
            self._log_error(f"report result to file: {self.file_name} failed, error: {e}")

    def __print_display_cursor_result(self):
        self.end_time = time.time()
        elapsed_time = self.end_time - self.start_time
        data = [["Status", "Result Details", "Time"], ["Completed", self.file_name, f"{elapsed_time:.2f} s"]]
        table = tabulate(data, headers="firstrow", tablefmt="grid")
        self._log_info("\nGather dbms_xplan.display_cursor:")
        self._log_info(table)
        self._log_info("\n")
        return

    def __init_db_conn(self, env):
        try:
            # env must be a list from parse_env_display (action="append")
            if not isinstance(env, list):
                self._log_error("Invalid env format. Please use --env key=value format, e.g., --env host=127.0.0.1 --env port=2881 --env user=test --env password=****** --env database=test")
                return False

            env_dict = StringUtils.parse_env_display(env)
            self.env = env_dict

            # Build db_info directly from env_dict parameters (no db_connect string parsing)
            self.db_conn = StringUtils.build_db_info_from_env(env_dict, self.stdio)
            if not self.db_conn:
                self._log_error("Failed to build database connection information from env parameters")
                return False

            if StringUtils.validate_db_info(self.db_conn):
                self.__init_tenant_connector()
                return True
            else:
                self._log_error("db connection information required: --env host=... --env port=... --env user=... --env password=... --env database=...")
                return False
        except Exception as e:
            self.stdio.exception(f"init db connector, error: {e}, please check --env option")

    def __init_tenant_connector(self):
        self.tenant_connector = OBConnector(
            context=self.context, ip=self.db_conn.get("host"), port=self.db_conn.get("port"), username=self.db_conn.get("user"), password=self.db_conn.get("password") or "", database=self.db_conn.get("database"), timeout=100
        )

    def __build_find_latest_log_cmd(self, log_path, suffix):
        return f"find \"{log_path}\" -type f -name \"*{suffix}.trac\" "

    def __get_overall_summary(self, node_summary_tuple):
        """
        Generate overall summary from gather tuples using BaseHandler template method.
        :param node_summary_tuple: List of tuples (node, is_err, error_msg, file_size, consume_time, pack_path)
        :return: Formatted summary table string
        """
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
            except Exception as e:
                self._log_verbose("Failed to format file size {0}: {1}".format(file_size, e))
                format_file_size = FileUtil.size_format(num=0, output_str=True)
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        # Use BaseHandler template method
        return self._generate_summary_table(field_names, summary_tab, "Gather dbms_xplan.enable_opt_trace")
