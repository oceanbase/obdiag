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
@file: core.py
@desc:
"""

from __future__ import absolute_import, division, print_function

import os
import traceback
from optparse import Values
from copy import copy

from src.common.ssh_client.remote_client import dis_rsa_algorithms
from src.handler.gather.gather_ash_report import GatherAshReportHandler
from src.handler.gather.gather_component_log import GatherComponentLogHandler
from src.handler.rca.rca_handler import RCAHandler
from src.handler.rca.rca_list import RcaScenesListHandler
from src.common.ssh import SshClient, SshConfig
from src.common.context import HandlerContextNamespace, HandlerContext
from src.common.config import ConfigManager, InnerConfigManager
from src.common.err import CheckStatus, SUG_SSH_FAILED
from src.handler.analyzer.analyze_flt_trace import AnalyzeFltTraceHandler
from src.handler.analyzer.analyze_log import AnalyzeLogHandler
from src.handler.analyzer.analyze_sql import AnalyzeSQLHandler
from src.handler.analyzer.analyze_sql_review import AnalyzeSQLReviewHandler
from src.handler.analyzer.analyze_parameter import AnalyzeParameterHandler
from src.handler.analyzer.analyze_variable import AnalyzeVariableHandler
from src.handler.analyzer.analyze_memory import AnalyzeMemoryHandler
from src.handler.analyzer.analyze_index_space import AnalyzeIndexSpaceHandler
from src.handler.checker.check_handler import CheckHandler
from src.handler.checker.check_list import CheckListHandler
from src.handler.gather.gather_awr import GatherAwrHandler
from src.handler.gather.gather_sysstat import GatherOsInfoHandler
from src.handler.gather.gather_obstack2 import GatherObstack2Handler
from src.handler.gather.gather_obadmin import GatherObAdminHandler
from src.handler.gather.gather_perf import GatherPerfHandler
from src.handler.gather.gather_plan_monitor import GatherPlanMonitorHandler
from src.handler.gather.gather_scenes import GatherSceneHandler
from src.handler.gather.scenes.list import GatherScenesListHandler
from src.handler.gather.gather_tabledump import GatherTableDumpHandler
from src.handler.gather.gather_parameters import GatherParametersHandler
from src.handler.gather.gather_variables import GatherVariablesHandler
from src.handler.gather.gather_dbms_xplan import GatherDBMSXPLANHandler
from src.handler.display.display_scenes import DisplaySceneHandler
from src.handler.display.scenes.list import DisplayScenesListHandler
from src.handler.tools.crypto_config_handler import CryptoConfigHandler
from src.handler.update.update import UpdateHandler
from src.common.result_type import ObdiagResult
from src.telemetry.telemetry import telemetry
from colorama import Fore, Style
from src.common.config_helper import ConfigHelper
from src.handler.analyzer.analyze_queue import AnalyzeQueueHandler
from src.common.tool import TimeUtils, Util
from src.common.command import get_observer_version_by_sql
from src.common.ob_connector import OBConnector


class ObdiagHome(object):

    def __init__(self, stdio=None, config_path=os.path.expanduser('~/.obdiag/config.yml'), inner_config_change_map=None, custom_config_env_list=None,config_password=None):
        self._optimize_manager = None
        self.stdio = None
        self._stdio_func = None
        self.cmds = []
        self.options = Values()
        self.namespaces = {}
        self.set_stdio(stdio)
        self.context = None
        self.inner_config_manager = InnerConfigManager(stdio=stdio, inner_config_change_map=inner_config_change_map)
        # obdiag.logger.error_stream
        if self.inner_config_manager.config.get("obdiag") is not None and self.inner_config_manager.config.get("obdiag").get("basic") is not None and self.inner_config_manager.config.get("obdiag").get("basic").get("print_type") is not None:
            stdio.set_err_stream(self.inner_config_manager.config.get("obdiag").get("logger").get("error_stream"))
        # obdiag.logger.silent
        if self.inner_config_manager.config.get("obdiag") is not None and self.inner_config_manager.config.get("obdiag").get("logger") is not None and self.inner_config_manager.config.get("obdiag").get("logger").get("silent") is not None:
            stdio.set_silent(self.inner_config_manager.config.get("obdiag").get("logger").get("silent"))
        self.set_stdio(stdio)
        self.config_manager = ConfigManager(config_path, stdio, custom_config_env_list,config_password=config_password)
        if (
            self.inner_config_manager.config.get("obdiag") is not None
            and self.inner_config_manager.config.get("obdiag").get("basic") is not None
            and self.inner_config_manager.config.get("obdiag").get("basic").get("telemetry") is not None
            and (self.inner_config_manager.config.get("obdiag").get("basic").get("telemetry") is False or "false" in self.inner_config_manager.config.get("obdiag").get("basic").get("telemetry").lower())
        ):
            telemetry.work_tag = False
        if self.inner_config_manager.config.get("obdiag") is not None and self.inner_config_manager.config.get("obdiag").get("basic") is not None and self.inner_config_manager.config.get("obdiag").get("basic").get("dis_rsa_algorithms") is not None:
            disable_rsa_algorithms = self.inner_config_manager.config.get("obdiag").get("basic").get("dis_rsa_algorithms")
            dis_rsa_algorithms(disable_rsa_algorithms)

    def fork(self, cmds=None, options=None, stdio=None):
        new_obdiag = copy(self)
        if cmds:
            new_obdiag.set_cmds(cmds)
        if options:
            new_obdiag.set_options(options)
        if stdio:
            new_obdiag.set_stdio(stdio)
        return new_obdiag

    def set_cmds(self, cmds):
        self.cmds = cmds

    def set_options(self, options):
        self.options = options

    def set_stdio(self, stdio):
        def _print(msg, *arg, **kwarg):
            sep = kwarg['sep'] if 'sep' in kwarg else None
            end = kwarg['end'] if 'end' in kwarg else None
            return print(msg, sep='' if sep is None else sep, end='\n' if end is None else end)

        self.stdio = stdio
        self._stdio_func = {}
        if not self.stdio:
            return
        for func in ['start_loading', 'stop_loading', 'print', 'confirm', 'verbose', 'warn', 'exception', 'error', 'critical', 'print_list', 'read']:
            self._stdio_func[func] = getattr(self.stdio, func, _print)

    def set_context_stdio(self):
        self.context = HandlerContext(stdio=self.stdio)

    def set_context(self, handler_name, namespace, config):
        self.context = HandlerContext(
            handler_name=handler_name,
            namespace=namespace,
            cluster_config=config.get_ob_cluster_config,
            obproxy_config=config.get_obproxy_config,
            ocp_config=config.get_ocp_config,
            oms_config=config.get_oms_config,
            cmd=self.cmds,
            options=self.options,
            stdio=self.stdio,
            inner_config=self.inner_config_manager.config,
        )
        telemetry.set_cluster_conn(self.context, config.get_ob_cluster_config)

    def set_context_skip_cluster_conn(self, handler_name, namespace, config):
        self.context = HandlerContext(
            handler_name=handler_name,
            namespace=namespace,
            cluster_config=config.get_ob_cluster_config,
            obproxy_config=config.get_obproxy_config,
            ocp_config=config.get_ocp_config,
            oms_config=config.get_oms_config,
            cmd=self.cmds,
            options=self.options,
            stdio=self.stdio,
            inner_config=self.inner_config_manager.config,
        )

    def set_offline_context(self, handler_name, namespace):
        self.context = HandlerContext(handler_name=handler_name, namespace=namespace, cmd=self.cmds, options=self.options, stdio=self.stdio, inner_config=self.inner_config_manager.config)

    def update_obcluster_nodes(self, config):
        config_data = config.config_data
        cluster_config = config.config_data.get("obcluster", {})

        lst = Util.get_option(self.options, 'config')
        if lst and any(item.startswith('obcluster.servers.nodes') for item in lst):
            self.stdio.verbose("You have already provided node information, so there is no need to query node information from the sys tenant")
            return

        ob_cluster = {"db_host": cluster_config.get("db_host"), "db_port": cluster_config.get("db_port"), "tenant_sys": {"user": cluster_config.get("tenant_sys", {}).get("user"), "password": cluster_config.get("tenant_sys", {}).get("password")}}

        if not ob_cluster["db_host"] or not ob_cluster["db_port"] or not ob_cluster["tenant_sys"]["user"]:
            raise ValueError("Missing required configuration values in ob_cluster or tenant_sys (excluding password)")

        if config_data.get('obcluster') and config_data.get('obcluster').get('servers') and config_data.get('obcluster').get('servers').get('nodes'):
            return

        ob_version = get_observer_version_by_sql(self.context, ob_cluster)
        obConnetcor = OBConnector(context=self.context, ip=ob_cluster["db_host"], port=ob_cluster["db_port"], username=ob_cluster["tenant_sys"]["user"], password=ob_cluster["tenant_sys"]["password"])

        sql = "select SVR_IP, SVR_PORT, ZONE, BUILD_VERSION from oceanbase.__all_server"
        if ob_version.startswith(("1", "2", "3")):
            sql = "select SVR_IP, SVR_PORT, ZONE, BUILD_VERSION from oceanbase.DBA_OB_SERVERS"

        try:
            res = obConnetcor.execute_sql(sql)
            if not res:
                raise Exception(f"Failed to get the node from SQL [{sql}], please check whether the --config option is correct!!!")

            host_info_list = [{"ip": row[0]} for row in res]
            self.stdio.verbose("get host info: %s", host_info_list)

            config_data_new = copy(config_data)
            config_data_new.setdefault('obcluster', {}).setdefault('servers', {}).setdefault('nodes', [])

            for item in host_info_list:
                config_data_new['obcluster']['servers']['nodes'].append({'ip': item['ip']})

            self.stdio.verbose("update nodes config: %s", config_data_new['obcluster']['servers']['nodes'])
            config.update_config_data(config_data_new)
        except Exception as e:
            self.stdio.error(f"An error occurred: {e}")

    def get_namespace(self, spacename):
        if spacename in self.namespaces:
            namespace = self.namespaces[spacename]
        else:
            namespace = HandlerContextNamespace(spacename=spacename)
            self.namespaces[spacename] = namespace
        return namespace

    def call_plugin(self, plugin, spacename=None, target_servers=None, **kwargs):
        args = {'namespace': spacename, 'namespaces': self.namespaces, 'cluster_config': None, 'obproxy_config': None, 'ocp_config': None, 'cmd': self.cmds, 'options': self.options, 'stdio': self.stdio, 'target_servers': target_servers}
        args.update(kwargs)
        self._call_stdio('verbose', 'Call %s ' % (plugin))
        return plugin(**args)

    def _call_stdio(self, func, msg, *arg, **kwarg):
        if func not in self._stdio_func:
            return None
        return self._stdio_func[func](msg, *arg, **kwarg)

    def ssh_clients_connect(self, servers, ssh_clients, user_config, fail_exit=False):
        self._call_stdio('start_loading', 'Open ssh connection')
        connect_io = self.stdio if fail_exit else self.stdio.sub_io()
        connect_status = {}
        success = True
        for server in servers:
            if server not in ssh_clients:
                client = SshClient(SshConfig(server.ip, user_config.username, user_config.password, user_config.key_file, user_config.port, user_config.timeout), self.stdio)
                error = client.connect(stdio=connect_io)
                connect_status[server] = status = CheckStatus()
                if error is not True:
                    success = False
                    status.status = CheckStatus.FAIL
                    status.error = error
                    status.suggests.append(SUG_SSH_FAILED.format())
                else:
                    status.status = CheckStatus.PASS
                    ssh_clients[server] = client
        self._call_stdio('stop_loading', 'succeed' if success else 'fail')
        return connect_status

    def gather_function(self, function_type, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.stdio.print("{0} start ...".format(function_type))
            self.set_context_stdio()
            self.update_obcluster_nodes(config)
            self.set_context(function_type, 'gather', config)
            options = self.context.options
            timestamp = TimeUtils.get_current_us_timestamp()
            self.context.set_variable('gather_timestamp', timestamp)
            if function_type == 'gather_log':
                handler = GatherComponentLogHandler()
                handler.init(
                    self.context,
                    target="observer",
                    from_option=Util.get_option(options, 'from'),
                    to_option=Util.get_option(options, 'to'),
                    since=Util.get_option(options, 'since'),
                    scope=Util.get_option(options, 'scope'),
                    grep=Util.get_option(options, 'grep'),
                    store_dir=Util.get_option(options, 'store_dir'),
                    temp_dir=Util.get_option(options, 'temp_dir'),
                    redact=Util.get_option(options, 'redact'),
                )
                return handler.handle()
            elif function_type == 'gather_awr':
                handler = GatherAwrHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_clog':
                self.context.set_variable('gather_obadmin_mode', 'clog')
                handler = GatherObAdminHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_slog':
                self.context.set_variable('gather_obadmin_mode', 'slog')
                handler = GatherObAdminHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_obstack':
                handler = GatherObstack2Handler(self.context)
                return handler.handle()
            elif function_type == 'gather_perf':
                handler = GatherPerfHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_plan_monitor':
                handler = GatherPlanMonitorHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_all':
                try:
                    handler_sysstat = GatherOsInfoHandler(self.context)
                    handler_sysstat.handle()
                except Exception as e:
                    self.stdio.error("gather_sysstat failed: %s", str(e))
                try:
                    handler_stack = GatherObstack2Handler(self.context)
                    handler_stack.handle()
                except Exception as e:
                    self.stdio.error("gather_obstack failed: %s", str(e))
                try:
                    handler_perf = GatherPerfHandler(self.context)
                    handler_perf.handle()
                except Exception as e:
                    self.stdio.error("gather_perf failed: %s", str(e))
                try:
                    handler_observer_log = GatherComponentLogHandler()
                    handler_observer_log.init(
                        self.context,
                        target="observer",
                        from_option=Util.get_option(options, 'from'),
                        to_option=Util.get_option(options, 'to'),
                        since=Util.get_option(options, 'since'),
                        grep=Util.get_option(options, 'grep'),
                        store_dir=Util.get_option(options, 'store_dir'),
                        temp_dir=Util.get_option(options, 'temp_dir'),
                        redact=Util.get_option(options, 'redact'),
                    )
                    handler_observer_log.handle()
                except Exception as e:
                    self.stdio.error("gather_observer_log failed: %s", str(e))
                try:
                    handler_obproxy = GatherComponentLogHandler()
                    handler_obproxy.init(
                        self.context,
                        target="obproxy",
                        from_option=Util.get_option(options, 'from'),
                        to_option=Util.get_option(options, 'to'),
                        since=Util.get_option(options, 'since'),
                        grep=Util.get_option(options, 'grep'),
                        store_dir=Util.get_option(options, 'store_dir'),
                        temp_dir="/tmp",
                        redact=Util.get_option(options, 'redact'),
                    )
                    return handler_obproxy.handle()
                except Exception as e:
                    self.stdio.error("gather_obproxy failed: %s", str(e))

            elif function_type == 'gather_sysstat':
                handler = GatherOsInfoHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_scenes_run':
                handler = GatherSceneHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_ash_report':
                handler = GatherAshReportHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_tabledump':
                handler = GatherTableDumpHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_parameters':
                handler = GatherParametersHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_variables':
                handler = GatherVariablesHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_dbms_xplan':
                handler = GatherDBMSXPLANHandler(self.context)
                return handler.handle()
            else:
                self._call_stdio('error', 'Not support gather function: {0}'.format(function_type))
                return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='Not support gather function: {0}'.format(function_type))

    def gather_obproxy_log(self, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.set_context_skip_cluster_conn('gather_obproxy_log', 'gather', config)
            options = self.context.options
            handler = GatherComponentLogHandler()
            handler.init(
                self.context,
                target="obproxy",
                from_option=Util.get_option(options, 'from'),
                to_option=Util.get_option(options, 'to'),
                since=Util.get_option(options, 'since'),
                scope=Util.get_option(options, 'scope'),
                grep=Util.get_option(options, 'grep'),
                store_dir=Util.get_option(options, 'store_dir'),
                temp_dir="/tmp",
                redact=Util.get_option(options, 'redact'),
            )
            return handler.handle()

    def gather_scenes_list(self, opt):
        self.set_offline_context('gather_scenes_list', 'gather')
        handler = GatherScenesListHandler(self.context)
        return handler.handle()

    def display_function(self, function_type, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.stdio.print("{0} start ...".format(function_type))
            self.set_context_stdio()
            self.update_obcluster_nodes(config)
            self.set_context(function_type, 'display', config)
            timestamp = TimeUtils.get_current_us_timestamp()
            self.context.set_variable('display_timestamp', timestamp)
            if function_type == 'display_scenes_run':
                handler = DisplaySceneHandler(self.context)
                return handler.handle()
            else:
                self._call_stdio('error', 'Not support display function: {0}'.format(function_type))
                return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='Not support display function: {0}'.format(function_type))

    def display_scenes_list(self, opt):
        self.set_offline_context('display_scenes_list', 'display')
        handler = DisplayScenesListHandler(self.context)
        return handler.handle()

    def analyze_fuction(self, function_type, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.stdio.print("{0} start ...".format(function_type))
            if function_type == 'analyze_log':
                self.set_context_stdio()
                self.update_obcluster_nodes(config)
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeLogHandler(self.context)
                return handler.handle()
            elif function_type == 'analyze_log_offline':
                self.set_context_skip_cluster_conn(function_type, 'analyze', config)
                handler = AnalyzeLogHandler(self.context)
                return handler.handle()
            elif function_type == 'analyze_queue':
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeQueueHandler(self.context)
                return handler.handle()
            elif function_type == 'analyze_flt_trace':
                self.set_context_stdio()
                self.update_obcluster_nodes(config)
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeFltTraceHandler(self.context)
                return handler.handle()
            elif function_type == 'analyze_parameter_default':
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeParameterHandler(self.context, 'default')
                return handler.handle()
            elif function_type == 'analyze_parameter_diff':
                self.set_context_skip_cluster_conn(function_type, 'analyze', config)
                handler = AnalyzeParameterHandler(self.context, 'diff')
                return handler.handle()
            elif function_type == 'analyze_variable_diff':
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeVariableHandler(self.context, 'diff')
                return handler.handle()
            # todo not support silent
            elif function_type == 'analyze_sql':
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeSQLHandler(self.context)
                handler.handle()
            elif function_type == 'analyze_sql_review':
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeSQLReviewHandler(self.context)
                handler.handle()
            elif function_type == 'analyze_index_space':
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeIndexSpaceHandler(self.context)
                return handler.handle()
            elif function_type == 'analyze_memory_offline':
                self.set_context_skip_cluster_conn(function_type, 'analyze', config)
                handler = AnalyzeMemoryHandler(self.context)
                return handler.handle()
            elif function_type == 'analyze_memory':
                self.set_context_stdio()
                self.update_obcluster_nodes(config)
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeMemoryHandler(self.context)
                return handler.handle()
            else:
                self._call_stdio('error', 'Not support analyze function: {0}'.format(function_type))
                return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='Not support analyze function: {0}'.format(function_type))

    def check(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            try:
                self.stdio.print("check start ...")
                self.set_context_stdio()
                self.update_obcluster_nodes(config)
                self.set_context('check', 'check', config)
                obproxy_check_handler = None
                observer_check_handler = None
                result_data = {}

                if self.context.obproxy_config.get("servers") is not None and len(self.context.obproxy_config.get("servers")) > 0:
                    obproxy_check_handler = CheckHandler(self.context, check_target_type="obproxy")
                    obproxy_result = obproxy_check_handler.handle()
                    result_data['obproxy'] = obproxy_result
                if self.context.cluster_config.get("servers") is not None and len(self.context.cluster_config.get("servers")) > 0:
                    observer_check_handler = CheckHandler(self.context, check_target_type="observer")
                    observer_result = observer_check_handler.handle()
                    result_data['observer'] = observer_result
                # when cases=build_before, obproxy_check_handler is not execute
                if obproxy_check_handler is not None and obproxy_check_handler.report:
                    obproxy_report_path = os.path.expanduser(obproxy_check_handler.report.get_report_path())
                    if os.path.exists(obproxy_report_path):
                        result_data['obproxy_report_path'] = os.path.abspath(obproxy_report_path)
                        self.stdio.print("Check obproxy finished. For more details, please run cmd '" + Fore.YELLOW + " cat {0} ".format(obproxy_check_handler.report.get_report_path()) + Style.RESET_ALL + "'")
                if observer_check_handler is not None:
                    observer_report_path = os.path.expanduser(observer_check_handler.report.get_report_path())
                    if os.path.exists(observer_report_path):
                        result_data['observer_report_path'] = os.path.abspath(observer_report_path)
                        self.stdio.print("Check observer finished. For more details, please run cmd'" + Fore.YELLOW + " cat {0} ".format(observer_check_handler.report.get_report_path()) + Style.RESET_ALL + "'")
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=result_data)
            except Exception as e:
                self.stdio.error("check Exception: {0}".format(e))
                self.stdio.verbose(traceback.format_exc())
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="check Exception: {0}".format(e))

    def check_list(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.set_offline_context('check_list', 'check_list')
            handler = CheckListHandler(self.context)
            return handler.handle()

    def rca_run(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.set_context('rca_run', 'rca_run', config)
            if config.get_ob_cluster_config.get("db_host") is not None and config.get_ob_cluster_config.get("servers") is not None:
                self.update_obcluster_nodes(config)
            try:
                handler = RCAHandler(self.context)
                return handler.handle()
            except Exception as e:
                self.stdio.error("rca run Exception: {0}".format(e))
                self.stdio.verbose(traceback.format_exc())
                return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="rca run Exception: {0}".format(e))

    def rca_list(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.set_offline_context('rca_list', 'rca_list')
            handler = RcaScenesListHandler(context=self.context)
            return handler.handle()

    def update(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.stdio.print("update start ...")
            self.set_offline_context('update', 'update')
            handler = UpdateHandler(self.context)
            UpdateHandler.context = self.context
            return handler.handle()

    def tool_crypto_config(self, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.set_offline_context('tool_crypto_config', 'tool_crypto_config')
            handler = CryptoConfigHandler(self.context)
            return handler.handle()

    def config(self, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='No such custum config')
        else:
            self.set_offline_context('config', 'config')
            config_helper = ConfigHelper(context=self.context)
            if Util.get_option(opt, 'file'):
                try:
                    config_helper.build_configuration_by_file(Util.get_option(opt, ''))
                except Exception as e:
                    self._call_stdio('error', 'Build configuration by ini file failed: {0}'.format(e))
                    return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data='Build configuration by ini file failed: {0}'.format(e))
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"msg": "config success"})
            config_helper.build_configuration()
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"msg": "config success"})
