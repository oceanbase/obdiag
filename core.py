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
from optparse import Values
from copy import copy

from handler.gather.gather_ash_report import GatherAshReportHandler
from handler.rca.rca_handler import RCAHandler
from handler.rca.rca_list import RcaScenesListHandler
from common.ssh import SshClient, SshConfig, dis_rsa_algorithms
from context import HandlerContextNamespace, HandlerContext
from config import ConfigManager, InnerConfigManager
from err import CheckStatus, SUG_SSH_FAILED
from handler.analyzer.analyze_flt_trace import AnalyzeFltTraceHandler
from handler.analyzer.analyze_log import AnalyzeLogHandler
from handler.checker.check_handler import CheckHandler
from handler.checker.check_list import CheckListHandler
from handler.gather.gather_log import GatherLogHandler
from handler.gather.gather_awr import GatherAwrHandler
from handler.gather.gather_obproxy_log import GatherObProxyLogHandler
from handler.gather.gather_sysstat import GatherOsInfoHandler
from handler.gather.gather_obstack2 import GatherObstack2Handler
from handler.gather.gather_obadmin import GatherObAdminHandler
from handler.gather.gather_perf import GatherPerfHandler
from handler.gather.gather_plan_monitor import GatherPlanMonitorHandler
from handler.gather.gather_scenes import GatherSceneHandler
from handler.gather.scenes.list import GatherScenesListHandler
from telemetry.telemetry import telemetry
from update.update import UpdateHandler
from colorama import Fore, Style
from common.config_helper import ConfigHelper

from common.tool import Util
from common.tool import TimeUtils


class ObdiagHome(object):

    def __init__(self, stdio=None, config_path=os.path.expanduser('~/.obdiag/config.yml')):
        self._optimize_manager = None
        self.stdio = None
        self._stdio_func = None
        self.cmds = []
        self.options = Values()
        self.namespaces = {}
        self.set_stdio(stdio)
        self.context = None
        self.inner_config_manager = InnerConfigManager(stdio)
        self.config_manager = ConfigManager(config_path, stdio)
        if (
            self.inner_config_manager.config.get("obdiag") is not None
            and self.inner_config_manager.config.get("obdiag").get("basic") is not None
            and self.inner_config_manager.config.get("obdiag").get("basic").get("telemetry") is not None
            and self.inner_config_manager.config.get("obdiag").get("basic").get("telemetry") is False
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

    def set_context(self, handler_name, namespace, config):
        self.context = HandlerContext(
            handler_name=handler_name,
            namespace=namespace,
            cluster_config=config.get_ob_cluster_config,
            obproxy_config=config.get_obproxy_config,
            ocp_config=config.get_ocp_config,
            cmd=self.cmds,
            options=self.options,
            stdio=self.stdio,
            inner_config=self.inner_config_manager.config,
        )
        telemetry.set_cluster_conn(config.get_ob_cluster_config)

    def set_context_skip_cluster_conn(self, handler_name, namespace, config):
        self.context = HandlerContext(
            handler_name=handler_name,
            namespace=namespace,
            cluster_config=config.get_ob_cluster_config,
            obproxy_config=config.get_obproxy_config,
            ocp_config=config.get_ocp_config,
            cmd=self.cmds,
            options=self.options,
            stdio=self.stdio,
            inner_config=self.inner_config_manager.config,
        )

    def set_offline_context(self, handler_name, namespace):
        self.context = HandlerContext(handler_name=handler_name, namespace=namespace, cmd=self.cmds, options=self.options, stdio=self.stdio, inner_config=self.inner_config_manager.config)

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
            return False
        else:
            self.stdio.print("{0} start ...".format(function_type))
            self.set_context(function_type, 'gather', config)
            timestamp = TimeUtils.get_current_us_timestamp()
            self.context.set_variable('gather_timestamp', timestamp)
            if function_type == 'gather_log':
                handler = GatherLogHandler(self.context)
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
                handler_sysstat = GatherOsInfoHandler(self.context)
                handler_sysstat.handle()
                handler_stack = GatherObstack2Handler(self.context)
                handler_stack.handle()
                handler_perf = GatherPerfHandler(self.context)
                handler_perf.handle()
                handler_log = GatherLogHandler(self.context)
                handler_log.handle()
                handler_obproxy = GatherObProxyLogHandler(self.context)
                handler_obproxy.handle()
                return True
            elif function_type == 'gather_sysstat':
                handler = GatherOsInfoHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_scenes_run':
                handler = GatherSceneHandler(self.context)
                return handler.handle()
            elif function_type == 'gather_ash_report':
                handler = GatherAshReportHandler(self.context)
                return handler.handle()
            else:
                self._call_stdio('error', 'Not support gather function: {0}'.format(function_type))
                return False

    def gather_obproxy_log(self, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return False
        else:
            self.set_context_skip_cluster_conn('gather_obproxy_log', 'gather', config)
            handler = GatherObProxyLogHandler(self.context)
            return handler.handle()

    def gather_scenes_list(self, opt):
        self.set_offline_context('gather_scenes_list', 'gather')
        handler = GatherScenesListHandler(self.context)
        return handler.handle()

    def analyze_fuction(self, function_type, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return False
        else:
            self.stdio.print("{0} start ...".format(function_type))
            if function_type == 'analyze_log':
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeLogHandler(self.context)
                handler.handle()
            elif function_type == 'analyze_log_offline':
                self.set_context_skip_cluster_conn(function_type, 'analyze', config)
                handler = AnalyzeLogHandler(self.context)
                handler.handle()
            elif function_type == 'analyze_flt_trace':
                self.set_context(function_type, 'analyze', config)
                handler = AnalyzeFltTraceHandler(self.context)
                handler.handle()
            else:
                self._call_stdio('error', 'Not support analyze function: {0}'.format(function_type))
                return False

    def check(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return False
        else:
            self.stdio.print("check start ...")
            self.set_context('check', 'check', config)
            obproxy_check_handler = None
            observer_check_handler = None
            if self.context.obproxy_config.get("servers") is not None and len(self.context.obproxy_config.get("servers")) > 0:
                obproxy_check_handler = CheckHandler(self.context, check_target_type="obproxy")
                obproxy_check_handler.handle()
                obproxy_check_handler.execute()
            if self.context.cluster_config.get("servers") is not None and len(self.context.cluster_config.get("servers")) > 0:
                observer_check_handler = CheckHandler(self.context, check_target_type="observer")
                observer_check_handler.handle()
                observer_check_handler.execute()
            if obproxy_check_handler is not None:
                obproxy_report_path = os.path.expanduser(obproxy_check_handler.report.get_report_path())
                if os.path.exists(obproxy_report_path):
                    self.stdio.print("Check obproxy finished. For more details, please run cmd '" + Fore.YELLOW + " cat {0} ".format(obproxy_check_handler.report.get_report_path()) + Style.RESET_ALL + "'")
            if observer_check_handler is not None:
                observer_report_path = os.path.expanduser(observer_check_handler.report.get_report_path())
                if os.path.exists(observer_report_path):
                    self.stdio.print("Check observer finished. For more details, please run cmd'" + Fore.YELLOW + " cat {0} ".format(observer_check_handler.report.get_report_path()) + Style.RESET_ALL + "'")

    def check_list(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return False
        else:
            self.set_offline_context('check_list', 'check_list')
            handler = CheckListHandler(self.context)
            handler.handle()

    def rca_run(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return False
        else:
            self.set_context('rca_run', 'rca_run', config)
            try:
                handler = RCAHandler(self.context)
                handler.handle()
                handler.execute()
            except Exception as e:
                self.stdio.error(e)

    def rca_list(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return False
        else:
            self.set_offline_context('rca_list', 'rca_list')
            handler = RcaScenesListHandler(context=self.context)
            handler.handle()

    def update(self, opts):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return False
        else:
            self.stdio.print("update start ...")
            self.set_offline_context('update', 'update')
            handler = UpdateHandler(self.context)
            handler.execute()

    def config(self, opt):
        config = self.config_manager
        if not config:
            self._call_stdio('error', 'No such custum config')
            return False
        else:
            self.set_offline_context('config', 'config')
            config_helper = ConfigHelper(context=self.context)
            config_helper.build_configuration()
