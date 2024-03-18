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
@time: 2022/6/21
@file: parser_utils.py
@desc:
"""
import argparse
import os


class StringMergeAction(argparse.Action):
    def __init__(self, option_strings, dest, **kwargs):
        super(StringMergeAction, self).__init__(option_strings, dest, **kwargs)

    def __call__(self, parser, namespace, values, option_string=None):
        setattr(namespace, self.dest, " ".join(values))


class ParserAction(object):
    def add_attribute_to_namespace(args, attr_name, attr_value):
        setattr(args, attr_name, attr_value)
        return args


class ArgParser(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_inst'):
            cls._inst = super(ArgParser, cls).__new__(cls)
            cls._inited = False
        return cls._inst

    def __init__(self, client):
        if not self._inited:
            self.client = client
            self._inited = True

    @staticmethod
    def get_arg_parser():
        return ArgParser(None)

    def parse_argv(self, argv=None):
        parser = argparse.ArgumentParser(description="Oceanbase Diagnostic Tool", prog=os.environ.get("PROG"),
                                         add_help=True)
        subparsers = parser.add_subparsers()

        # 定义一部分公共参数,可以被子命令复用
        parents_time_arguments = argparse.ArgumentParser(add_help=False)
        parents_time_arguments.add_argument("--from", nargs=2, action=StringMergeAction,
                                            help="specify the start of the time range. format: yyyy-mm-dd hh:mm:ss.",
                                            metavar="datetime")
        parents_time_arguments.add_argument("--to", nargs=2, action=StringMergeAction,
                                            help="specify the end of the time range. format: yyyy-mm-dd hh:mm:ss.",
                                            metavar="datetime")
        parents_time_arguments.add_argument("--since",
                                            help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' ["
                                                 "m]inutes. before to now. format: <n> <m|h|d>. example: 1h.",
                                            metavar="'n'<m|h|d>")
        parents_common_arguments = argparse.ArgumentParser(add_help=False)
        parents_common_arguments.add_argument("--store_dir", metavar="store_dir",
                                              help="the dir to store gather result, current dir by default.")
        parents_common_arguments.add_argument("-c", metavar="config", help="obdiag custom config")

        parser_version = subparsers.add_parser(
            "version", help="Oceanbase Diagnostic Tool Version",
            epilog="Example: obdiag version",
            conflict_handler='resolve',
            description="Oceanbase Diagnostic Tool Version"
        )
        parser_version.set_defaults(version=self.client.obdiag_version)

        parser_obdiag_display = subparsers.add_parser(
            "display-trace", help="Display obdiag trace log",
            epilog="Example: obdiag display",
            conflict_handler='resolve',
            description="Display obdiag trace log"
        )
        parser_obdiag_display.add_argument("--trace_id", metavar="trace_id", nargs=1,
                                           help="obdiag-trace trace_id", required=True)
        parser_obdiag_display.set_defaults(display=self.client.obdiag_display)

        # 通过sys租户快速生成配置文件
        parser_config = subparsers.add_parser(
            "config", help="Quick build config",
            epilog="Example: obdiag config -h127.0.0.1 -uroot@sys -ptest -P2881",
            conflict_handler='resolve',
            description="Quick build config"
        )
        parser_config.add_argument("-h", metavar="db_host", nargs=1,
                                   help="database host", required=True)
        parser_config.add_argument("-u", metavar="sys_user", nargs=1,
                                   help="sys tenant user", required=True)
        parser_config.add_argument("-p", metavar="password", nargs=1,
                                   help="password", required=False, default="")
        parser_config.add_argument("-P", metavar="port", nargs=1,
                                   help="db port", required=False, default=2881)
        parser_config.set_defaults(config=self.client.quick_build_configuration)

        # gather命令
        parser_gather = subparsers.add_parser("gather", help="Gather logs and other information", )

        # 定义gather命令的子命令
        subparsers_gather = parser_gather.add_subparsers()
        # 定义gather命令的子命令: log
        gather_log_arguments = subparsers_gather.add_parser(
            "log", help="Filter and gather logs into a package",
            epilog="Example: obdiag gather log --scope observer "
                   "--from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the logs of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the obdiag machine.")

        gather_log_arguments.set_defaults(gather_log=self.client.handle_gather_log_command)
        gather_log_arguments.add_argument("--scope", metavar="scope", nargs=1,
                                          choices=["observer", "election", "rootservice", "all"],
                                          default="all",
                                          help="log type constrains, "
                                               "choices=[observer, election, rootservice, all], "
                                               "default=all")
        gather_log_arguments.add_argument("--grep", metavar="grep", nargs='+',
                                          help="specify keywords constrain")
        gather_log_arguments.add_argument("--encrypt", metavar="encrypt", nargs=1,
                                          choices=["true", "false"],
                                          default="false",
                                          help="Whether the returned results need to be encrypted, "
                                               "choices=[true, false], "
                                               "default=false")

        # 定义gather命令的子命令: sysstat, 收集主机层面的信息
        gather_sysstat_arguments = subparsers_gather.add_parser(
            "sysstat", help="Gather sysstat info",
            epilog="Example: obdiag gather sysstat",
            parents=[parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the os info "
                        "compress and pack, and transmit to the specified path of the obdiag machine.")

        gather_sysstat_arguments.set_defaults(gather_sysstat=self.client.handle_gather_sysstat_command)

        # 定义gather命令的子命令: stack
        gather_obstack_arguments = subparsers_gather.add_parser(
            "stack", help="Gather ob stack",
            epilog="Example: obdiag gather stack",
            parents=[parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the ob stack "
                        "compress and pack, and transmit to the specified path of the obdiag machine.")

        gather_obstack_arguments.set_defaults(gather_obstack=self.client.handle_gather_obstack_command)

        # gather 子命令 awr
        gather_awr_arguments = subparsers_gather.add_parser(
            "awr", help="Filter and gather awr reports",
            epilog="Example: obdiag gather awr --cluster_name demo1 "
                   "--from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the awr of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the obdiag machine.")

        gather_awr_arguments.set_defaults(gather_awr=self.client.handle_gather_awr_command)
        gather_awr_arguments.add_argument("--cluster_name", metavar="cluster_name", required=True,
                                          nargs=1, help="cluster name.")

        # 定义gather命令的子命令: perf
        gather_perf_arguments = subparsers_gather.add_parser(
            "perf", help="Gather perf info",
            epilog="Example: obdiag gather perf",
            parents=[parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the perf info "
                        "compress and pack, and transmit to the specified path of the obdiag machine.")
        gather_perf_arguments.set_defaults(gather_sysstat=self.client.handle_gather_perf_command)
        gather_perf_arguments.add_argument("--scope", metavar="scope", nargs=1,
                                           choices=["sample", "flame", "all"],
                                           default="all",
                                           help="perf type constrains, "
                                                "choices=[sample, flame, all], "
                                                "default=all")

        # gather 子命令 plan_monitor
        gather_plan_monitor_arguments = subparsers_gather.add_parser(
            "plan_monitor", help="Filter and gather sql plan monitor reports",
            epilog="Example: obdiag gather plan_monitor --trace_id xxxxx",
            parents=[parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the sql plan monitor of the specified trace_id "
                        "compress and pack, and transmit to the specified path of the obdiag machine.")

        gather_plan_monitor_arguments.set_defaults(gather_plan_monitor=self.client.handle_gather_plan_monitor)
        gather_plan_monitor_arguments.add_argument("--trace_id", metavar="trace_id", required=True,
                                                   nargs=1, help=" sql trace id")
        gather_plan_monitor_arguments.add_argument("--env", metavar="env", type=str,
                                                   help='env, eg: "{env1=xxx, env2=xxx}"')

        # gather 子命令 clog
        gather_clog_arguments = subparsers_gather.add_parser(
            "clog", help="Filter and gather clog",
            epilog="Example: obdiag gather clog --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the clog of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the obdiag machine.")
        gather_clog_arguments.set_defaults(gather_clog=self.client.handle_gather_clog_command)
        gather_clog_arguments.add_argument("--encrypt", metavar="encrypt", nargs=1,
                                           choices=["true", "false"],
                                           default="false",
                                           help="Whether the returned results need to be encrypted, "
                                                "choices=[true, false], "
                                                "default=false")

        # gather 子命令 slog
        gather_slog_arguments = subparsers_gather.add_parser(
            "slog", help="Filter and gather slog",
            epilog="Example: obdiag gather slog --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the slog of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the obdiag machine.")
        gather_slog_arguments.set_defaults(gather_slog=self.client.handle_gather_slog_command)
        gather_slog_arguments.add_argument("--encrypt", metavar="encrypt", nargs=1,
                                           choices=["true", "false"],
                                           default="false",
                                           help="Whether the returned results need to be encrypted, "
                                                "choices=[true, false], "
                                                "default=false")

        # 定义gather命令的子命令: obproxy_log
        gather_obproxy_log_arguments = subparsers_gather.add_parser(
            "obproxy_log", help="Filter and gather obproxy logs into a package",
            epilog="Example: obdiag gather obproxy_log --scope obproxy "
                   "--from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the logs of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the obdiag machine.")

        gather_obproxy_log_arguments.set_defaults(gather_obproxy_log=self.client.handle_gather_obproxy_log_command)
        gather_obproxy_log_arguments.add_argument("--scope", metavar="scope", nargs=1,
                                                  choices=["obproxy", "obproxy_digest", "obproxy_stat", "obproxy_slow",
                                                           "obproxy_limit", "all"],
                                                  default="all",
                                                  help="log type constrains, "
                                                       "choices=[obproxy, obproxy_digest, obproxy_stat, obproxy_slow, obproxy_limit, all], "
                                                       "default=all")
        gather_obproxy_log_arguments.add_argument("--grep", metavar="grep", nargs='+',
                                                  help="specify keywords constrain")
        gather_obproxy_log_arguments.add_argument("--encrypt", metavar="encrypt", nargs=1,
                                                  choices=["true", "false"],
                                                  default="false",
                                                  help="Whether the returned results need to be encrypted, "
                                                       "choices=[true, false], "
                                                       "default=false")

        # gather all
        gather_all_arguments = subparsers_gather.add_parser(
            "all", help="Gather all",
            epilog="Example: obdiag gather all --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather all reports")

        gather_all_arguments.set_defaults(
            gather_log=self.client.handle_gather_log_command,
            gather_sysstat=self.client.handle_gather_sysstat_command,
            gather_obstack=self.client.handle_gather_obstack_command,
            gather_perf=self.client.handle_gather_perf_command
        )
        gather_all_arguments.add_argument("--scope", metavar="scope", nargs=1,
                                          choices=["observer", "election", "rootservice", "all"],
                                          default="all",
                                          help="log type constrains, "
                                               "choices=[observer, election, rootservice, all], "
                                               "default=all")
        gather_all_arguments.add_argument("--encrypt", metavar="encrypt", nargs=1,
                                          choices=["true", "false"],
                                          default="false",
                                          help="Whether the returned results need to be encrypted, "
                                               "choices=[true, false], "
                                               "default=false")
        gather_all_arguments.add_argument("--grep", metavar="grep", nargs='+',
                                          help="specify keywords constrain for log")

        gather_scene = subparsers_gather.add_parser(
            "scene", help="Gather scene info",
            conflict_handler='resolve',
            description="gather scene")
        # gather scene list
        subparsers_gather_scene = gather_scene.add_subparsers()
        gather_scene_arguments = subparsers_gather_scene.add_parser(
            "run",
            help="Gather scene run",
            parents=[parents_time_arguments, parents_common_arguments],
            epilog="Example: obdiag gather scene run --scene=xxx",
            conflict_handler='resolve',
            description="gather scene run")
        gather_scene_arguments.set_defaults(gather_scene=self.client.handle_gather_scene_command)
        gather_scene_arguments.add_argument("--scene", metavar="scene", nargs=1, required=True, help="specify scene")
        gather_scene_arguments.add_argument("--env", metavar="env", type=str, help='env, eg: "{env1=xxx, env2=xxx}"')
        gather_scene_arguments.add_argument("--dis_update",type=bool, metavar="dis_update", nargs=1,help="The type is bool. --dis_update is assigned any value representing true",required=False)

        # gather scene list
        gather_scene_list_arguments = subparsers_gather_scene.add_parser(
            "list",
            help="Gather scene list",
            epilog="Example: obdiag gather scene list",
            conflict_handler='resolve',
            description="gather scene list")
        gather_scene_list_arguments.set_defaults(gather_scene_list=self.client.handle_gather_scene_list_command)

        # analyze
        parser_analyze = subparsers.add_parser("analyze", help="analyze logs and other information", )
        subparsers_analyze = parser_analyze.add_subparsers()
        analyze_log_arguments = subparsers_analyze.add_parser(
            "log", help="Filter and analyze observer logs",
            epilog="Example1: obdiag analyze log --scope observer --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00\n\n"
                   "Example2: obdiag analyze log --scope observer --since 1h --grep STORAGE\n\n"
                   "Example3: obdiag analyze log --files observer.log.20230831142211247\n\n"
                   "Example4: obdiag analyze log --files ./log/",
            parents=[parents_time_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, analyze observer logs")

        analyze_log_arguments.set_defaults(analyze_log=self.client.handle_analyze_log_command)
        analyze_log_arguments.add_argument("--scope", metavar="scope", nargs=1,
                                           choices=["observer", "election", "rootservice", "all"],
                                           default="all",
                                           help="log type constrains, "
                                                "choices=[observer, election, rootservice, all], "
                                                "default=all")
        analyze_log_arguments.add_argument("--log_level", metavar="log_level", nargs=1,
                                           choices=["DEBUG", "TRACE", "INFO", "WDIAG", "WARN", "EDIAG", "ERROR"],
                                           help="log level constrains, "
                                                "choices=[DEBUG, TRACE, INFO, WDIAG, WARN, EDIAG, ERROR], "
                                                "default=WARN")
        analyze_log_arguments.add_argument("--files", metavar="files", nargs='+',
                                           help="specify file")
        analyze_log_arguments.add_argument("--grep", metavar="grep", nargs='+',
                                           help="specify keywords constrain")

        analyze_flt_trace_arguments = subparsers_analyze.add_parser(
            "flt_trace", help="Filter and analyze observer trace log",
            epilog="Example1: obdiag analyze flt_trace --flt_trace_id <flt_trace_id>\n\n",
            parents=[parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, analyze observer logs")
        analyze_flt_trace_arguments.set_defaults(analyze_flt_trace=self.client.handle_analyze_flt_trace_command)
        analyze_flt_trace_arguments.add_argument("--files", metavar="files", nargs='+',
                                                 help="specify file")
        analyze_flt_trace_arguments.add_argument("--flt_trace_id", metavar="flt_trace_id", nargs=1, required=True,
                                                 help="flt trace id")
        analyze_flt_trace_arguments.add_argument("--top", metavar="top", nargs=1, help="top leaf span")
        analyze_flt_trace_arguments.add_argument("--recursion", metavar="recursion", nargs=1,
                                                 help="Maximum number of recursion")
        analyze_flt_trace_arguments.add_argument("--output", metavar="output", nargs=1,
                                                 help="Print the result to the maximum output line on the screen")

        # 定义巡检参数check arguments
        check_arguments = subparsers.add_parser("check", help="do check",
                                                epilog="Example: obdiag check list\n\n"
                                                       "Example: obdiag check --cases=ad\n\n",
                                                conflict_handler='resolve', )
        check_arguments.set_defaults(check=self.client.handle_check_command)

        subparsers_check = check_arguments.add_subparsers()
        check_arguments.set_defaults(check=self.client.handle_check_command)
        check_arguments.add_argument("--cases", metavar="cases", nargs=1,
                                     help="check observer's cases on package_file", required=False)
        check_arguments.add_argument("--obproxy_cases", metavar="obproxy_cases", nargs=1,
                                     help="check obproxy's cases on package_file", required=False)
        check_arguments.add_argument("--store_dir", metavar="store_dir", nargs=1,
                                     help="report path", required=False)
        check_arguments.add_argument("-c", metavar="config", help="obdiag custom config")
        check_arguments.add_argument("--dis_update",type=bool, metavar="dis_update", nargs=1,help="The type is bool. --dis_updata is assigned any value representing true",required=False)
        check_list_arguments = subparsers_check.add_parser(
            "list", help="show list of check list",
            epilog="Example: obdiag check list\n\n", )
        check_list_arguments.set_defaults(check=self.client.handle_check_list_command)

        # rca arguments
        rca_arguments = subparsers.add_parser("rca", help="root cause analysis",
                                              epilog="Example: obdiag rca run --scene=disconnection\n\n"
                                                     "Example: obdiag rca list",
                                              conflict_handler='resolve', )
        subparsers_rca = rca_arguments.add_subparsers()
        rca_list_arguments = subparsers_rca.add_parser(
            "list", help="show list of rca list",
            epilog="Example: obdiag rca list\n\n",)
        rca_list_arguments.set_defaults(rca_list=self.client.handle_rca_list_command)

        rca_run_arguments = subparsers_rca.add_parser(
            "run", help="Filter and analyze observer trace log",
            epilog="Example: obdiag rca run --scene=disconnection\n\n",
            conflict_handler='resolve',
            description="According to the input parameters, rca run")
        rca_run_arguments.set_defaults(rca_run=self.client.handle_rca_run_command)
        rca_run_arguments.add_argument("--scene", metavar="scene", nargs=1,help="scene name. The argument is required.", required=True)
        rca_run_arguments.add_argument("--parameters", metavar="parameters", nargs=1,help="Other parameters required for the scene, input in JSON format.",required=False)
        rca_run_arguments.add_argument("--store_dir", metavar="store_dir", nargs=1,help="result path",required=False)
        rca_run_arguments.add_argument("-c", metavar="config", help="obdiag custom config")

        # 定义升级参数update arguments
        update_arguments = subparsers.add_parser("update", help="Update cheat files",
                                                epilog="Example: obdiag update\n\n",
                                                conflict_handler='resolve', )
        update_arguments.set_defaults(check=self.client.handle_update_command)
        update_arguments.add_argument("--file", metavar="file", help="obdiag update cheat file path. Please note that you need to ensure the reliability of the files on your own")
        update_arguments.add_argument("--force",type=bool, metavar="force", nargs=1,help="Force Update",required=False)
        # parse args
        args = parser.parse_args(args=argv)
        return args
