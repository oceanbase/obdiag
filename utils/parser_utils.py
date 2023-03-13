#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
        parents_observer_arguments = argparse.ArgumentParser(add_help=False)
        parents_observer_arguments.add_argument("--ob_install_dir", metavar="ob_install_dir",
                                                help="the dir to ob log dir")

        parents_obproxy_arguments = argparse.ArgumentParser(add_help=False)
        parents_obproxy_arguments.add_argument("--obproxy_install_dir", metavar="obproxy_install_dir",
                                                help="the dir to obproxy log dir")

        parents_common_arguments = argparse.ArgumentParser(add_help=False)
        parents_common_arguments.add_argument("--store_dir", metavar="store_dir",
                                              help="the dir to store gather result, current dir by default.")

        # 通过ocp快速生成配置文件
        parser_config = subparsers.add_parser(
            "config", help="Quick build config",
            epilog="Example: ./obdiag config --cluster_name demo1 --cluster_id xxx",
            conflict_handler='resolve',
            description="Quick build config"
        )
        parser_config.add_argument("--cluster_name", metavar="cluster_name", nargs=1,
                                   help="cluster name", required=True)
        parser_config.add_argument("--cluster_id", metavar="cluster_id", nargs=1,
                                   help="cluster id", required=True)
        parser_config.set_defaults(config=self.client.quick_build_configuration)

        # gather命令
        parser_gather = subparsers.add_parser("gather", help="Gather logs and other information", )

        # 定义gather命令的子命令
        subparsers_gather = parser_gather.add_subparsers()
        # 定义gather命令的子命令: log
        gather_log_arguments = subparsers_gather.add_parser(
            "log", help="Filter and gather logs into a package",
            epilog="Example: ./obdiag gather log --scope observer "
                   "--from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments, parents_observer_arguments],
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
        gather_log_arguments.add_argument("--grep", metavar="grep", nargs=1,
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
            epilog="Example: ./obdiag gather sysstat",
            parents=[parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the os info "
                        "compress and pack, and transmit to the specified path of the obdiag machine.")

        gather_sysstat_arguments.set_defaults(gather_sysstat=self.client.handle_gather_sysstat_command)

        # gather 子命令 awr
        gather_awr_arguments = subparsers_gather.add_parser(
            "awr", help="Filter and gather awr reports",
            epilog="Example: ./obdiag gather awr --cluster_name demo1 "
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
            epilog="Example: ./obdiag gather perf",
            parents=[parents_observer_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the perf info "
                        "compress and pack, and transmit to the specified path of the obdiag machine.")
        gather_perf_arguments.set_defaults(gather_sysstat=self.client.handle_gather_perf_command)
        gather_perf_arguments.add_argument("--scope", metavar="scope", nargs=1,
                                          choices=["sample", "flame", "pstack", "all"],
                                          default="all",
                                          help="perf type constrains, "
                                               "choices=[sample, flame, pstack, all], "
                                               "default=all")

        # gather 子命令 plan_monitor
        gather_plan_monitor_arguments = subparsers_gather.add_parser(
            "plan_monitor", help="Filter and gather sql plan monitor reports",
            epilog="Example: ./obdiag gather plan_monitor --trace_id xxxxx",
            parents=[parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the sql plan monitor of the specified trace_id "
                        "compress and pack, and transmit to the specified path of the obdiag machine.")

        gather_plan_monitor_arguments.set_defaults(gather_plan_monitor=self.client.handle_gather_plan_monitor)
        gather_plan_monitor_arguments.add_argument("--trace_id", metavar="trace_id", required=True,
                                                   nargs=1, help=" sql trace id")

        # gather 子命令 clog
        gather_clog_arguments = subparsers_gather.add_parser(
            "clog", help="Filter and gather clog",
            epilog="Example: ./obdiag gather clog --cluster_name demo1 --from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments, parents_observer_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the clog of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the obdiag machine.")
        gather_clog_arguments.set_defaults(gather_clog=self.client.handle_gather_clog_command)
        gather_clog_arguments.add_argument("--cluster_name", metavar="cluster_name", required=True,
                                              nargs=1, help="cluster name.")
        gather_clog_arguments.add_argument("--encrypt", metavar="encrypt", nargs=1,
                                              choices=["true", "false"],
                                              default="false",
                                              help="Whether the returned results need to be encrypted, "
                                                   "choices=[true, false], "
                                                   "default=false")

        # gather 子命令 slog
        gather_slog_arguments = subparsers_gather.add_parser(
            "slog", help="Filter and gather slog",
            epilog="Example: ./obdiag gather slog --cluster_name demo1 "
                   "--from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments, parents_observer_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the slog of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the obdiag machine.")
        gather_slog_arguments.set_defaults(gather_slog=self.client.handle_gather_slog_command)
        gather_slog_arguments.add_argument("--cluster_name", metavar="cluster_name", required=True,
                                           nargs=1, help="cluster name.")
        gather_slog_arguments.add_argument("--encrypt", metavar="encrypt", nargs=1,
                                           choices=["true", "false"],
                                           default="false",
                                           help="Whether the returned results need to be encrypted, "
                                                "choices=[true, false], "
                                                "default=false")

        # 定义gather命令的子命令: obproxy_log
        gather_obproxy_log_arguments = subparsers_gather.add_parser(
            "obproxy_log", help="Filter and gather obproxy logs into a package",
            epilog="Example: ./obdiag gather obproxy_log --scope obproxy "
                   "--from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_common_arguments, parents_obproxy_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the logs of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the obdiag machine.")

        gather_obproxy_log_arguments.set_defaults(gather_obproxy_log=self.client.handle_gather_obproxy_log_command)
        gather_obproxy_log_arguments.add_argument("--scope", metavar="scope", nargs=1,
                                          choices=["obproxy", "obproxy_digest", "obproxy_stat", "obproxy_slow", "obproxy_limit", "all"],
                                          default="all",
                                          help="log type constrains, "
                                               "choices=[obproxy, obproxy_digest, obproxy_stat, obproxy_slow, obproxy_limit, all], "
                                               "default=all")
        gather_obproxy_log_arguments.add_argument("--grep", metavar="grep", nargs=1,
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
            epilog="Example: ./obdiag gather all --cluster_name demo1 "
                   "--from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ",
            parents=[parents_time_arguments, parents_observer_arguments, parents_common_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather all reports")

        gather_all_arguments.set_defaults(
            gather_awr=self.client.handle_gather_awr_command,
            gather_log=self.client.handle_gather_log_command,
            gather_sysstat=self.client.handle_gather_sysstat_command,
            gather_perf=self.client.handle_gather_perf_command,
            gather_clog=self.client.handle_gather_clog_command,
            gather_slog=self.client.handle_gather_slog_command,
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
        gather_all_arguments.add_argument("--grep", metavar="grep", nargs=1,
                                          help="specify keywords constrain for log")
        gather_all_arguments.add_argument("--cluster_name", metavar="cluster_name", required=True,
                                          nargs=1, help="cluster name, awr report need")

        # parse args
        args = parser.parse_args(args=argv)
        return args
