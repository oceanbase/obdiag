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
        parser = argparse.ArgumentParser(description="Oceanbase Diagnosis Gather", prog=os.environ.get("PROG"),
                                         add_help=True)
        subparsers = parser.add_subparsers()

        # 定义一部分公共参数,可以被子命令复用
        time_range_arguments = argparse.ArgumentParser()
        time_range_arguments.add_argument("--from", nargs=2, action=StringMergeAction,
                                          help="specify the start of the time range. format: yyyy-mm-dd hh:mm:ss.",
                                          metavar="datetime")
        time_range_arguments.add_argument("--to", nargs=2, action=StringMergeAction,
                                          help="specify the end of the time range. format: yyyy-mm-dd hh:mm:ss.",
                                          metavar="datetime")
        time_range_arguments.add_argument("--since",
                                          help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes."
                                               " before to now. format: <n> <m|h|d>. example: 1h.",
                                          metavar="'n'<m|h|d>")

        # 通过ocp快速生成配置文件
        parser_config = subparsers.add_parser(
            "config", help="Quick build config",
            epilog="Example: ./odg_ctl config --cluster_name demo1 --cluster_id xxx",
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
        # 定义gather命令的子命令1: log
        gather_log_arguments = subparsers_gather.add_parser(
            "log", help="Filter and gather logs into a package",
            epilog="Example: ./odg_ctl gather log --scope observer "
                   "--from 2022-06-16 18:25:00 --to 2022-06-16 18:30:00 ", parents=[time_range_arguments],
            conflict_handler='resolve',
            description="According to the input parameters, gather the logs of the specified range "
                        "(whether it is time range), compress and pack, "
                        "and transmit to the specified path of the odg machine.")

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
        gather_log_arguments.add_argument("--ob_log_dir", metavar="ob_log_dir",
                                          help="the dir to ob log dir")
        gather_log_arguments.add_argument("--store_dir", metavar="store_dir",
                                          help="the dir to store logs, current dir by default.")
        # parse args
        args = parser.parse_args(args=argv)
        return args
