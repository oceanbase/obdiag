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
@file: diag_cmd.py
@desc:
"""

from __future__ import absolute_import, division, print_function
from src.common.tool import Util, StringUtils, check_new_obdiag_version
import os
import sys
import textwrap
import re
import json
from uuid import uuid1 as uuid, UUID
from optparse import OptionParser, BadOptionError, Option, IndentedHelpFormatter
from src.common.core import ObdiagHome
from src.common.result_type import ObdiagResult
from src.common.stdio import IO
from src.common.version import get_obdiag_version
from src.telemetry.telemetry import telemetry
from src.common.version import OBDIAG_VERSION

# TODO when obdiag_version ≥ 3.0, the default value of err_stream will be changed to sys.stderr
ROOT_IO = IO(1, error_stream=sys.stdout)
OBDIAG_HOME_PATH = os.path.join(os.getenv('HOME'), 'oceanbase-diagnostic-tool')


class OptionHelpFormatter(IndentedHelpFormatter):

    def format_option(self, option):
        result = []
        opts = self.option_strings[option]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else:
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if option.help:
            help_text = self.expand_default(option)
            help_lines = help_text.split('\n')
            if len(help_lines) == 1:
                help_lines = textwrap.wrap(help_text, self.help_width)
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line) for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result)


class AllowUndefinedOptionParser(OptionParser):
    IS_TTY = sys.stdin.isatty()

    def __init__(self, usage=None, option_list=None, option_class=Option, version=None, conflict_handler="resolve", description=None, formatter=None, add_help_option=True, prog=None, epilog=None, allow_undefine=True, undefine_warn=True):
        OptionParser.__init__(self, usage, option_list, option_class, version, conflict_handler, description, formatter, add_help_option, prog, epilog)
        self.allow_undefine = allow_undefine
        self.undefine_warn = undefine_warn

    def warn(self, msg, file=None):
        if self.IS_TTY:
            print("%s %s" % (IO.WARNING_PREV, msg))
        else:
            print('warn: %s' % msg)

    def _process_long_opt(self, rargs, values):
        try:
            value = rargs[0]
            OptionParser._process_long_opt(self, rargs, values)
        except BadOptionError as e:
            if self.allow_undefine:
                key = e.opt_str
                value = value[len(key) + 1 :]
                setattr(values, key.strip('-').replace('-', '_'), value if value != '' else True)
                self.undefine_warn and self.warn(e)
            else:
                raise e

    def _process_short_opts(self, rargs, values):
        try:
            value = rargs[0]
            OptionParser._process_short_opts(self, rargs, values)
        except BadOptionError as e:
            if self.allow_undefine:
                key = e.opt_str
                value = value[len(key) + 1 :]
                setattr(values, key.strip('-').replace('-', '_'), value if value != '' else True)
                self.undefine_warn and self.warn(e)
            else:
                raise e


class BaseCommand(object):

    def __init__(self, name, summary):
        self.name = name
        self.summary = summary
        self.args = []
        self.cmds = []
        self.opts = {}
        self.prev_cmd = ''
        self.is_init = False
        self.hidden = False
        self.has_trace = True
        self.inner_config_change_map = {}
        self.parser = AllowUndefinedOptionParser(add_help_option=True)
        self.parser.add_option('-h', '--help', action='callback', callback=self._show_help, help='Show help and exit.')
        self.parser.add_option('-v', '--verbose', action='callback', callback=self._set_verbose, help='Activate verbose output.')
        self.parser.add_option('--config_password', type="string", help='config password')
        self.parser.add_option('--inner_config', action='callback', type="str", callback=self._inner_config_change, help='change inner config. ')

    def _set_verbose(self, *args, **kwargs):
        ROOT_IO.set_verbose_level(0xFFFFFFF)

    def _inner_config_change(self, option, opt_str, value, parser):
        """
        Inner config change
        """
        try:
            key, val = value.split('=')
            if key is None or key == "":
                return
            m = self._inner_config_change_set(key, val)

            def _change_inner_config(conf_map, change_conf_map):
                for change_conf_map_key, change_conf_map_value in change_conf_map.items():
                    if change_conf_map_key in conf_map:
                        if isinstance(change_conf_map_value, dict):
                            _change_inner_config(conf_map[change_conf_map_key], change_conf_map_value)
                        else:
                            conf_map[change_conf_map_key] = change_conf_map_value
                    else:
                        conf_map[change_conf_map_key] = change_conf_map_value
                return conf_map

            self.inner_config_change_map = _change_inner_config(self.inner_config_change_map, m)
        except Exception as e:
            raise Exception("Key or val ({1}) is illegal: {0}".format(e, value))

    def _inner_config_change_set(self, key, val):
        def recursion(change_map, key, val):
            if key is None or key == "":
                raise Exception("key is None")
            if val is None or val == "":
                raise Exception("val is None")
            if key.startswith(".") or key.endswith("."):
                raise Exception("Key starts or ends '.'")
            if "." in key:
                map_key = key.split(".")[0]
                change_map[map_key] = recursion({}, key[len(map_key) + 1 :], val)
                return change_map
            else:
                change_map[key] = val
                return change_map

        return recursion({}, key, val)

    def init(self, cmd, args):
        if self.is_init is False:
            self.prev_cmd = cmd
            self.args = args
            self.is_init = True
            self.parser.prog = self.prev_cmd
            option_list = self.parser.option_list[2:]
            option_list.append(self.parser.option_list[0])
            option_list.append(self.parser.option_list[1])
            self.parser.option_list = option_list
        return self

    def parse_command(self):
        self.opts, self.cmds = self.parser.parse_args(self.args)
        return self.opts

    def do_command(self):
        raise NotImplementedError

    def _show_help(self, *args, **kwargs):
        ROOT_IO.print(self._mk_usage())
        self.parser.exit(0)

    def _mk_usage(self):
        return self.parser.format_help(OptionHelpFormatter())


class ObdiagOriginCommand(BaseCommand):
    OBDIAG_PATH = OBDIAG_HOME_PATH

    def __init__(self, name, summary):
        super().__init__(name, summary)
        self.trace_id = uuid()

    @property
    def enable_log(self):
        return True

    def is_valid_time_format(self, time_string):
        time_pattern = r'^\d{2}:\d{2}:\d{2}$'
        return bool(re.match(time_pattern, time_string))

    def preprocess_argv(self, argv):
        """
        Preprocesses the command line arguments to ensure that date-time strings for --from and --to
        options are properly quoted, even if they are originally provided without quotes.
        """
        processed_argv = []
        from_index = None
        to_index = None
        for i, arg in enumerate(argv):
            if arg == '--from':
                from_index = i + 1
            elif arg == '--to':
                to_index = i + 1

            if from_index is not None and i == from_index:
                next_arg = argv[i + 1] if i + 1 < len(argv) else None
                if next_arg and self.is_valid_time_format(next_arg):
                    processed_argv.append(argv[i] + ' ' + next_arg)
                    from_index = None
                    i += 1
                else:
                    processed_argv.append(arg)
            elif to_index is not None and i == to_index:
                next_arg = argv[i + 1] if i + 1 < len(argv) else None
                if next_arg and self.is_valid_time_format(next_arg):
                    processed_argv.append(argv[i] + ' ' + next_arg)
                    to_index = None
                    i += 1
                else:
                    processed_argv.append(arg)
            else:
                processed_argv.append(arg)
        return processed_argv

    def parse_command(self):
        self.args = self.preprocess_argv(self.args)
        return super(ObdiagOriginCommand, self).parse_command()

    def start_check(self):
        current_work_path = os.getcwd()
        home_path = os.path.expanduser("~")
        if '.' in OBDIAG_VERSION:
            if current_work_path.startswith(home_path + "/.obdiag"):
                if StringUtils.compare_versions_lower(OBDIAG_VERSION, "3.0.0"):
                    ROOT_IO.warn("Currently executing in obdiag home directory!")
                else:
                    ROOT_IO.error("Cannot be executed in the obdiag working directory!")
                    ROOT_IO.exit(1)

    def do_command(self):
        self.parse_command()
        self.start_check()
        trace_id = uuid()
        ret = False
        try:
            log_directory = os.path.join(os.path.expanduser("~"), ".obdiag", "log")
            if not os.path.exists(log_directory):
                os.makedirs(log_directory, exist_ok=True)
            log_path = os.path.join(log_directory, 'obdiag.log')
            if self.enable_log:
                ROOT_IO.init_trace_logger(log_path, 'obdiag', self.trace_id)
            ROOT_IO.track_limit += 1
            ROOT_IO.verbose('cmd: %s' % self.prev_cmd)
            ROOT_IO.verbose('opts: %s' % self.opts)
            custom_config_env_list = Util.get_option(self.opts, 'config')
            config_path = os.path.expanduser('~/.obdiag/config.yml')
            config_password = Util.get_option(self.opts, 'config_password')
            if custom_config_env_list is None:
                custom_config = Util.get_option(self.opts, 'c')
                if custom_config:
                    if os.path.exists(os.path.abspath(custom_config)):
                        config_path = custom_config
                    else:
                        ROOT_IO.error('The option you provided with -c: {0} is not exist.'.format(custom_config))
                        return
            obdiag = ObdiagHome(stdio=ROOT_IO, config_path=config_path, inner_config_change_map=self.inner_config_change_map, custom_config_env_list=custom_config_env_list, config_password=config_password)
            ROOT_IO.print('obdiag version: {}'.format(OBDIAG_VERSION))
            obdiag.set_options(self.opts)
            obdiag.set_cmds(self.cmds)
            ret = None
            try:
                ret = self._do_command(obdiag)
                exit_code = 0
            except Exception as e:
                ROOT_IO.exception(e)
                ROOT_IO.error('command failed. Please contact OceanBase community. e: {0}'.format(e))
                ret = ObdiagResult(code=ObdiagResult.SERVER_ERROR_CODE, error_data="command failed. Please contact OceanBase community. e: {0}".format(e))
                exit_code = 1
            # if silent is true ,print ret
            if ROOT_IO.silent:
                if isinstance(ret, ObdiagResult) is False:
                    ROOT_IO.error('The return value of the command is not ObdiagResult. Please contact OceanBase community. The return value is: {0}'.format(ret))
                    ret = ObdiagResult(code=ObdiagResult.SERVER_ERROR_CODE, error_data="The return value of the command is not ObdiagResult. Maybe the command not support silent. Please contact thebase community.")
                ret.set_trace_id(self.trace_id)

                def args_to_str(args):
                    args_str = ""
                    for arg in args:
                        args_str += arg + " "
                    return args_str.strip()

                ret.set_command(self.prev_cmd + " " + args_to_str(self.args))
                ROOT_IO.set_silent(False)
                # get silent_type
                silent_type = self.inner_config_change_map.get("obdiag", {}).get("logger", {}).get("silent_type", None)
                if silent_type == "md":
                    ROOT_IO.print(ret.get_result_md())
                else:
                    ROOT_IO.print(ret.get_result())
                ROOT_IO.set_silent(True)
            if self.has_trace:
                ROOT_IO.print('Trace ID: %s' % self.trace_id)
                ROOT_IO.print('If you want to view detailed obdiag logs, please run: {0} display-trace {1}'.format(obdiag_bin, self.trace_id))
            telemetry.put_data()
            # check obdiag new version
            if not ROOT_IO.silent:
                check_new_obdiag_version(ROOT_IO)
            if ROOT_IO.silent:
                if ret.get_code() == ObdiagResult.SUCCESS_CODE:
                    return True
                else:
                    return False
            return True
        except NotImplementedError:
            ROOT_IO.exception('command \'%s\' is not implemented' % self.prev_cmd)
        except SystemExit:
            pass
        except KeyboardInterrupt:
            ROOT_IO.exception('Keyboard Interrupt')
        except:
            e = sys.exc_info()[1]
            ROOT_IO.exception('Running Error: %s' % e)

    def _do_command(self, obdiag):
        raise NotImplementedError

    def get_white_ip_list(self):
        if self.opts.white:
            return self.opts.white.split(',')
        ROOT_IO.warn("Security Risk: the whitelist is empty and anyone can request this program!")
        if ROOT_IO.confirm("Do you want to continue?"):
            return []
        wthite_ip_list = ROOT_IO.read("Please enter the whitelist, eq: '192.168.1.1'")
        raise wthite_ip_list.split(',')


class DisplayTraceCommand(ObdiagOriginCommand):

    def __init__(self):
        super(DisplayTraceCommand, self).__init__('display-trace', 'display trace_id log.')
        self.has_trace = False

    @property
    def enable_log(self):
        return False

    def _do_command(self, obdiag):
        from src.common.ssh import LocalClient

        if not self.cmds:
            return self._show_help()
        log_dir = os.path.expanduser('~/.obdiag/log')
        trace_id = self.cmds[0]
        ROOT_IO.verbose('Get log by trace_id')
        try:
            if UUID(trace_id).version != 1:
                ROOT_IO.critical('%s is not trace id' % trace_id)
                return False
        except:
            ROOT_IO.print('%s is not trace id' % trace_id)
            return False
        cmd = 'cd {} && grep -h "\[{}\]" $(ls -tr {}*) | sed "s/\[{}\] //g" '.format(log_dir, trace_id, log_dir, trace_id)
        data = LocalClient.execute_command(cmd)
        ROOT_IO.print(data.stdout)
        return True


obdiag_bin = "obdiag"


class MajorCommand(BaseCommand):

    def __init__(self, name, summary):
        super(MajorCommand, self).__init__(name, summary)
        self.commands = {}

    def _mk_usage(self):
        if self.commands:
            usage = ['%s <command> [options]\n\nAvailable commands:\n' % self.prev_cmd]
            commands = [x for x in self.commands.values() if not (hasattr(x, 'hidden') and x.hidden)]
            commands.sort(key=lambda x: x.name)
            for command in commands:
                if command.hidden is False:
                    usage.append("%-12s %s\n" % (command.name, command.summary))
            self.parser.set_usage('\n'.join(usage))
        return super(MajorCommand, self)._mk_usage()

    def do_command(self):
        if not self.is_init:
            ROOT_IO.error('%s command not init' % self.prev_cmd)
            raise SystemExit('command not init')
        if len(self.args) < 1:
            ROOT_IO.print('You need to give some commands.\n\nTry `obdiag --help` for more information.')
            self._show_help()
            return False
        base, args = self.args[0], self.args[1:]
        if base not in self.commands:
            self.parse_command()
            self._show_help()
            return False
        cmd = '%s %s' % (self.prev_cmd, base)
        ROOT_IO.track_limit += 1
        global obdiag_bin
        obdiag_bin_list = cmd.split()
        obdiag_bin = obdiag_bin_list[0]
        if "main.py" in cmd:
            telemetry.work_tag = False
        telemetry.push_cmd_info("cmd: {0}. args:{1}".format(cmd, args))
        return self.commands[base].init(cmd, args).do_command()

    def register_command(self, command):
        self.commands[command.name] = command


class ObdiagGatherAllCommand(ObdiagOriginCommand):

    def init(self, cmd, args):
        super(ObdiagGatherAllCommand, self).init(cmd, args)
        return self

    def __init__(self):
        super(ObdiagGatherAllCommand, self).__init__('all', 'Gather OceanBase diagnostic info')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--grep', action="append", type='string', help="specify keywords constrain")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('--temp_dir', type='string', help='the dir for temporarily storing files on nodes', default='/tmp')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherAllCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_all', self.opts)


class ObdiagGatherLogCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherLogCommand, self).__init__('log', 'Gather OceanBase logs from OceanBase machines')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--scope', type='string', help="log type constrains, choices=[observer, election, rootservice, all]", default='all')
        self.parser.add_option('--grep', action="append", type='string', help="specify keywords constrain")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('--temp_dir', type='string', help='the dir for temporarily storing files on nodes', default='/tmp')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')
        self.parser.add_option('--redact', type='string', help='desensitization options', default='')

    def init(self, cmd, args):
        super(ObdiagGatherLogCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_log', self.opts)


class ObdiagGatherParameterCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherParameterCommand, self).__init__('parameter', 'Gather OceanBase parameters from OceanBase database')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherParameterCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_parameters', self.opts)


class ObdiagGatherVariableCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherVariableCommand, self).__init__('variable', 'Gather OceanBase variables from OceanBase database')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherVariableCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_variables', self.opts)


class ObdiagGatherSysStatCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherSysStatCommand, self).__init__('sysstat', 'Gather Host information')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherSysStatCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_sysstat', self.opts)


class ObdiagGatherStackCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherStackCommand, self).__init__('stack', 'Gather stack')

        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherStackCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_obstack', self.opts)


class ObdiagGatherPerfCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherPerfCommand, self).__init__('perf', 'Gather perf')

        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('--scope', type='string', help="perf type constrains, choices=[sample, flame, pstack, all]", default='all')
        self.parser.add_option('--count', type='int', help="perf event period to sample >= 1000000", default='100000000')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherPerfCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_perf', self.opts)


class ObdiagGatherSlogCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherSlogCommand, self).__init__('slog', 'Gather slog')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherSlogCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_slog', self.opts)


class ObdiagGatherClogCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherClogCommand, self).__init__('clog', 'Gather clog')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherClogCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_clog', self.opts)


class ObdiagGatherAwrCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherAwrCommand, self).__init__('awr', 'Gather ParalleSQL information')
        self.parser.add_option('--cluster_name', type='string', help='cluster_name from ocp')
        self.parser.add_option('--cluster_id', type='string', help='cluster_id from ocp')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherAwrCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_awr', self.opts)


class ObdiagGatherPlanMonitorCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherPlanMonitorCommand, self).__init__('plan_monitor', 'Gather ParalleSQL information')
        self.parser.add_option('--trace_id', type='string', help='sql trace id')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('--env', type='string', help='''env, eg: "{db_connect='-h127.0.0.1 -P2881 -utest@test -p****** -Dtest'}"''')
        self.parser.add_option('--skip', type='string', help="choices=[dbms_xplan]")
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherPlanMonitorCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_plan_monitor', self.opts)


class ObdiagGatherObproxyLogCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherObproxyLogCommand, self).__init__('obproxy_log', 'Gather obproxy log from obproxy machines')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--scope', type='string', help="log type constrains, choices=[obproxy, obproxy_limit, obproxy_stat, obproxy_digest, obproxy_slow, obproxy_diagnosis, obproxy_error, all]", default='all')
        self.parser.add_option('--grep', action="append", type='string', help="specify keywords constrain")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherObproxyLogCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_obproxy_log(self.opts)


class ObdiagGatherSceneListCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherSceneListCommand, self).__init__('list', 'gather scene list')

    def init(self, cmd, args):
        super(ObdiagGatherSceneListCommand, self).init(cmd, args)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_scenes_list(self.opts)


class ObdiagGatherSceneRunCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherSceneRunCommand, self).__init__('run', 'gather scene run')
        self.parser.add_option('--scene', type='string', help="Specify the scene to be gather")
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--env', type='string', help='env, eg: "{env1=xxx, env2=xxx}"')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('--temp_dir', type='string', help='the dir for temporarily storing files on nodes', default='/tmp')
        self.parser.add_option('--skip_type', type='string', help='The types of gather to be skipped, choices=[ssh, sql]')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')
        self.parser.add_option('--redact', type='string', help='desensitization options', default='')

    def init(self, cmd, args):
        super(ObdiagGatherSceneRunCommand, self).init(cmd, args)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_scenes_run', self.opts)


class ObdiagGatherAshReportCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherAshReportCommand, self).__init__('ash', 'Gather ash report')
        self.parser.add_option('--trace_id', type='string', help="The TRACE.ID of the SQL to be sampled, if left blank or filled with NULL, indicates that TRACE.ID is not restricted.")
        self.parser.add_option('--sql_id', type='string', help="The SQL.ID, if left blank or filled with NULL, indicates that SQL.ID is not restricted.")
        # WAIT_CLASS
        self.parser.add_option('--wait_class', type='string', help='Event types to be sampled.')
        self.parser.add_option('--report_type', type='string', help='Report type, currently only supports text and html （need 4.2.4.0 or higer) type.', default='TEXT')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherAshReportCommand, self).init(cmd, args)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_ash_report', self.opts)


class ObdiagGatherTableDumpHandler(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherTableDumpHandler, self).__init__('tabledump', 'gather tabledump')
        self.parser.add_option('--database', type='string', help="Specifies the name of the database to connect to.")
        self.parser.add_option('--table', type='string', help="Specifies the name of the table in the database to operate on.")
        self.parser.add_option('--user', type='string', help="The username to use for the database connection.")
        self.parser.add_option('--password', type='string', help="The password for the database user. If not specified, an attempt will be made to connect without a password.", default='')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./obdiag_gather_report')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherTableDumpHandler, self).init(cmd, args)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_tabledump', self.opts)


class ObdiagGatherDBMSXPLANHandler(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagGatherDBMSXPLANHandler, self).__init__('dbms_xplan', 'gather dbms_xplan')
        self.parser.add_option('--trace_id', type='string', help="Specifies the name of the database to connect to.")
        self.parser.add_option('--scope', type='string', help="choices=[opt_trace, display_cursor, all]", default='all')
        self.parser.add_option('--user', type='string', help="The username to use for the database connection.")
        self.parser.add_option('--password', type='string', help="The password for the database user. If not specified, an attempt will be made to connect without a password.", default='')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherDBMSXPLANHandler, self).init(cmd, args)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_dbms_xplan', self.opts)


class ObdiagGatherCoreCommand(ObdiagOriginCommand):
    def __init__(self):
        super(ObdiagGatherCoreCommand, self).__init__('core', 'Gather core diagnostic information')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes before to now. format: <n><m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagGatherCoreCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.gather_function('gather_core', self.opts)


class ObdiagDisplaySceneListCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagDisplaySceneListCommand, self).__init__('list', 'display scene list')

    def init(self, cmd, args):
        super(ObdiagDisplaySceneListCommand, self).init(cmd, args)
        return self

    def _do_command(self, obdiag):
        return obdiag.display_scenes_list(self.opts)


class ObdiagDisplaySceneRunCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagDisplaySceneRunCommand, self).__init__('run', 'display scene run')
        self.parser.add_option('--scene', type='string', help="Specify the scene to be display")
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--env', action="append", type='string', help='env options Format: --env key=value')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagDisplaySceneRunCommand, self).init(cmd, args)
        return self

    def _do_command(self, obdiag):
        return obdiag.display_function('display_scenes_run', self.opts)


class ObdiagAnalyzeLogCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagAnalyzeLogCommand, self).__init__('log', 'Analyze OceanBase log from online observer machines or offline OceanBase log files')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--scope', type='string', help="log type constrains, choices=[observer, election, rootservice, all]", default='all')
        self.parser.add_option('--grep', action="append", type='string', help="specify keywords constrain")
        self.parser.add_option('--log_level', type='string', help="OceanBase logs greater than or equal to this level will be analyze, choices=[DEBUG, TRACE, INFO, WDIAG, WARN, EDIAG, ERROR]")
        self.parser.add_option('--files', action="append", type='string', help="specify files")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--temp_dir', type='string', help='the dir for temporarily storing files on nodes', default='/tmp')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagAnalyzeLogCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        offline_args_sign = '--files'
        if self.args and (offline_args_sign in self.args):
            return obdiag.analyze_fuction('analyze_log_offline', self.opts)
        else:
            return obdiag.analyze_fuction('analyze_log', self.opts)


class ObdiagAnalyzeQueueCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagAnalyzeQueueCommand, self).__init__('queue', 'Analyze oceanbase log from online observer machines to registration queue')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--tenant', type='string', help="Specify tenantname ")
        self.parser.add_option('--queue', type='int', help="quene size ", default=50)

    def init(self, cmd, args):
        super(ObdiagAnalyzeQueueCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.analyze_fuction('analyze_queue', self.opts)


class ObdiagAnalyzeFltTraceCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagAnalyzeFltTraceCommand, self).__init__('flt_trace', 'Analyze OceanBase trace.log from online observer machines or offline OceanBase trace.log files')
        self.parser.add_option('--flt_trace_id', type='string', help="flt trace id, . format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx")
        self.parser.add_option('--files', action="append", help="specify files")
        self.parser.add_option('--top', type='string', help="top leaf span", default=5)
        self.parser.add_option('--recursion', type='string', help="Maximum number of recursion", default=8)
        self.parser.add_option('--output', type='string', help="Print the result to the maximum output line on the screen", default=60)
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('--temp_dir', type='string', help='the dir for temporarily storing files on nodes', default='/tmp')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagAnalyzeFltTraceCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.analyze_fuction('analyze_flt_trace', self.opts)


class ObdiagAnalyzeParameterDiffCommand(ObdiagOriginCommand):
    def __init__(self):
        super(ObdiagAnalyzeParameterDiffCommand, self).__init__('diff', 'Analyze the parameter configurations between observers and identify the parameters with different values among the observers')
        self.parser.add_option('--file', type='string', help="specify initialization parameter file")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagAnalyzeParameterDiffCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.analyze_fuction('analyze_parameter_diff', self.opts)


class ObdiagAnalyzeParameterDefaultCommand(ObdiagOriginCommand):
    def __init__(self):
        super(ObdiagAnalyzeParameterDefaultCommand, self).__init__('default', 'Analyze the parameter to identify parameters with non-default values')
        self.parser.add_option('--file', type='string', help="specify initialization parameter file")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagAnalyzeParameterDefaultCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.analyze_fuction('analyze_parameter_default', self.opts)


class ObdiagAnalyzeParameterCommand(MajorCommand):
    def __init__(self):
        super(ObdiagAnalyzeParameterCommand, self).__init__('parameter', 'Analyze OceanBase parameters info')
        self.register_command(ObdiagAnalyzeParameterDiffCommand())
        self.register_command(ObdiagAnalyzeParameterDefaultCommand())


class ObdiagAnalyzeVariableDiffCommand(ObdiagOriginCommand):
    def __init__(self):
        super(ObdiagAnalyzeVariableDiffCommand, self).__init__('diff', 'Analyze and identify variables that have changed compared to the specified variable file')
        self.parser.add_option('--file', type='string', help="specify initialization variable file")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagAnalyzeVariableDiffCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.analyze_fuction('analyze_variable_diff', self.opts)


class ObdiagAnalyzeVariableCommand(MajorCommand):
    def __init__(self):
        super(ObdiagAnalyzeVariableCommand, self).__init__('variable', 'Analyze OceanBase variables info')
        self.register_command(ObdiagAnalyzeVariableDiffCommand())


class ObdiagAnalyzeMemoryCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagAnalyzeMemoryCommand, self).__init__('memory', 'Analyze OceanBase Memory info from online observer machines or offline OceanBase log files')
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--files', action="append", type='string', help="specify files")
        self.parser.add_option('--store_dir', type='string', help='the dir to store gather result, current dir by default.', default='./')
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--temp_dir', type='string', help='the dir for temporarily storing files on nodes', default='/tmp')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')
        self.parser.add_option('--version', type="string", help='specify the OceanBase version of the log file to be analyzed.This option is only used for offline analysis.')

    def init(self, cmd, args):
        super(ObdiagAnalyzeMemoryCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        offline_args_sign = '--files'
        if self.args and (offline_args_sign in self.args):
            return obdiag.analyze_fuction('analyze_memory_offline', self.opts)
        else:
            return obdiag.analyze_fuction('analyze_memory', self.opts)


class ObdiagAnalyzeIndexSpaceCommand(ObdiagOriginCommand):
    def __init__(self):
        super(ObdiagAnalyzeIndexSpaceCommand, self).__init__('index_space', 'Analyze the space of existing or non-existent index and estimate it through the columns included in the index')
        self.parser.add_option('--tenant_name', type='string', help="tenant name")
        self.parser.add_option('--table_name', type='string', help="table name")
        self.parser.add_option('--index_name', type='string', help="specify the index name if an index already exists in the table")
        self.parser.add_option('--column_names', type='string', help="specify the column names of index that have not been created yet;eg:--column_names=c1,c2,c3")
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagAnalyzeIndexSpaceCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.analyze_fuction('analyze_index_space', self.opts)


class ObdiagAnalyzeSQLCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagAnalyzeSQLCommand, self).__init__('sql', 'Analyze OceanBase sql from sql_audit ')
        self.parser.add_option('--tenant_name', type='string', help="tenant name")
        self.parser.add_option('--host', type='string', help="tenant connection host")
        self.parser.add_option('--port', type='string', help="tenant connection port")
        self.parser.add_option('--password', type='string', help="tenant connection user password", default='')
        self.parser.add_option('--user', type='string', help="tenant connection user name")
        self.parser.add_option('--from', type='string', help="specify the start of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--to', type='string', help="specify the end of the time range. format: 'yyyy-mm-dd hh:mm:ss'")
        self.parser.add_option('--since', type='string', help="Specify time range that from 'n' [d]ays, 'n' [h]ours or 'n' [m]inutes. before to now. format: <n> <m|h|d>. example: 1h.", default='30m')
        self.parser.add_option('--level', type='string', help="The alarm level, optional parameters [critical, warn, notice, ok]", default='notice')
        self.parser.add_option('--output', type='string', help="The format of the output results, choices=[json, html]", default='html')
        self.parser.add_option('--limit', type='string', help="The limit on the number of data rows returned by sql_audit for the tenant.", default=2000)
        self.parser.add_option('--store_dir', type='string', help='the dir to store result, current dir by default.', default='./obdiag_analyze/')
        self.parser.add_option('--elapsed_time', type='string', help='The minimum threshold for filtering execution time, measured in microseconds.', default=100000)
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagAnalyzeSQLCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.analyze_fuction('analyze_sql', self.opts)


class ObdiagAnalyzeSQLReviewCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagAnalyzeSQLReviewCommand, self).__init__('sql_review', 'Analyze OceanBase sql from file')
        self.parser.add_option('--host', type='string', help="tenant connection host")
        self.parser.add_option('--port', type='string', help="tenant connection port")
        self.parser.add_option('--password', type='string', help="tenant connection user password", default='')
        self.parser.add_option('--user', type='string', help="tenant connection user name")
        self.parser.add_option('--files', type='string', action="append", help="specify files")
        self.parser.add_option('--level', type='string', help="The alarm level, optional parameters [critical, warn, notice, ok]", default='notice')
        self.parser.add_option('--output', type='string', help="The format of the output results, choices=[json, html]", default='html')
        self.parser.add_option('--store_dir', type='string', help='the dir to store result, current dir by default.', default='./obdiag_analyze/')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')

    def init(self, cmd, args):
        super(ObdiagAnalyzeSQLReviewCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.analyze_fuction('analyze_sql_review', self.opts)


class ObdiagCheckRunCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagCheckRunCommand, self).__init__('run', 'check OceanBase cluster')
        self.parser.add_option('--cases', type='string', help="check observer's cases on package_file")
        self.parser.add_option('--obproxy_cases', type='string', help="check obproxy's cases on package_file")
        self.parser.add_option('--store_dir', type='string', help='the dir to store check result, current dir by default.', default='./check_report/')
        self.parser.add_option('--report_type', type='string', help='The type of the check report, support "table", "json", "xml", "yaml". "html", default table', default='table')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')
        self.parser.add_option('--env', action="append", type='string', help='env of scene')

    def init(self, cmd, args):
        super(ObdiagCheckRunCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):

        return obdiag.check(self.opts)


class ObdiagCheckListCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagCheckListCommand, self).__init__('list', 'check list')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')
        # self.parser.add_option('--all', action="append", type='string', help='env of scene')

    def init(self, cmd, args):
        super(ObdiagCheckListCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.check_list(self.opts)


class ObdiagRCARunCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagRCARunCommand, self).__init__('run', 'root cause analysis')
        self.parser.add_option('--scene', type='string', help="rca scene name. The argument is required.")
        self.parser.add_option('--store_dir', type='string', help='the dir to store rca result, current dir by default.', default='./obdiag_rca/')
        self.parser.add_option('--env', action='callback', type='string', callback=self._env_scene, help='env of scene')
        self.parser.add_option('--report_type', type='string', help='The type of the rca report, support "table", "json", "xml", "yaml", "html". default table', default='table')
        self.parser.add_option('-c', type='string', help='obdiag custom config', default=os.path.expanduser('~/.obdiag/config.yml'))
        self.parser.add_option('--config', action="append", type="string", help='config options Format: --config key=value')
        self.scene_input_param_map = {}

    def _env_scene(self, option, opt_str, value, parser):
        """
        env of scene
        """
        try:
            # env option is json format
            try:
                self.scene_input_param_map = json.loads(value)
                return
            except Exception as e:
                ROOT_IO.verbose("env option {0} is not json.".format(value))

            # env option is key=val format
            key, val = value.split('=', 1)
            if key is None or key == "":
                return
            m = self._env_scene_set(key, val)

            def _scene_input_param(param_map, scene_param_map):
                for scene_param_map_key, scene_param_map_value in scene_param_map.items():
                    if scene_param_map_key in param_map:
                        if isinstance(scene_param_map_value, dict):
                            _scene_input_param(param_map[scene_param_map_key], scene_param_map_value)
                        else:
                            param_map[scene_param_map_key] = scene_param_map_value
                    else:
                        param_map[scene_param_map_key] = scene_param_map_value
                return param_map

            self.scene_input_param_map = _scene_input_param(self.scene_input_param_map, m)
        except Exception as e:
            raise Exception("Key or val ({1}) is illegal: {0}".format(e, value))

    def _env_scene_set(self, key, val):
        def recursion(param_map, key, val):
            if key is None or key == "":
                raise Exception("key is None")
            if val is None or val == "":
                raise Exception("val is None")
            if key.startswith(".") or key.endswith("."):
                raise Exception("Key starts or ends '.'")
            if "." in key:
                map_key = key.split(".")[0]
                param_map[map_key] = recursion({}, key[len(map_key) + 1 :], val)
                return param_map
            else:
                param_map[key] = val
                return param_map

        return recursion({}, key, val)

    def init(self, cmd, args):
        super(ObdiagRCARunCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        Util.set_option(self.opts, 'env', self.scene_input_param_map)
        return obdiag.rca_run(self.opts)


class ObdiagRCAListCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagRCAListCommand, self).__init__('list', 'show list of rca list')

    def init(self, cmd, args):
        super(ObdiagRCAListCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.rca_list(self.opts)


class ObdiagConfigCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagConfigCommand, self).__init__('config', 'Quick build config')
        self.parser.add_option('-h', type='string', help="database host")
        self.parser.add_option('-u', type='string', help='sys_user', default='root@sys')
        self.parser.add_option('-p', type='string', help="password", default='')
        self.parser.add_option('-P', type='string', help="port")

    def init(self, cmd, args):
        super(ObdiagConfigCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.config(self.opts)


class ObdiagUpdateCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagUpdateCommand, self).__init__('update', 'Update cheat files')
        self.parser.add_option('--file', type='string', help="obdiag update cheat file path. Please note that you need to ensure the reliability of the files on your own.")
        self.parser.add_option(
            '--force',
            action='store_true',
            help='You can force online upgrades by adding --force in the command',
        )

    def init(self, cmd, args):
        super(ObdiagUpdateCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.update(self.opts)


class ObdiagToolCryptoConfigCommand(ObdiagOriginCommand):

    def __init__(self):
        super(ObdiagToolCryptoConfigCommand, self).__init__('crypto_config', 'obdiag tool crypto. Crypto the config.yaml of obdiag file')
        self.parser.add_option('--key', type='string', help="the key to encrypt or decrypt")
        self.parser.add_option('--file', type='string', help="the file path to encrypt or decrypt")
        self.parser.add_option('--encrypted_file', type='string', help="the file path to encrypt")

    def init(self, cmd, args):
        super(ObdiagToolCryptoConfigCommand, self).init(cmd, args)
        self.parser.set_usage('%s [options]' % self.prev_cmd)
        return self

    def _do_command(self, obdiag):
        return obdiag.tool_crypto_config(self.opts)


class ObdiagGatherCommand(MajorCommand):

    def __init__(self):
        super(ObdiagGatherCommand, self).__init__('gather', 'Gather OceanBase diagnostic info')
        self.register_command(ObdiagGatherAllCommand())
        self.register_command(ObdiagGatherLogCommand())
        self.register_command(ObdiagGatherSysStatCommand())
        self.register_command(ObdiagGatherStackCommand())
        self.register_command(ObdiagGatherPerfCommand())
        self.register_command(ObdiagGatherSlogCommand())
        self.register_command(ObdiagGatherClogCommand())
        self.register_command(ObdiagGatherPlanMonitorCommand())
        self.register_command(ObdiagGatherAwrCommand())
        self.register_command(ObdiagGatherObproxyLogCommand())
        self.register_command(ObdiagGatherSceneCommand())
        self.register_command(ObdiagGatherAshReportCommand())
        self.register_command(ObdiagGatherTableDumpHandler())
        self.register_command(ObdiagGatherParameterCommand())
        self.register_command(ObdiagGatherVariableCommand())
        self.register_command(ObdiagGatherDBMSXPLANHandler())
        self.register_command(ObdiagGatherCoreCommand())


class ObdiagDisplayCommand(MajorCommand):

    def __init__(self):
        super(ObdiagDisplayCommand, self).__init__('display', 'display oceanbase info')
        self.register_command(ObdiagDisplaySceneCommand())


class ObdiagDisplaySceneCommand(MajorCommand):

    def __init__(self):
        super(ObdiagDisplaySceneCommand, self).__init__('scene', 'Display scene diagnostic info')
        self.register_command(ObdiagDisplaySceneListCommand())
        self.register_command(ObdiagDisplaySceneRunCommand())


class ObdiagGatherSceneCommand(MajorCommand):

    def __init__(self):
        super(ObdiagGatherSceneCommand, self).__init__('scene', 'Gather scene diagnostic info')
        self.register_command(ObdiagGatherSceneListCommand())
        self.register_command(ObdiagGatherSceneRunCommand())


class ObdiagAnalyzeCommand(MajorCommand):

    def __init__(self):
        super(ObdiagAnalyzeCommand, self).__init__('analyze', 'Analyze OceanBase diagnostic info')
        self.register_command(ObdiagAnalyzeLogCommand())
        self.register_command(ObdiagAnalyzeFltTraceCommand())
        self.register_command(ObdiagAnalyzeParameterCommand())
        self.register_command(ObdiagAnalyzeVariableCommand())
        self.register_command(ObdiagAnalyzeQueueCommand())
        self.register_command(ObdiagAnalyzeIndexSpaceCommand())
        self.register_command(ObdiagAnalyzeMemoryCommand())
        # self.register_command(ObdiagAnalyzeSQLCommand())
        # self.register_command(ObdiagAnalyzeSQLReviewCommand())


class ObdiagRCACommand(MajorCommand):

    def __init__(self):
        super(ObdiagRCACommand, self).__init__('rca', 'root cause analysis')
        self.register_command(ObdiagRCARunCommand())
        self.register_command(ObdiagRCAListCommand())


class ObdiagCheckCommand(MajorCommand):

    def __init__(self):
        super(ObdiagCheckCommand, self).__init__('check', 'Check OceanBase cluster info')
        self.register_command(ObdiagCheckRunCommand())
        self.register_command(ObdiagCheckListCommand())


class ToolCommand(MajorCommand):
    def __init__(self):
        super(ToolCommand, self).__init__('tool', 'obdiag tool')
        self.register_command(ObdiagToolCryptoConfigCommand())


class MainCommand(MajorCommand):

    def __init__(self):
        super(MainCommand, self).__init__('obdiag', '')
        self.register_command(DisplayTraceCommand())
        self.register_command(ObdiagGatherCommand())
        self.register_command(ObdiagAnalyzeCommand())
        self.register_command(ObdiagDisplayCommand())
        self.register_command(ObdiagCheckCommand())
        self.register_command(ObdiagRCACommand())
        self.register_command(ObdiagConfigCommand())
        self.register_command(ObdiagUpdateCommand())
        self.register_command(ToolCommand())
        self.parser.version = get_obdiag_version()
        self.parser._add_version_option()
