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
@time: 2024/01/17
@file: cpu_high.py
@desc:
"""
import os
from utils.shell_utils import SshHelper
from common.logger import logger
from handler.gather.gather_obstack2 import GatherObstack2Handler
from handler.gather.gather_perf import GatherPerfHandler
from utils.parser_utils import ParserAction
from handler.gather.gather_log import GatherLogHandler

class CPUHighScene(object):
    def __init__(self, nodes, cluster, report_path, task_variable_dict=None, args=None, env={}):
        if task_variable_dict is None:
            self.task_variable_dict = {}
        else:
            self.task_variable_dict = task_variable_dict
        self.nodes = nodes
        self.cluster = cluster
        self.report_path = report_path
        self.args = args
        self.env = env
        self.is_ssh = True
        self.ob_nodes = nodes

    def execute(self):
        self.__gather_obstack()
        self.__gather_perf()
        self.__gather_current_clocksource()
        self.__gather_log()

    def __gather_obstack(self):
        logger.info("gather obstack start")
        obstack = GatherObstack2Handler(nodes=self.nodes, gather_pack_dir=self.report_path, is_scene=True)
        obstack.handle(self.args)
        logger.info("gather obstack end")

    def __gather_perf(self):
        logger.info("gather perf start")
        perf = GatherPerfHandler(nodes=self.nodes, gather_pack_dir=self.report_path, is_scene=True)
        self.args = ParserAction.add_attribute_to_namespace(self.args, 'scope', "all")
        perf.handle(self.args)
        logger.info("gather perf end")

    def __gather_current_clocksource(self):
        try:
            logger.info("gather current_clocksource start")
            for node in self.nodes:
                ssh_helper = SshHelper(self.is_ssh, node.get("ip"), node.get("user"), node.get("password"), node.get("port"), node.get("private_key"), node)
                cmd = 'cat /sys/devices/system/clocksource/clocksource0/current_clocksource'
                logger.info("gather current_clocksource, run cmd = [{0}]".format(cmd))
                result = ssh_helper.ssh_exec_cmd(cmd)
                file_path = os.path.join(self.report_path, "current_clocksource_{ip}_result.txt".format(ip=str(node.get("ip")).replace('.', '_')))
                self.report(file_path, cmd, result)
            logger.info("gather current_clocksource end")
        except Exception as e:
            logger.error("SshHandler init fail. Please check the node conf. Exception : {0} .".format(e))

    def __gather_log(self):
        try:
            logger.info("gather observer log start")
            handler = GatherLogHandler(nodes=self.ob_nodes, gather_pack_dir=self.report_path, is_scene=True)
            self.args = ParserAction.add_attribute_to_namespace(self.args, 'grep', None)
            handler.handle(self.args)
            logger.info("gather observer log end")
        except Exception as e:
            logger.error("gather observer log failed, error: {0}".format(e))
            raise Exception("gather observer log failed, error: {0}".format(e))

    def report(self, file_path, command, data):
        try:
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write('\n\n' + 'shell > ' + command + '\n')
                f.write(data + '\n')
        except Exception as e:
            logger.error("report sql result to file: {0} failed, error: ".format(file_path))
