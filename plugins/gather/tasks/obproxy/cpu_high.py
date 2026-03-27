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
@time: 2026/03/26
@file: cpu_high.py
@desc: Gather CPU diagnostics for OBProxy nodes (perf record/script, top, perf top, logs).
       See https://github.com/oceanbase/obdiag/issues/683
"""

import os

from src.common.command import get_obproxy_pid, mkdir
from src.common.command import SshClient
from src.common.stdio import SafeStdio
from src.handler.gather.gather_component_log import GatherComponentLogHandler


class CpuHigh(SafeStdio):
    def init(self, context, scene_name, report_path, task_variable_dict=None, env=None):
        self.context = context
        self.stdio = context.stdio
        self.task_variable_dict = task_variable_dict or {}
        self.report_path = report_path
        self.env = context.get_variable("env") or {}
        self.obproxy_nodes = (context.obproxy_config or {}).get('servers', [])

    def execute(self):
        if not self.obproxy_nodes:
            self.stdio.warn("[obproxy.cpu_high] no obproxy nodes found in config, skip")
            return

        perf_count = self.env.get("perf_count", 100000000)
        try:
            perf_count = int(perf_count)
        except (TypeError, ValueError):
            perf_count = 100000000

        for node in self.obproxy_nodes:
            try:
                self._gather_node(node, perf_count)
            except Exception as e:
                self.stdio.error("[obproxy.cpu_high] node {ip} failed: {err}".format(ip=node.get("ip", "?"), err=e))

        self._gather_log()

    # ------------------------------------------------------------------
    def _gather_node(self, node, perf_count):
        ip = node.get("ip", "unknown")
        self.stdio.print("[obproxy.cpu_high] start gather node {0}".format(ip))

        ssh = SshClient(self.context, node)
        pid = get_obproxy_pid(ssh, self.stdio)
        if not pid:
            self.stdio.warn("[obproxy.cpu_high] cannot get obproxy pid on {0}, skip".format(ip))
            return

        remote_dir = "/tmp/obdiag_obproxy_cpu_{ip}".format(ip=ip.replace(".", "_"))
        mkdir(ssh, remote_dir, self.stdio)

        self.stdio.print("[obproxy.cpu_high] node {0}, pid={1}".format(ip, pid))

        # 1. perf record for flame graph (sample.data → sample.viz)
        self._run(ssh, "perf record -o {d}/sample.data -e cycles -c {c} -p {p} -g -- sleep 20".format(d=remote_dir, c=perf_count, p=pid))
        self._run(ssh, "perf script -i {d}/sample.data -F ip,sym -f > {d}/sample.viz".format(d=remote_dir))

        # 2. perf record for flame graph (flame.data → flame.viz)
        self._run(ssh, "perf record -o {d}/flame.data -F 99 -p {p} -g -- sleep 20".format(d=remote_dir, p=pid))
        self._run(ssh, "perf script -i {d}/flame.data > {d}/flame.viz".format(d=remote_dir))

        # 3. top -Hp (one snapshot of all threads)
        self._run(ssh, "top -Hp {p} -b -n 1 > {d}/top.txt".format(p=pid, d=remote_dir))

        # 4. perf top (10-second window, up to 200 lines)
        self._run(ssh, "timeout 10 perf top -p {p} --stdio 2>&1 | head -200 > {d}/perf_top.txt".format(p=pid, d=remote_dir))

        # download collected data
        local_dir = os.path.join(self.report_path, "obproxy_cpu_{ip}".format(ip=ip.replace(".", "_")))
        os.makedirs(local_dir, exist_ok=True)
        tar_path = "{d}.tar.gz".format(d=remote_dir)
        self._run(ssh, "tar czf {t} -C {d} .".format(t=tar_path, d=remote_dir))

        local_tar = os.path.join(local_dir, "obproxy_cpu_{ip}.tar.gz".format(ip=ip.replace(".", "_")))
        ssh.download(tar_path, local_tar)
        self._run(ssh, "rm -rf {d} {t}".format(d=remote_dir, t=tar_path))

        self.stdio.print("[obproxy.cpu_high] node {0} done, saved to {1}".format(ip, local_tar))

    def _run(self, ssh, cmd):
        self.stdio.verbose("[obproxy.cpu_high] exec: {0}".format(cmd))
        return ssh.exec_cmd(cmd)

    def _gather_log(self):
        try:
            self.stdio.print("[obproxy.cpu_high] gather obproxy log start")
            handler = GatherComponentLogHandler()
            handler.init(self.context, store_dir=self.report_path, target="obproxy", is_scene=True)
            handler.handle()
            self.stdio.print("[obproxy.cpu_high] gather obproxy log end")
        except Exception as e:
            self.stdio.error("[obproxy.cpu_high] gather obproxy log failed: {0}".format(e))


cpu_high = CpuHigh()
