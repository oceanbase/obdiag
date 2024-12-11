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
@time: 2024/6/24
@file: kubernetes_client.py
@desc:
"""

from obdiag.common.ssh_client.base import SsherClient
from kubernetes import client, config
from kubernetes.stream import stream


class KubernetesClient(SsherClient):
    def __init__(self, context=None, node=None):
        super().__init__(context, node)
        try:
            self.namespace = self.node.get("namespace")
            self.pod_name = self.node.get("pod_name")
            self.container_name = self.node.get("container_name") or "observer"
            config_file = self.node.get("kubernetes_config_file")
            if config_file is None or config_file == "":
                context.stdio.verbose("KubernetesClient load_kube_config from default config file in cluster.")
                config.load_incluster_config()
            else:
                context.stdio.verbose("KubernetesClient load_kube_config from {0}".format(config_file))
                config.kube_config.load_kube_config(config_file=config_file)
            self.client = client.CoreV1Api()
        except Exception as e:
            raise Exception("KubernetesClient load_kube_config error. Please check the config. {0}".format(e))

    def exec_cmd(self, cmd):
        exec_command = ['/bin/sh', '-c', cmd]
        self.stdio.verbose("KubernetesClient exec_cmd: {0}".format(cmd))
        try:
            resp = stream(self.client.connect_get_namespaced_pod_exec, self.pod_name, self.namespace, command=exec_command, stderr=True, stdin=False, stdout=True, tty=False, container=self.container_name)
            self.stdio.verbose("KubernetesClient exec_cmd.resp: {0}".format(resp))
            if "init system (PID 1). Can't operate." in resp:
                return "KubernetesClient can't get the resp by {0}".format(cmd)
            return resp
        except Exception as e:
            return f"KubernetesClient can't get the resp by {cmd}: {str(e)}"

    def download(self, remote_path, local_path):
        return self.__download_file_from_pod(self.namespace, self.pod_name, self.container_name, remote_path, local_path)

    def __download_file_from_pod(self, namespace, pod_name, container_name, file_path, local_path):
        exec_command = ['tar', 'cf', '-', '-C', '/', file_path]
        resp = stream(self.client.connect_get_namespaced_pod_exec, pod_name, namespace, command=exec_command, stderr=True, stdin=False, stdout=True, tty=False, container=container_name, _preload_content=False)
        with open(local_path, 'wb') as file:
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    out = resp.read_stdout()
                    file.write(out.encode('utf-8'))
                if resp.peek_stderr():
                    err = resp.read_stderr()
                    self.stdio.error("ERROR: ", err)
                    break
            resp.close()

    def upload(self, remote_path, local_path):
        return self.__upload_file_to_pod(self.namespace, self.pod_name, self.container_name, local_path, remote_path)

    def __upload_file_to_pod(self, namespace, pod_name, container_name, local_path, remote_path):
        config.load_kube_config()
        v1 = client.CoreV1Api()
        exec_command = ['tar', 'xvf', '-', '-C', '/', remote_path]
        with open(local_path, 'rb') as file:
            resp = stream(v1.connect_get_namespaced_pod_exec, pod_name, namespace, command=exec_command, stderr=True, stdin=True, stdout=True, tty=False, container=container_name, _preload_content=False)
            # Support data flow for tar command
            commands = []
            commands.append(file.read())
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    self.stdio.verbose("STDOUT: %s" % resp.read_stdout())
                if resp.peek_stderr():
                    self.stdio.error("STDERR: %s" % resp.read_stderr())
                if commands:
                    c = commands.pop(0)
                    resp.write_stdin(c)
                else:
                    break
            resp.close()

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        return self.__ssh_invoke_shell_switch_user(new_user, cmd, time_out)

    def __ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        command = ['/bin/sh', '-c', cmd]
        # 构建执行tar命令串，该命令串在切换用户后执行
        exec_command = ['su', '-u', new_user, "&"] + command
        resp = stream(self.client.connect_get_namespaced_pod_exec, self.pod_name, self.namespace, command=exec_command, stderr=True, stdin=False, stdout=True, tty=False, container=self.container_name)
        parts = resp.split('\n', maxsplit=1)
        if len(parts) < 2:
            return ""
        result = parts[1]
        return result

    def get_name(self):
        return "kubernetes_{0}_{1}".format(self.namespace, self.pod_name)

    def get_ip(self):
        if self.node.get("ip") is None:
            raise Exception("kubernetes need set the ip of observer")
        return self.node.get("ip")
