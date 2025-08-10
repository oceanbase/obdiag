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
@time: 2024/6/24
@file: kubernetes_client.py
@desc:
"""
import os
import select
import tarfile
import tempfile
from tempfile import TemporaryFile
from src.common.ssh_client.base import SsherClient
from kubernetes import client, config
from kubernetes.stream import stream
from kubernetes.stream.ws_client import STDERR_CHANNEL, STDOUT_CHANNEL
from websocket import ABNF


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
            self.stdio.error("KubernetesClient can't get the resp by {0}: {1}".format(cmd, e))
            raise e

    def download(self, remote_path, local_path):
        return self.__download_file_from_pod(self.namespace, self.pod_name, self.container_name, remote_path, local_path)

    def __download_file_from_pod(self, namespace, pod_name, container_name, file_path, local_path):
        dir = os.path.dirname(file_path)
        bname = os.path.basename(file_path)
        exec_command = ['/bin/sh', '-c', f'cd {dir}; tar cf - {bname}']

        with TemporaryFile() as tar_buffer:
            exec_stream = stream(self.client.connect_get_namespaced_pod_exec, pod_name, namespace, command=exec_command, stderr=True, stdin=True, stdout=True, tty=False, _preload_content=False, container=container_name)
            # Copy file to stream
            try:
                reader = WSFileManager(exec_stream)
                while True:
                    out, err, closed = reader.read_bytes()
                    if out:
                        tar_buffer.write(out)
                    elif err:
                        self.stdio.error("Error copying file {0}".format(err.decode("utf-8", errors='ignore')))
                    if closed:
                        break
                exec_stream.close()
                tar_buffer.flush()
                tar_buffer.seek(0)
                with tarfile.open(fileobj=tar_buffer, mode='r:') as tar:
                    member = tar.getmember(bname)
                    local_path = os.path.dirname(local_path)
                    tar.extract(member, path=local_path)
                return True
            except Exception as e:
                raise e

    def upload(self, remote_path, local_path):
        return self.__upload_file_to_pod(self.namespace, self.pod_name, self.container_name, local_path, remote_path)

    def __upload_file_to_pod(self, namespace, pod_name, container_name, local_path, remote_path):
        self.stdio.verbose("upload file to pod")
        self.stdio.verbose("local_path: {0}".format(local_path))
        remote_path_file_name = os.path.basename(remote_path)
        remote_path = "{0}/".format(os.path.dirname(remote_path))
        self.stdio.verbose("remote_path: {0}".format(remote_path))
        src_path = local_path
        dest_dir = remote_path

        with tempfile.NamedTemporaryFile(delete=False) as temp_tar:
            with tarfile.open(fileobj=temp_tar, mode='w') as tar:
                arcname = os.path.basename(src_path)
                tar.add(src_path, arcname=arcname)
            temp_tar_path = temp_tar.name
        self.stdio.verbose(temp_tar_path)

        try:
            # read tar_data from file
            with open(temp_tar_path, 'rb') as f:
                tar_data = f.read()

            # execute tar command in pod
            command = ["tar", "xvf", "-", "-C", dest_dir]
            ws_client = stream(self.client.connect_get_namespaced_pod_exec, pod_name, namespace, command=command, stderr=True, stdin=True, stdout=True, tty=False, _preload_content=False)

            # send tar_data to pod
            chunk_size = 4096
            for i in range(0, len(tar_data), chunk_size):
                chunk = tar_data[i : i + chunk_size]
                ws_client.write_stdin(chunk)

            # 关闭输入流并等待完成
            # ws_client.write_stdin(None)  # 发送EOF
            while ws_client.is_open():
                ws_client.update(timeout=1)

            # 获取错误输出
            stderr = ws_client.read_channel(2)  # STDERR_CHANNEL=2
            if stderr:
                raise RuntimeError(f"Pod执行错误: {stderr.decode('utf-8')}")
            ws_client.close()
        finally:
            os.remove(temp_tar_path)

        if remote_path_file_name != os.path.basename(local_path):
            self.stdio.verbose("move")
            self.exec_cmd("mv {0}/{1} {0}/{2}".format(remote_path, os.path.basename(local_path), remote_path_file_name))

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        return self.__ssh_invoke_shell_switch_user(new_user, cmd, time_out)

    def __ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        command = ['/bin/sh', '-c', cmd]
        # exec comm
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


class WSFileManager:
    """
    WS wrapper to manage read and write bytes in K8s WSClient
    """

    def __init__(self, ws_client):
        """

        :param wsclient: Kubernetes WSClient
        """
        self.ws_client = ws_client

    def read_bytes(self, timeout=0):
        """
        Read slice of bytes from stream

        :param timeout: read timeout
        :return: stdout, stderr and closed stream flag
        """
        stdout_bytes = None
        stderr_bytes = None

        if self.ws_client.is_open():
            if not self.ws_client.sock.connected:
                self.ws_client._connected = False
            else:
                r, _, _ = select.select((self.ws_client.sock.sock,), (), (), timeout)
                if r:
                    op_code, frame = self.ws_client.sock.recv_data_frame(True)
                    if op_code == ABNF.OPCODE_CLOSE:
                        self.ws_client._connected = False
                    elif op_code == ABNF.OPCODE_BINARY or op_code == ABNF.OPCODE_TEXT:
                        data = frame.data
                        if len(data) > 1:
                            channel = data[0]
                            data = data[1:]
                            if data:
                                if channel == STDOUT_CHANNEL:
                                    stdout_bytes = data
                                elif channel == STDERR_CHANNEL:
                                    stderr_bytes = data
        return stdout_bytes, stderr_bytes, not self.ws_client._connected
