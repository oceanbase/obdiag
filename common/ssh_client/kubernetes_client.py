from common.ssh_client.ssh import SsherClient
from kubernetes import client, config
from kubernetes.stream import stream


class KubernetesClient(SsherClient):
    def __init__(self, context=None, node=None):
        super().__init__(context, node)
        # TODO support other config_file
        config.kube_config.load_kube_config()
        self.namespace = self.node.get("namespace")
        self.pod_name = self.node.get("pod_name")
        self.container_name = self.node.get("container_name")
        self.client = client.CoreV1Api()

    def exec_cmd(self, cmd):
        exec_command = ['/bin/sh', '-c', cmd]
        resp = stream(self.client.connect_get_namespaced_pod_exec, self.pod_name, self.namespace, command=exec_command, stderr=True, stdin=False, stdout=True, tty=False, container=self.container_name)
        parts = resp.split('\n', maxsplit=1)
        if len(parts) < 2:
            return ""
        result = parts[1]
        return result

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
            # 支持tar命令的数据流
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
        raise Exception("the client type is not support ssh invoke shell switch user")

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
