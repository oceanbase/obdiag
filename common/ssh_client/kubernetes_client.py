from common.ssh_client.ssh import SsherClient


class KubernetesClient(SsherClient):
    def __init__(self, context=None, node=None):
        super().__init__(context, node)

    def exec_cmd(self, cmd):

        raise Exception("the client type is not support exec_cmd")

    def download(self, remote_path, local_path):
        raise Exception("the client type is not support download")

    def upload(self, remote_path, local_path):
        raise Exception("the client type is not support upload")

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        raise Exception("the client type is not support ssh invoke shell switch user")
