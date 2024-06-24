from common.ssh_client.ssh import SsherClient
import subprocess32 as subprocess
import shutil


class LocalClient(SsherClient):
    def __init__(self, context=None, node=None):
        super().__init__(context, node)

    def exec_cmd(self, cmd):
        try:
            self.stdio.verbose("[local host] run cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = out.communicate()
            if stderr:
                self.stdio.error("run cmd = [{0}] on localhost, stderr=[{1}]".format(cmd, stderr))
            return stdout
        except:
            self.stdio.error("run cmd = [{0}] on localhost".format(cmd))

    def download(self, remote_path, local_path):
        try:
            shutil.copy(remote_path, local_path)
        except Exception as e:
            self.stdio.error("download file from localhost, remote_path=[{0}], local_path=[{1}], error=[{2}]".format(remote_path, local_path, str(e)))

    def upload(self, remote_path, local_path):
        try:
            shutil.copy(local_path, remote_path)
        except Exception as e:
            self.stdio.error("upload file to localhost, remote _path =[{0}], local _path=[{1}], error=[{2}]".format(remote_path, local_path, str(e)))
            raise Exception("the client type is not support upload")

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        try:
            cmd = "su - {0} -c '{1}'".format(new_user, cmd)
            self.stdio.verbose("[local host] ssh_invoke_shell_switch_user cmd = [{0}] on localhost".format(cmd))
            out = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE, shell=True)
            stdout, stderr = out.communicate()
            if stderr:
                self.stdio.error("run cmd = [{0}] on localhost, stderr=[{1}]".format(cmd, stderr))
            return stdout
        except:
            self.stdio.error("run cmd = [{0}] on localhost".format(cmd))
        raise Exception("the client type is not support ssh invoke shell switch user")
