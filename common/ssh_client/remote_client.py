import os
import time

import paramiko
from paramiko.ssh_exception import SSHException, AuthenticationException

from common.obdiag_exception import OBDIAGShellCmdException, OBDIAGSSHConnException
from common.ssh_client.ssh import SsherClient

ENV_DISABLE_RSA_ALGORITHMS = 0


def dis_rsa_algorithms(state=0):
    """
    Disable RSA algorithms in OpenSSH server.
    """
    global ENV_DISABLE_RSA_ALGORITHMS
    ENV_DISABLE_RSA_ALGORITHMS = state


class RemoteClient(SsherClient):
    def __init__(self, context, node):
        super().__init__(context, node)
        self._sftp_client = None
        self._disabled_rsa_algorithms = None
        self.host_ip = self.node("ip")
        self.username = self.node.get("username")
        self.ssh_port = self.node.get("ssh_port")
        self.need_password = True
        self.password = self.node.get("ssh_password")
        self.key_file = self.node.get("ssh_key_file")
        self.key_file = os.path.expanduser(self.key_file)
        self.ssh_type = self.node.get("ssh_type") or "remote"
        self._ssh_fd = None
        self._sftp_client = None
        DISABLED_ALGORITHMS = dict(pubkeys=["rsa-sha2-512", "rsa-sha2-256"])
        if ENV_DISABLE_RSA_ALGORITHMS == 1:
            self._disabled_rsa_algorithms = DISABLED_ALGORITHMS
        self.ssh_type = "remote"
        if len(self.key_file) > 0:
            try:
                self._ssh_fd = paramiko.SSHClient()
                self._ssh_fd.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
                self._ssh_fd.load_system_host_keys()
                self._ssh_fd.connect(hostname=self.host_ip, username=self.username, key_filename=self.key_file, port=self.ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)
            except AuthenticationException:
                self.password = input("Authentication failed, Input {0}@{1} password:\n".format(self.username, self.ssh_port))
                self.need_password = True
                self._ssh_fd.connect(hostname=self.host_ip, username=self.username, password=self.password, port=self.ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)
            except Exception as e:
                raise OBDIAGSSHConnException("ssh {0}@{1}: failed, exception:{2}".format(self.host_ip, self.ssh_port, e))
        else:
            self._ssh_fd = paramiko.SSHClient()
            self._ssh_fd.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
            self._ssh_fd.load_system_host_keys()
            self.need_password = True
            self._ssh_fd.connect(hostname=host_ip, username=username, password=password, port=ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)

    def exec_cmd(self, cmd):
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            err_text = stderr.read()
            if len(err_text):
                raise Exception("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.host_ip, cmd, err_text))
        except SSHException as e:
            raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))
        return stdout.read().decode('utf-8')

    def download(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self.stdio.verbose('Download {0}:{1}'.format(self.host_ip, remote_path))
        self._sftp_client.get(remote_path, local_path, callback=self.progress_bar)
        self._sftp_client.close()

    def upload(self, remote_path, local_path):
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self._sftp_client.put(remote_path, local_path)
        self._sftp_client.close()

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        try:
            ssh = self._ssh_fd.invoke_shell()
            ssh.send('su {0}\n'.format(new_user))
            ssh.send('{}\n'.format(cmd))
            time.sleep(time_out)
            self._ssh_fd.close()
            result = ssh.recv(65535)
        except SSHException as e:
            raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))
        return result
