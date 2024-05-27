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
@file: ssh.py
@desc:
"""

from __future__ import absolute_import, division, print_function

import enum
import getpass
import os
import tempfile
import warnings
import sys

import paramiko
import time
import docker
from glob import glob
from paramiko import AuthenticationException, SFTPClient
from paramiko.client import SSHClient, AutoAddPolicy
from paramiko.ssh_exception import NoValidConnectionsError, SSHException
from multiprocessing.queues import Empty
from multiprocessing import Queue
from multiprocessing.pool import ThreadPool
from common.tool import COMMAND_ENV, DirectoryUtil, FileUtil, Timeout
from common.obdiag_exception import OBDIAGSSHConnException
from common.obdiag_exception import OBDIAGShellCmdException
from common.tool import StringUtils
from common.tool import TimeUtils
from stdio import SafeStdio
from err import EC_SSH_CONNECT
from subprocess32 import Popen, PIPE

warnings.filterwarnings("ignore")


__all__ = ("SshClient", "SshConfig", "LocalClient", "ConcurrentExecutor")


class SshConfig(object):

    def __init__(self, host, username='root', password=None, key_filename=None, port=22, timeout=30):
        self.host = host
        self.username = username
        self.password = password if password is None else str(password)
        self.key_filename = key_filename
        self.port = int(port)
        self.timeout = int(timeout)

    def __str__(self):
        return '%s@%s' % (self.username, self.host)


class SshReturn(object):

    def __init__(self, code, stdout, stderr):
        self.code = code
        self.stdout = stdout
        self.stderr = stderr

    def __bool__(self):
        return self.code == 0

    def __nonzero__(self):
        return self.__bool__()


class FeatureSshReturn(SshReturn, SafeStdio):

    def __init__(self, popen, timeout, stdio):
        self.popen = popen
        self.timeout = timeout
        self.stdio = stdio
        self._code = None
        self._stdout = None
        self._stderr = None

    def _get_return(self):
        if self._code is None:
            try:
                p = self.popen
                output, error = p.communicate(timeout=self.timeout)
                self._stdout = output.decode(errors='replace')
                self._stderr = error.decode(errors='replace')
                self._code = p.returncode
                verbose_msg = 'exited code %s' % self._code
                if self._code:
                    verbose_msg += ', error output:\n%s' % self._stderr
                self.stdio.verbose(verbose_msg)
            except Exception as e:
                self._stdout = ''
                self._stderr = str(e)
                self._code = 255
                verbose_msg = 'exited code 255, error output:\n%s' % self._stderr
                self.stdio.verbose(verbose_msg)
                self.stdio.exception('')

    @property
    def code(self):
        self._get_return()
        return self._code

    @property
    def stdout(self):
        self._get_return()
        return self._stdout

    @property
    def stderr(self):
        self._get_return()
        return self._stderr


class FutureSshReturn(SshReturn):

    def __init__(self, client, command, timeout=None, stdio=None):
        self.client = client
        self.command = command
        self.timeout = timeout
        self.stdio = stdio if stdio else client.stdio
        if self.stdio:
            self.stdio = self.stdio.sub_io()
        self.finsh = False
        super(FutureSshReturn, self).__init__(127, '', '')

    def set_return(self, ssh_return):
        self.code = ssh_return.code
        self.stdout = ssh_return.stdout
        self.stderr = ssh_return.stderr
        self.finsh = True


class ConcurrentExecutor(object):

    def __init__(self, workers=None):
        self.workers = workers
        self.futures = []

    def add_task(self, client, command, timeout=None, stdio=None):
        ret = FutureSshReturn(client, command, timeout, stdio=stdio)
        self.futures.append(ret)
        return ret

    def size(self):
        return len(self.futures)

    @staticmethod
    def execute(future):
        client = SshClient(future.client.config, future.stdio)
        future.set_return(client.execute_command(future.command, timeout=future.timeout))
        return future

    def submit(self):
        rets = []
        pool = ThreadPool(processes=self.workers)
        try:
            results = pool.map(ConcurrentExecutor.execute, tuple(self.futures))
            for r in results:
                rets.append(r)
        finally:
            pool.close()
        self.futures = []
        return rets


class LocalClient(SafeStdio):

    @staticmethod
    def init_env(env=None):
        if env is None:
            return None
        env_t = COMMAND_ENV.copy()
        env_t.update(env)
        return env_t

    @staticmethod
    def execute_command_background(command, env=None, timeout=None, stdio=None):
        stdio.verbose('local background execute: %s ' % command, end='')
        try:
            p = Popen(command, env=LocalClient.init_env(env), shell=True, stdout=PIPE, stderr=PIPE)
            return FeatureSshReturn(p, timeout, stdio)
        except Exception as e:
            output = ''
            error = str(e)
            code = 255
            verbose_msg = 'exited code 255, error output:\n%s' % error
            stdio.verbose(verbose_msg)
            stdio.exception('')
            return SshReturn(code, output, error)

    @staticmethod
    def execute_command(command, env=None, timeout=None, stdio=None):
        stdio.verbose('local execute: %s ' % command, end='')
        try:
            p = Popen(command, env=LocalClient.init_env(env), shell=True, stdout=PIPE, stderr=PIPE)
            output, error = p.communicate(timeout=timeout)
            code = p.returncode
            output = output.decode(errors='replace')
            error = error.decode(errors='replace')
            verbose_msg = 'exited code %s' % code
            if code:
                verbose_msg += ', error output:\n%s' % error
            stdio.verbose(verbose_msg)
        except Exception as e:
            output = ''
            error = str(e)
            code = 255
            verbose_msg = 'exited code 255, error output:\n%s' % error
            stdio.verbose(verbose_msg)
            stdio.exception('')
        return SshReturn(code, output, error)

    @staticmethod
    def put_file(local_path, remote_path, stdio=None):
        if LocalClient.execute_command('mkdir -p %s && cp -f %s %s' % (os.path.dirname(remote_path), local_path, remote_path), stdio=stdio):
            return True
        return False

    @staticmethod
    def put_dir(local_dir, remote_dir, stdio=None):
        if os.path.isdir(local_dir):
            local_dir = os.path.join(local_dir, '*')
        if os.path.exists(os.path.dirname(local_dir)) and not glob(local_dir):
            stdio.verbose("%s is empty" % local_dir)
            return True
        if LocalClient.execute_command('mkdir -p %s && cp -frL %s %s' % (remote_dir, local_dir, remote_dir), stdio=stdio):
            return True
        return False

    @staticmethod
    def write_file(content, file_path, mode='w', stdio=None):
        stdio.verbose('write {} to {}'.format(content, file_path))
        try:
            with FileUtil.open(file_path, mode, stdio=stdio) as f:
                f.write(content)
                f.flush()
            return True
        except:
            stdio.exception('')
            return False

    @staticmethod
    def get_file(local_path, remote_path, stdio=None):
        return LocalClient.put_file(remote_path, local_path, stdio=stdio)

    @staticmethod
    def get_dir(local_path, remote_path, stdio=None):
        return LocalClient.put_dir(remote_path, local_path, stdio=stdio)

    @staticmethod
    def run_command(command, env=None, timeout=None, print_stderr=True, elimit=0, olimit=0, stdio=None):
        stdio.verbose('local execute: %s ' % command)
        stdout = ""
        process = None
        try:
            with Timeout(timeout):
                process = Popen(command, env=LocalClient.init_env(env), shell=True, stdout=PIPE, stderr=PIPE)
                while process.poll() is None:
                    lines = process.stdout.readline()
                    line = lines.strip()
                    if line:
                        stdio.print(line.decode("utf8", 'ignore'))
                stderr = process.stderr.read().decode("utf8", 'ignore')
                code = process.returncode
                verbose_msg = 'exit code {}'.format(code)
                if code != 0 and stderr:
                    verbose_msg += ', error output:\n'
                stdio.verbose(verbose_msg)
                if print_stderr:
                    stdio.print(stderr)
                if elimit == 0:
                    stderr = ""
                elif elimit > 0:
                    stderr = stderr[-elimit:]
        except Exception as e:
            if process:
                process.terminate()
            stdout = ''
            stderr = str(e)
            code = 255
            verbose_msg = 'exited code 255, error output:\n%s' % stderr
            stdio.verbose(verbose_msg)
            stdio.exception('')
        finally:
            if process:
                process.terminate()
        return SshReturn(code, stdout, stderr)


class RemoteTransporter(enum.Enum):
    CLIENT = 0
    RSYNC = 1

    def __lt__(self, other):
        return self.value < other.value

    def __gt__(self, other):
        return self.value > other.value


class SshClient(SafeStdio):

    DEFAULT_PATH = '/sbin:/usr/local/bin:/usr/bin:/usr/local/sbin:/usr/sbin:'
    LOCAL_HOST = ['127.0.0.1', 'localhost', '127.1', '127.0.1']
    DISABLED_ALGORITHMS = dict(pubkeys=["rsa-sha2-512", "rsa-sha2-256"])

    def __init__(self, config, stdio=None):
        self.config = config
        self.stdio = stdio
        self.sftp = None
        self.is_connected = False
        self.ssh_client = SSHClient()
        self.env_str = ''
        self._remote_transporter = None
        self.task_queue = None
        self.result_queue = None
        self._is_local = self.is_local()
        if self._is_local:
            self.env = {}
        else:
            self.env = {'PATH': self.DEFAULT_PATH}
            self._update_env()

        self._disabled_rsa_algorithms = None
        super(SshClient, self).__init__()

    def _init_queue(self):
        self.task_queue = Queue()
        self.result_queue = Queue()

    def _update_env(self):
        env = []
        for key in self.env:
            if self.env[key]:
                env.append('export %s=%s$%s;' % (key, self.env[key], key))
        self.env_str = ''.join(env)

    def add_env(self, key, value, rewrite=False, stdio=None):
        if key not in self.env or not self.env[key] or rewrite:
            stdio.verbose('%s@%s set env %s to \'%s\'' % (self.config.username, self.config.host, key, value))
            if self._is_local:
                self._add_env_for_local(key, value, rewrite)
            else:
                self.env[key] = value
        else:
            stdio.verbose('%s@%s append \'%s\' to %s' % (self.config.username, self.config.host, value, key))
            if self._is_local:
                self._add_env_for_local(key, value, rewrite)
            else:
                self.env[key] += value
        self._update_env()

    def _add_env_for_local(self, key, value, rewrite=False):
        if rewrite:
            self.env[key] = value
        else:
            if key not in self.env:
                self.env[key] = COMMAND_ENV.get(key, '')
            self.env[key] += value

    def get_env(self, key, stdio=None):
        return self.env[key] if key in self.env else None

    def del_env(self, key, stdio=None):
        if key in self.env:
            stdio.verbose('%s@%s delete env %s' % (self.config.username, self.config.host, key))
            del self.env[key]
            self._update_env()

    def __str__(self):
        return '%s@%s:%d' % (self.config.username, self.config.host, self.config.port)

    def is_localhost(self, stdio=None):
        return self.config.host in self.LOCAL_HOST

    def _login(self, stdio=None, exit=True):
        if self.is_connected:
            return True
        err = None
        try:
            self.ssh_client.set_missing_host_key_policy(AutoAddPolicy())
            stdio.verbose('host: %s, port: %s, user: %s, password: %s' % (self.config.host, self.config.port, self.config.username, self.config.password))
            self.ssh_client.connect(
                self.config.host, port=self.config.port, username=self.config.username, password=self.config.password, key_filename=self.config.key_filename, timeout=self.config.timeout, disabled_algorithms=self._disabled_rsa_algorithms
            )
            self.is_connected = True
        except AuthenticationException:
            stdio.exception('')
            err = EC_SSH_CONNECT.format(user=self.config.username, ip=self.config.host, message='username or password error')
        except NoValidConnectionsError:
            stdio.exception('')
            err = EC_SSH_CONNECT.format(user=self.config.username, ip=self.config.host, message='time out')
        except BaseException as e:
            stdio.exception('')
            err = EC_SSH_CONNECT.format(user=self.config.username, ip=self.config.host, message=e)
        if err:
            if exit:
                stdio.critical(err)
                return err
            stdio.error(err)
            return err
        return self.is_connected

    def _open_sftp(self, stdio=None):
        if self.sftp:
            return True
        if self._login(stdio=stdio):
            SFTPClient.from_transport(self.ssh_client.get_transport())
            self.sftp = self.ssh_client.open_sftp()
            return True
        return False

    def is_local(self):
        return self.is_localhost() and self.config.username == getpass.getuser()

    def connect(self, stdio=None, exit=True):
        if self._is_local:
            return True
        return self._login(stdio=stdio, exit=exit)

    def reconnect(self, stdio=None):
        self.close(stdio=stdio)
        return self.connect(stdio=stdio)

    def close(self, stdio=None):
        if self._is_local:
            return True
        if self.is_connected:
            self.ssh_client.close()
        if self.sftp:
            self.sftp = None

    def __del__(self):
        self.close()

    def _execute_command(self, command, timeout=None, retry=3, stdio=None):
        if not self._login(stdio):
            return SshReturn(255, '', 'connect failed')
        try:
            stdin, stdout, stderr = self.ssh_client.exec_command(command, timeout=timeout)
            output = stdout.read().decode(errors='replace')
            error = stderr.read().decode(errors='replace')
            if output:
                idx = output.rindex('\n')
                code = int(output[idx:])
                stdout = output[:idx]
                verbose_msg = 'exited code %s' % code
            else:
                code, stdout = 1, ''
            if code:
                verbose_msg = 'exited code %s, error output:\n%s' % (code, error)
            stdio.verbose(verbose_msg)
        except SSHException as e:
            if retry:
                self.close()
                return self._execute_command(command, retry - 1, stdio)
            else:
                stdio.exception('')
                stdio.critical('%s@%s connect failed: %s' % (self.config.username, self.config.host, e))
                raise e
        except Exception as e:
            stdio.exception('')
            code = 255
            stdout = ''
            error = str(e)
        return SshReturn(code, stdout, error)

    def execute_command(self, command, timeout=None, stdio=None):
        if timeout is None:
            timeout = self.config.timeout
        elif timeout <= 0:
            timeout = None

        if self._is_local:
            return LocalClient.execute_command(command, self.env if self.env else None, timeout, stdio=stdio)

        verbose_msg = '%s execute: %s ' % (self.config, command)
        stdio.verbose(verbose_msg, end='')
        command = '(%s %s);echo -e "\n$?\c"' % (self.env_str, command.strip(';').lstrip('\n'))
        return self._execute_command(command, retry=3, timeout=timeout, stdio=stdio)

    @property
    def remote_transporter(self):
        if self._remote_transporter is not None:
            return self._remote_transporter
        _transporter = RemoteTransporter.CLIENT
        if not self._is_local and self._remote_transporter is None:
            if not self.config.password and not self.disable_rsync:
                ret = LocalClient.execute_command('rsync -h', stdio=self.stdio) and self.execute_command('rsync -h', stdio=self.stdio)
                if ret:
                    _transporter = RemoteTransporter.RSYNC
        self._remote_transporter = _transporter
        self.stdio.verbose("current remote_transporter {}".format(self._remote_transporter))
        return self._remote_transporter

    def put_file(self, local_path, remote_path, stdio=None):
        if not os.path.isfile(local_path):
            stdio.error('path: %s is not file' % local_path)
            return False
        if self._is_local:
            return LocalClient.put_file(local_path, remote_path, stdio=stdio)
        if not self._open_sftp(stdio=stdio):
            return False
        return self._put_file(local_path, remote_path, stdio=stdio)

    def write_file(self, content, file_path, mode='w', stdio=None):
        if self._is_local:
            return LocalClient.write_file(content, file_path, mode, stdio)
        return self._write_file(content, file_path, mode, stdio)

    def _write_file(self, content, file_path, mode='w', stdio=None):
        stdio.verbose('write {} to {}: {}'.format(content, self, file_path))
        try:
            with tempfile.NamedTemporaryFile(mode=mode) as f:
                f.write(content)
                f.flush()
                return self.put_file(f.name, file_path, stdio=stdio)
        except:
            stdio.exception('')
            return False

    @property
    def _put_file(self):
        if self.remote_transporter == RemoteTransporter.RSYNC:
            return self._rsync_put_file
        else:
            return self._client_put_file

    def _client_put_file(self, local_path, remote_path, stdio=None):
        if self.execute_command('mkdir -p %s && rm -fr %s' % (os.path.dirname(remote_path), remote_path), stdio=stdio):
            stdio.verbose('send %s to %s' % (local_path, remote_path))
            if self.sftp.put(local_path.replace('~', os.getenv('HOME')), remote_path.replace('~', os.getenv('HOME'))):
                return self.execute_command('chmod %s %s' % (oct(os.stat(local_path).st_mode)[-3:], remote_path))
        return False

    def _rsync(self, source, target, stdio=None):
        identity_option = ""
        if self.config.key_filename:
            identity_option += '-i {key_filename} '.format(key_filename=self.config.key_filename)
        if self.config.port:
            identity_option += '-p {}'.format(self.config.port)
        cmd = 'yes | rsync -a -W -e "ssh {identity_option}" {source} {target}'.format(identity_option=identity_option, source=source, target=target)
        ret = LocalClient.execute_command(cmd, stdio=stdio)
        return bool(ret)

    def _rsync_put_dir(self, local_path, remote_path, stdio=None):
        stdio.verbose('send %s to %s by rsync' % (local_path, remote_path))
        source = os.path.join(local_path, '*')
        if os.path.exists(os.path.dirname(source)) and not glob(source):
            stdio.verbose("%s is empty" % source)
            return True
        target = "{user}@{host}:{remote_path}".format(user=self.config.username, host=self.config.host, remote_path=remote_path)
        if self._rsync(source, target, stdio=stdio):
            return True
        else:
            return False

    def _rsync_put_file(self, local_path, remote_path, stdio=None):
        if not self.execute_command('mkdir -p %s' % os.path.dirname(remote_path), stdio=stdio):
            return False
        stdio.verbose('send %s to %s by rsync' % (local_path, remote_path))
        target = "{user}@{host}:{remote_path}".format(user=self.config.username, host=self.config.host, remote_path=remote_path)
        if self._rsync(local_path, target, stdio=stdio):
            return True
        else:
            return False

    def put_dir(self, local_dir, remote_dir, stdio=None):
        if self._is_local:
            return LocalClient.put_dir(local_dir, remote_dir, stdio=stdio)
        if not self._open_sftp(stdio=stdio):
            return False
        if not self.execute_command('mkdir -p %s' % remote_dir, stdio=stdio):
            return False
        stdio.start_loading('Send %s to %s' % (local_dir, remote_dir))
        ret = self._put_dir(local_dir, remote_dir, stdio=stdio)
        stdio.stop_loading('succeed' if ret else 'fail')
        return ret

    @property
    def _put_dir(self):
        if self.remote_transporter == RemoteTransporter.RSYNC:
            return self._rsync_put_dir
        else:
            return self._client_put_dir

    def _client_put_dir(self, local_dir, remote_dir, stdio=None):
        has_failed = False
        ret = LocalClient.execute_command('find -L %s -type f' % local_dir)
        if not ret:
            has_failed = True
        all_files = ret.stdout.strip().split('\n') if ret.stdout else []
        ret = LocalClient.execute_command('find %s -type d' % local_dir)
        if not ret:
            has_failed = True
        all_dirs = ret.stdout.strip().split('\n') if ret.stdout else []
        self._filter_dir_in_file_path(all_files, all_dirs)
        for local_path in all_files:
            remote_path = os.path.join(remote_dir, os.path.relpath(local_path, local_dir))
            if not self._client_put_file(local_path, remote_path, stdio=stdio):
                stdio.error('Fail to get %s' % remote_path)
                has_failed = True
        for local_path in all_dirs:
            remote_path = os.path.join(remote_dir, os.path.relpath(local_path, local_dir))
            stat = oct(os.stat(local_path).st_mode)[-3:]
            cmd = '[ -d "{remote_path}" ] || (mkdir -p {remote_path}; chmod {stat} {remote_path})'.format(remote_path=remote_path, stat=stat)
            if not self.execute_command(cmd):
                has_failed = True
        return not has_failed

    def get_file(self, local_path, remote_path, stdio=None):
        dirname, _ = os.path.split(local_path)
        if not dirname:
            dirname = os.getcwd()
            local_path = os.path.join(dirname, local_path)
        if os.path.exists(dirname):
            if not os.path.isdir(dirname):
                stdio.error('%s is not directory' % dirname)
                return False
        elif not DirectoryUtil.mkdir(dirname, stdio=stdio):
            return False
        if os.path.exists(local_path) and not os.path.isfile(local_path):
            stdio.error('path: %s is not file' % local_path)
            return False
        if self._is_local:
            return LocalClient.get_file(local_path, remote_path, stdio=stdio)
        if not self._open_sftp(stdio=stdio):
            return False
        return self._get_file(local_path, remote_path, stdio=stdio)

    @property
    def _get_file(self):
        if self.remote_transporter == RemoteTransporter.RSYNC:
            return self._rsync_get_file
        else:
            return self._client_get_file

    def _rsync_get_dir(self, local_path, remote_path, stdio=None):
        source = "{user}@{host}:{remote_path}".format(user=self.config.username, host=self.config.host, remote_path=remote_path)
        if "*" not in remote_path:
            source = os.path.join(source, "*")
        target = local_path
        stdio.verbose('get %s from %s by rsync' % (local_path, remote_path))
        if LocalClient.execute_command('mkdir -p {}'.format(local_path), stdio=stdio) and self._rsync(source, target, stdio=stdio):
            return True
        else:
            return False

    def _rsync_get_file(self, local_path, remote_path, stdio=None):
        source = "{user}@{host}:{remote_path}".format(user=self.config.username, host=self.config.host, remote_path=remote_path)
        target = local_path
        stdio.verbose('get %s from %s by rsync' % (local_path, remote_path))
        if self._rsync(source, target, stdio=stdio):
            return True
        else:
            return False

    def _client_get_file(self, local_path, remote_path, stdio=None):
        try:
            self.sftp.get(remote_path, local_path)
            stat = self.sftp.stat(remote_path)
            os.chmod(local_path, stat.st_mode)
            return True
        except Exception as e:
            stdio.exception('get %s from %s@%s:%s failed: %s' % (local_path, self.config.username, self.config.host, remote_path, e))
        return False

    def get_dir(self, local_dir, remote_dir, stdio=None):
        dirname, _ = os.path.split(local_dir)
        if not dirname:
            dirname = os.getcwd()
            local_dir = os.path.join(dirname, local_dir)
        if "*" in dirname:
            stdio.error('Invalid directory {}'.format(dirname))
            return False
        if os.path.exists(dirname):
            if not os.path.isdir(dirname):
                stdio.error('%s is not directory' % dirname)
                return False
        elif not DirectoryUtil.mkdir(dirname, stdio=stdio):
            return False
        if os.path.exists(local_dir) and not os.path.isdir(local_dir):
            stdio.error('%s is not directory' % local_dir)
            return False
        if self._is_local:
            return LocalClient.get_dir(local_dir, remote_dir, stdio=stdio)
        if not self._open_sftp(stdio=stdio):
            return False
        stdio.start_loading('Get %s from %s' % (local_dir, remote_dir))
        ret = self._get_dir(local_dir, remote_dir, stdio=stdio)
        stdio.stop_loading('succeed' if ret else 'fail')
        return ret

    @property
    def _get_dir(self):
        if self.remote_transporter == RemoteTransporter.RSYNC:
            return self._rsync_get_dir
        else:
            return self._client_get_dir

    def _client_get_dir(self, local_dir, remote_dir, stdio=None):
        task_queue = []
        has_failed = False
        if DirectoryUtil.mkdir(local_dir, stdio=stdio):
            try:
                ret = self.execute_command('find %s -type f' % remote_dir)
                if not ret:
                    stdio.verbose(ret.stderr)
                    has_failed = True
                all_files = ret.stdout.strip().split('\n') if ret.stdout else []
                ret = self.execute_command('find %s -type d' % remote_dir)
                if not ret:
                    has_failed = True
                all_dirs = ret.stdout.strip().split('\n') if ret.stdout else []
                self._filter_dir_in_file_path(all_files, all_dirs)
                for f in all_files:
                    task_queue.append(f)
                if "*" in remote_dir:
                    remote_base_dir = os.path.dirname(remote_dir)
                else:
                    remote_base_dir = remote_dir
                for remote_path in task_queue:
                    local_path = os.path.join(local_dir, os.path.relpath(remote_path, remote_dir))
                    if not self._client_get_file(local_path, remote_path, stdio=stdio):
                        stdio.error('Fail to get %s' % remote_path)
                        has_failed = True
                for remote_path in all_dirs:
                    try:
                        local_path = os.path.join(local_dir, os.path.relpath(remote_path, remote_base_dir))
                        if not os.path.exists(local_path):
                            stat = self.sftp.stat(remote_path)
                            os.makedirs(local_path, mode=stat.st_mode)
                    except Exception as e:
                        stdio.exception('Fail to make directory %s in local: %s' % (remote_path, e))
                        has_failed = True
                return not has_failed
            except Exception as e:
                stdio.exception('Fail to get %s: %s' % (remote_dir, e))

    @staticmethod
    def _filter_dir_in_file_path(files, directories):
        skip_directories = []
        for path in files:
            dir_name = os.path.dirname(path)
            while dir_name not in ["/", ".", ""]:
                if dir_name in skip_directories:
                    break
                if dir_name in directories:
                    directories.remove(dir_name)
                    skip_directories.append(dir_name)
                dir_name = os.path.dirname(dir_name)

    def file_downloader(self, local_dir, remote_dir, stdio=None):
        try:
            client = SshClient(config=self.config, stdio=None)
            client._open_sftp(stdio=stdio)
            client._remote_transporter = self.remote_transporter
            while True:
                remote_path = self.task_queue.get(block=False)
                local_path = os.path.join(local_dir, os.path.relpath(remote_path, remote_dir))
                if client.get_file(local_path, remote_path, stdio=stdio):
                    self.result_queue.put(remote_path)
                else:
                    stdio.error('Fail to get %s' % remote_path)
        except Empty:
            return
        except:
            stdio.exception("")
            stdio.exception('Failed to get %s' % remote_dir)

    def file_uploader(self, local_dir, remote_dir, stdio=None):
        try:
            client = SshClient(config=self.config, stdio=None)
            client._remote_transporter = self.remote_transporter
            while True:
                local_path, is_dir = self.task_queue.get(block=False)
                remote_path = os.path.join(remote_dir, os.path.relpath(local_path, local_dir))
                if is_dir:
                    stat = oct(os.stat(local_path).st_mode)[-3:]
                    cmd = '[ -d "{remote_path}" ] || (mkdir -p {remote_path}; chmod {stat} {remote_path})'.format(remote_path=remote_path, stat=stat)
                    if client.execute_command(cmd):
                        self.result_queue.put(remote_path)
                else:
                    if client.put_file(local_path, remote_path, stdio=stdio):
                        self.result_queue.put(remote_path)
                    else:
                        stdio.error('Fail to get %s' % remote_path)
        except Empty:
            return
        except:
            stdio.exception("")
            stdio.verbose('Failed to get %s' % remote_dir)


# TODO ENV_DISABLE_RSA_ALGORITHMS need get by context.inner_context
ENV_DISABLE_RSA_ALGORITHMS = 0


def dis_rsa_algorithms(state=0):
    """
    Disable RSA algorithms in OpenSSH server.
    """
    global ENV_DISABLE_RSA_ALGORITHMS
    ENV_DISABLE_RSA_ALGORITHMS = state


class SshHelper(object):
    def __init__(self, is_ssh=None, host_ip=None, username=None, password=None, ssh_port=None, key_file=None, node=None, stdio=None):
        if node is None:
            node = {}
        self.is_ssh = is_ssh
        self.stdio = stdio
        self.host_ip = host_ip
        self.username = username
        self.ssh_port = node.get("ssh_port") or ssh_port
        self.need_password = True
        self.password = node.get("ssh_password") or password
        self.key_file = node.get("ssh_key_file") or key_file
        self.key_file = os.path.expanduser(self.key_file)
        self.ssh_type = node.get("ssh_type") or "remote"
        self._ssh_fd = None
        self._sftp_client = None
        if "ssh_type" in node and node.get("ssh_type") == "docker":
            try:
                self.ssh_type = node["ssh_type"]
                self.stdio.verbose("use ssh_type:{0} , node info : {1}".format(self.ssh_type, StringUtils.node_cut_passwd_for_log(node)))
                self.node = node
                # docker_permissions_check
                if self.ssh_type == "docker":
                    self.client = docker.from_env()
                    if "container_name" not in node:
                        self.stdio.error("SshHelper init docker Exception: 'container_name' not in node")
                        raise Exception("SshHelper init docker Exception: 'container_name' not in node")
                else:
                    self.stdio.error("SshHelper init not support the ssh_type : {0}".format(self.ssh_type))
                    raise Exception("SshHelper init not support the ssh_type : {0}".format(self.ssh_type))

            except Exception as e:
                self.stdio.error("SshHelper init docker Exception: {0}".format(e))
                raise Exception("SshHelper init docker Exception: {0}".format(e))

            return

        if self.is_ssh:
            self._disabled_rsa_algorithms = None
            DISABLED_ALGORITHMS = dict(pubkeys=["rsa-sha2-512", "rsa-sha2-256"])
            if ENV_DISABLE_RSA_ALGORITHMS == 1:
                self._disabled_rsa_algorithms = DISABLED_ALGORITHMS
            self.ssh_type = "remote"
            if len(self.key_file) > 0:
                try:
                    self._ssh_fd = paramiko.SSHClient()
                    self._ssh_fd.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
                    self._ssh_fd.load_system_host_keys()
                    self._ssh_fd.connect(hostname=host_ip, username=username, key_filename=self.key_file, port=ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)
                except AuthenticationException:
                    self.password = input("Authentication failed, Input {0}@{1} password:\n".format(username, host_ip))
                    self.need_password = True
                    self._ssh_fd.connect(hostname=host_ip, username=username, password=password, port=ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)
                except Exception as e:
                    raise OBDIAGSSHConnException("ssh {0}@{1}: failed, exception:{2}".format(username, host_ip, e))
            else:
                self._ssh_fd = paramiko.SSHClient()
                self._ssh_fd.set_missing_host_key_policy(paramiko.client.AutoAddPolicy())
                self._ssh_fd.load_system_host_keys()
                self.need_password = True
                self._ssh_fd.connect(hostname=host_ip, username=username, password=password, port=ssh_port, disabled_algorithms=self._disabled_rsa_algorithms)

    def ssh_exec_cmd(self, cmd):
        if self.ssh_type == "docker":
            try:
                self.stdio.verbose("ssh_exec_cmd docker {0} cmd: {1}".format(self.node.get("container_name"), cmd))
                client_result = self.client.containers.get(self.node["container_name"])
                result = client_result.exec_run(
                    cmd=["bash", "-c", cmd],
                    detach=False,
                    stdout=True,
                    stderr=True,
                )
                if result.exit_code != 0:
                    raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.node["container_name"], cmd, result.output.decode('utf-8')))

            except Exception as e:
                self.stdio.error("sshHelper ssh_exec_cmd docker Exception: {0}".format(e))
                raise Exception("sshHelper ssh_exec_cmd docker Exception: {0}".format(e))

            return result.output.decode('utf-8')
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            err_text = stderr.read()
            if len(err_text):
                raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.host_ip, cmd, err_text))
        except SSHException as e:
            raise OBDIAGShellCmdException("Execute Shell command on server {0} failed, " "command=[{1}], exception:{2}".format(self.host_ip, cmd, e))
        return stdout.read().decode('utf-8')

    def ssh_exec_cmd_ignore_err(self, cmd):
        if self.ssh_type == "docker":
            try:
                client_result = self.client.containers.get(self.node["container_name"])
                result = client_result.exec_run(
                    cmd=["bash", "-c", cmd],
                    detach=False,
                    stdout=True,
                    stderr=True,
                )
            except Exception as e:
                self.stdio.error("sshHelper ssh_exec_cmd docker Exception: {0}".format(e))
                raise Exception("sshHelper ssh_exec_cmd docker Exception: {0}".format(e))

            return result.output.decode('utf-8')

        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            return stdout.read().decode('utf-8')
        except SSHException as e:
            print("Execute Shell command on server {0} failed,command=[{1}], exception:{2}".format(self.node, cmd, e))

    def ssh_exec_cmd_ignore_exception(self, cmd):
        if self.ssh_type == "docker":
            try:
                client_result = self.client.containers.get(self.node["container_name"])
                result = client_result.exec_run(
                    cmd=["bash", "-c", cmd],
                    detach=False,
                    stdout=True,
                    stderr=True,
                )
                return result.output.decode('utf-8')
            except Exception as e:
                self.stdio.error("sshHelper ssh_exec_cmd_ignore_exception docker Exception: {0}".format(e))
                pass
                # raise Exception("sshHelper ssh_exec_cmd docker Exception: {0}".format(e))
            return

        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            return stderr.read().decode('utf-8')
        except SSHException as e:
            pass

    def ssh_exec_cmd_get_stderr(self, cmd):
        if self.ssh_type == "docker":
            try:
                client_result = self.client.containers.get(self.node["container_name"])
                result = client_result.exec_run(
                    cmd=["bash", "-c", cmd],
                    detach=False,
                    stdout=True,
                    stderr=True,
                )
                return result.output.decode('utf-8')
            except Exception as e:
                self.stdio.error("sshHelper ssh_exec_cmd_ignore_exception docker Exception: {0}".format(e))
                pass
                # raise Exception("sshHelper ssh_exec_cmd docker Exception: {0}".format(e))
            return
        try:
            stdin, stdout, stderr = self._ssh_fd.exec_command(cmd)
            return stderr.read().decode('utf-8')
        except SSHException as e:
            pass

    def progress_bar(self, transferred, to_be_transferred, suffix=''):
        bar_len = 20
        filled_len = int(round(bar_len * transferred / float(to_be_transferred)))
        percents = round(20.0 * transferred / float(to_be_transferred), 1)
        bar = '\033[32;1m%s\033[0m' % '=' * filled_len + '-' * (bar_len - filled_len)
        print_percents = round((percents * 5), 1)
        sys.stdout.flush()
        sys.stdout.write('Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m%s\033[0m' % print_percents, '% [', self.translate_byte(transferred), ']', suffix))
        if transferred == to_be_transferred:
            sys.stdout.write('Downloading [%s] %s%s%s %s %s\r' % (bar, '\033[32;1m%s\033[0m' % print_percents, '% [', self.translate_byte(transferred), ']', suffix))
            print()

    def download(self, remote_path, local_path):
        if self.ssh_type == "docker":
            try:
                self.stdio.verbose("remote_path: {0}:{1} to local_path:{2}".format(self.node["container_name"], remote_path, local_path))
                client_result = self.client.containers.get(self.node["container_name"])
                data, stat = client_result.get_archive(remote_path)
                with open(local_path, "wb") as f:
                    for chunk in data:
                        f.write(chunk)
                return
            except Exception as e:
                self.stdio.error("sshHelper download docker Exception: {0}".format(e))
                raise Exception("sshHelper download docker Exception: {0}".format(e))
            return

        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        print('Download {0}:{1}'.format(self.host_ip, remote_path))
        self._sftp_client.get(remote_path, local_path, callback=self.progress_bar)
        self._sftp_client.close()

    def translate_byte(self, B):
        B = float(B)
        KB = float(1024)
        MB = float(KB**2)
        GB = float(MB**2)
        TB = float(GB**2)
        if B < KB:
            return '{} {}'.format(B, 'bytes' if B > 1 else "byte")
        elif KB < B < MB:
            return '{:.2f} KB'.format(B / KB)
        elif MB < B < GB:
            return '{:.2f} MB'.format(B / MB)
        elif GB < B < TB:
            return '{:.2f} GB'.format(B / GB)
        else:
            return '{:.2f} TB'.format(B / TB)

    def upload(self, remote_path, local_path):
        if self.ssh_type == "docker":
            try:
                self.stdio.verbose(" local_path:{0} to remote_path:{1}:{2}".format(local_path, self.node["container_name"], remote_path))

                self.client.containers.get(self.node["container_name"]).put_archive(remote_path, local_path)

                return
            except Exception as e:
                self.stdio.error("sshHelper upload docker Exception: {0}".format(e))
                raise Exception("sshHelper upload docker Exception: {0}".format(e))
            return
        transport = self._ssh_fd.get_transport()
        self._sftp_client = paramiko.SFTPClient.from_transport(transport)
        self._sftp_client.put(remote_path, local_path)
        self._sftp_client.close()

    def ssh_close(self):
        if self.ssh_type == "docker":
            self.client.close()
            return
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None

    def __del__(self):
        if self._sftp_client is not None:
            self._sftp_client.close()
            self._sftp_client = None

    def ssh_invoke_shell_switch_user(self, new_user, cmd, time_out):
        if self.ssh_type == "docker":
            try:
                exec_id = self.client.exec_create(container=self.node["container_name"], command=['su', '- ' + new_user])
                response = self.client.exec_start(exec_id)

                return response
            except Exception as e:
                self.stdio.error("sshHelper ssh_invoke_shell_switch_user docker Exception: {0}".format(e))
                raise Exception("sshHelper ssh_invoke_shell_switch_user docker Exception: {0}".format(e))
            return
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

    def get_name(self):
        if self.ssh_type == "docker":
            return "(docker)" + self.node.get("container_name")
        return self.host_ip
