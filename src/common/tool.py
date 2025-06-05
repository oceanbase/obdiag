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
@file: tool.py
@desc:
"""

from __future__ import absolute_import, division, print_function

import http
import io
import bz2
import random
import ssl
import sys
import stat
import gzip
import fcntl
import signal
import re
import hashlib
import uuid
from src.common.constant import const
import tabulate
import socket
import requests
import decimal
import json
import time
import datetime
import string
import oyaml as yaml
import lzma
import pymysql as mysql
import shutil
import tarfile
import pyminizip
import os
from datetime import timedelta
from random import choice
from io import BytesIO
import copy
from colorama import Fore, Style
from ruamel.yaml import YAML
from src.common.err import EC_SQL_EXECUTE_FAILED
from src.common.stdio import SafeStdio
from src.common.version import OBDIAG_VERSION

_open = open
encoding_open = open

__all__ = ("Timeout", "DynamicLoading", "ConfigUtil", "DirectoryUtil", "FileUtil", "YamlLoader", "COMMAND_ENV", "TimeUtils", "NetUtils", "StringUtils", "YamlUtils", "Util")

_WINDOWS = os.name == 'nt'


class Timeout(object):

    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise TimeoutError(self.error_message)

    def _is_timeout(self):
        return self.seconds and self.seconds > 0

    def __enter__(self):
        if self._is_timeout():
            signal.signal(signal.SIGALRM, self.handle_timeout)
            signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        if self._is_timeout():
            signal.alarm(0)


timeout = Timeout


class DynamicLoading(object):
    class Module(object):

        def __init__(self, module):
            self.module = module
            self.count = 0

    LIBS_PATH = {}
    MODULES = {}

    @staticmethod
    def add_lib_path(lib):
        if lib not in DynamicLoading.LIBS_PATH:
            DynamicLoading.LIBS_PATH[lib] = 0
        if DynamicLoading.LIBS_PATH[lib] == 0:
            sys.path.insert(0, lib)
        DynamicLoading.LIBS_PATH[lib] += 1

    @staticmethod
    def add_libs_path(libs):
        for lib in libs:
            DynamicLoading.add_lib_path(lib)

    @staticmethod
    def remove_lib_path(lib):
        if lib not in DynamicLoading.LIBS_PATH:
            return
        if DynamicLoading.LIBS_PATH[lib] < 1:
            return
        try:
            DynamicLoading.LIBS_PATH[lib] -= 1
            if DynamicLoading.LIBS_PATH[lib] == 0:
                idx = sys.path.index(lib)
                del sys.path[idx]
        except:
            pass

    @staticmethod
    def remove_libs_path(libs):
        for lib in libs:
            DynamicLoading.remove_lib_path(lib)

    @staticmethod
    def import_module(name, stdio=None):
        if name not in DynamicLoading.MODULES:
            try:
                stdio and getattr(stdio, 'verbose', print)('import %s' % name)
                module = __import__(name)
                DynamicLoading.MODULES[name] = DynamicLoading.Module(module)
            except:
                stdio and getattr(stdio, 'exception', print)('import %s failed' % name)
                stdio and getattr(stdio, 'verbose', print)('sys.path: %s' % sys.path)
                return None
        DynamicLoading.MODULES[name].count += 1
        stdio and getattr(stdio, 'verbose', print)('add %s ref count to %s' % (name, DynamicLoading.MODULES[name].count))
        return DynamicLoading.MODULES[name].module

    @staticmethod
    def export_module(name, stdio=None):
        if name not in DynamicLoading.MODULES:
            return
        if DynamicLoading.MODULES[name].count < 1:
            return
        try:
            DynamicLoading.MODULES[name].count -= 1
            stdio and getattr(stdio, 'verbose', print)('sub %s ref count to %s' % (name, DynamicLoading.MODULES[name].count))
            if DynamicLoading.MODULES[name].count == 0:
                stdio and getattr(stdio, 'verbose', print)('export %s' % name)
                del sys.modules[name]
                del DynamicLoading.MODULES[name]
        except:
            stdio and getattr(stdio, 'exception', print)('export %s failed' % name)


class ConfigUtil(object):

    @staticmethod
    def get_value_from_dict(conf, key, default=None, transform_func=None):
        try:
            # 不要使用 conf.get(key, default)来替换，这里还有类型转换的需求
            value = conf[key]
            return transform_func(value) if value is not None and transform_func else value
        except:
            return default

    @staticmethod
    def get_list_from_dict(conf, key, transform_func=None):
        try:
            return_list = conf[key]
            if transform_func:
                return [transform_func(value) for value in return_list]
            else:
                return return_list
        except:
            return []

    @staticmethod
    def get_random_pwd_by_total_length(pwd_length=10):
        char = string.ascii_letters + string.digits
        pwd = ""
        for i in range(pwd_length):
            pwd = pwd + random.choice(char)
        return pwd

    @staticmethod
    def get_random_pwd_by_rule(lowercase_length=2, uppercase_length=2, digits_length=2, punctuation_length=2):
        pwd = ""
        for i in range(lowercase_length):
            pwd += random.choice(string.ascii_lowercase)
        for i in range(uppercase_length):
            pwd += random.choice(string.ascii_uppercase)
        for i in range(digits_length):
            pwd += random.choice(string.digits)
        for i in range(punctuation_length):
            pwd += random.choice('(._+@#%)')
        pwd_list = list(pwd)
        random.shuffle(pwd_list)
        return ''.join(pwd_list)

    @staticmethod
    def passwd_format(passwd):
        return "'{}'".format(passwd.replace("'", "'\"'\"'"))


class ConfigOptionsParserUtil(object):
    def __init__(self):
        self.config_dict = {}
        self.key_mapping = {
            'db_host': 'obcluster.db_host',
            'db_port': 'obcluster.db_port',
            'tenant_sys.user': 'obcluster.tenant_sys.user',
            'tenant_sys.password': 'obcluster.tenant_sys.password',
            'ssh_username': 'obcluster.servers.global.ssh_username',
            'ssh_password': 'obcluster.servers.global.ssh_password',
            'ssh_port': 'obcluster.servers.global.ssh_port',
            'home_path': 'obcluster.servers.global.home_path',
            'obproxy_home_path': 'obproxy.servers.global.home_path',
        }

    def set_nested_value(self, d, keys, value):
        """Recursively set the value in a nested dictionary."""
        if len(keys) > 1:
            if 'nodes' in keys[0]:
                try:
                    # Handle nodes
                    parts = keys[0].split('[')
                    base_key = parts[0]
                    index = int(parts[1].rstrip(']'))
                    if base_key not in d:
                        d[base_key] = []
                    while len(d[base_key]) <= index:
                        d[base_key].append({})
                    self.set_nested_value(d[base_key][index], keys[1:], value)
                except (IndexError, ValueError) as e:
                    raise ValueError(f"Invalid node index in key '{keys[0]}'") from e
            else:
                if keys[0] not in d:
                    d[keys[0]] = {}
                d[keys[0]] = self.set_nested_value(d[keys[0]], keys[1:], value)
        else:
            d[keys[0]] = value
        return d

    def parse_config(self, input_array):
        for item in input_array:
            try:
                key, value = item.split('=', 1)
                # Map short keys to full keys if needed
                if key in self.key_mapping:
                    key = self.key_mapping[key]
                keys = key.split('.')
                self.set_nested_value(self.config_dict, keys, value)
            except ValueError:
                raise ValueError(f"Invalid input format for item '{item}'")

        self.config_dict = self.add_default_values(self.config_dict)
        return self.config_dict

    def add_default_values(self, d):
        if isinstance(d, dict):
            for k, v in d.items():
                if k == 'login':
                    if 'password' not in v:
                        v['password'] = ''
                elif k == 'tenant_sys':
                    if 'password' not in v:
                        v['password'] = ''
                elif k == 'global':
                    if 'ssh_username' not in v:
                        v['ssh_username'] = ''
                    if 'ssh_password' not in v:
                        v['ssh_password'] = ''
                elif k == 'servers':
                    # Ensure 'nodes' is present and initialized as an empty list
                    if 'nodes' not in v:
                        v['nodes'] = []
                    if 'global' not in v:
                        v['global'] = {}
                    self.add_default_values(v['global'])
                    for node in v['nodes']:
                        if isinstance(node, dict):
                            self.add_default_values(node)
                elif isinstance(v, dict):
                    self.add_default_values(v)
                elif isinstance(v, list):
                    for node in v:
                        if isinstance(node, dict):
                            self.add_default_values(node)
        return d


class DirectoryUtil(object):

    @staticmethod
    def get_owner(path):
        return os.stat(path)[stat.ST_UID]

    @staticmethod
    def list_dir(path, stdio=None):
        files = []
        if os.path.isdir(path):
            for fn in os.listdir(path):
                fp = os.path.join(path, fn)
                if os.path.isdir(fp):
                    files += DirectoryUtil.list_dir(fp)
                else:
                    files.append(fp)
        return files

    @staticmethod
    def copy(src, dst, stdio=None):
        if not os.path.isdir(src):
            stdio and getattr(stdio, 'error', print)("cannot copy tree '%s': not a directory" % src)
            return False
        try:
            names = os.listdir(src)
        except:
            stdio and getattr(stdio, 'exception', print)("error listing files in '%s':" % (src))
            return False

        if DirectoryUtil.mkdir(dst, stdio):
            return False

        ret = True
        links = []
        for n in names:
            src_name = os.path.join(src, n)
            dst_name = os.path.join(dst, n)
            if os.path.islink(src_name):
                link_dest = os.readlink(src_name)
                links.append((link_dest, dst_name))

            elif os.path.isdir(src_name):
                ret = DirectoryUtil.copy(src_name, dst_name, stdio) and ret
            else:
                FileUtil.copy(src_name, dst_name, stdio)
        for link_dest, dst_name in links:
            FileUtil.symlink(link_dest, dst_name, stdio)
        return ret

    @staticmethod
    def mkdir(path, mode=0o755, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('mkdir %s' % path)
        try:
            os.makedirs(path, mode=mode)
            return True
        except OSError as e:
            if e.errno == 17:
                return True
            elif e.errno == 20:
                stdio and getattr(stdio, 'error', print)('%s is not a directory', path)
            else:
                stdio and getattr(stdio, 'error', print)('failed to create directory %s', path)
            stdio and getattr(stdio, 'exception', print)('')
        except:
            stdio and getattr(stdio, 'exception', print)('')
            stdio and getattr(stdio, 'error', print)('failed to create directory %s', path)
        return False

    @staticmethod
    def rm(path, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('rm %s' % path)
        try:
            if os.path.exists(path):
                if os.path.islink(path):
                    os.remove(path)
                else:
                    shutil.rmtree(path)
            return True
        except Exception as e:
            stdio and getattr(stdio, 'exception', print)('')
            stdio and getattr(stdio, 'error', print)('failed to remove %s', path)
        return False


class FileUtil(object):
    COPY_BUFSIZE = 1024 * 1024 if _WINDOWS else 64 * 1024

    @staticmethod
    def checksum(target_path, stdio=None):
        from src.common.ssh import LocalClient

        if not os.path.isfile(target_path):
            info = 'No such file: ' + target_path
            if stdio:
                getattr(stdio, 'error', print)(info)
                return False
            else:
                raise IOError(info)
        ret = LocalClient.execute_command('md5sum {}'.format(target_path), stdio=stdio)
        if ret:
            return ret.stdout.strip().split(' ')[0].encode('utf-8')
        else:
            m = hashlib.md5()
            with open(target_path, 'rb') as f:
                m.update(f.read())
            return m.hexdigest().encode(sys.getdefaultencoding())

    @staticmethod
    def copy_fileobj(fsrc, fdst):
        fsrc_read = fsrc.read
        fdst_write = fdst.write
        while True:
            buf = fsrc_read(FileUtil.COPY_BUFSIZE)
            if not buf:
                break
            fdst_write(buf)

    @staticmethod
    def copy(src, dst, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('copy %s %s' % (src, dst))
        if os.path.exists(src) and os.path.exists(dst) and os.path.samefile(src, dst):
            info = "`%s` and `%s` are the same file" % (src, dst)
            if stdio:
                getattr(stdio, 'error', print)(info)
                return False
            else:
                raise IOError(info)

        for fn in [src, dst]:
            try:
                st = os.stat(fn)
            except OSError:
                pass
            else:
                if stat.S_ISFIFO(st.st_mode):
                    info = "`%s` is a named pipe" % fn
                    if stdio:
                        getattr(stdio, 'error', print)(info)
                        return False
                    else:
                        raise IOError(info)

        try:
            if os.path.islink(src):
                FileUtil.symlink(os.readlink(src), dst)
                return True
            with FileUtil.open(src, 'rb') as fsrc, FileUtil.open(dst, 'wb') as fdst:
                FileUtil.copy_fileobj(fsrc, fdst)
                os.chmod(dst, os.stat(src).st_mode)
                return True
        except Exception as e:
            if int(getattr(e, 'errno', -1)) == 26:
                from src.common.ssh import LocalClient

                if LocalClient.execute_command('/usr/bin/cp -f %s %s' % (src, dst), stdio=stdio):
                    return True
            elif stdio:
                getattr(stdio, 'exception', print)('copy error: %s' % e)
            else:
                raise e
        return False

    @staticmethod
    def symlink(src, dst, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('link %s %s' % (src, dst))
        try:
            if DirectoryUtil.rm(dst, stdio):
                os.symlink(src, dst)
                return True
        except Exception as e:
            if stdio:
                getattr(stdio, 'exception', print)('link error: %s' % e)
            else:
                raise e
        return False

    @staticmethod
    def open(path, _type='r', encoding=None, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('open %s for %s' % (path, _type))
        if os.path.exists(path):
            if os.path.isfile(path):
                return encoding_open(path, _type, encoding=encoding)
            info = '%s is not file' % path
            if stdio:
                getattr(stdio, 'error', print)(info)
                return None
            else:
                raise IOError(info)
        dir_path, file_name = os.path.split(path)
        if not dir_path or DirectoryUtil.mkdir(dir_path, stdio=stdio):
            return encoding_open(path, _type, encoding=encoding)
        info = '%s is not file' % path
        if stdio:
            getattr(stdio, 'error', print)(info)
            return None
        else:
            raise IOError(info)

    @staticmethod
    def unzip(source, ztype=None, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('unzip %s' % source)
        if not ztype:
            ztype = source.split('.')[-1]
        try:
            if ztype == 'bz2':
                s_fn = bz2.BZ2File(source, 'r')
            elif ztype == 'xz':
                s_fn = lzma.LZMAFile(source, 'r')
            elif ztype == 'gz':
                s_fn = gzip.GzipFile(source, 'r')
            else:
                s_fn = open(source, 'r')
            return s_fn
        except:
            stdio and getattr(stdio, 'exception', print)('failed to unzip %s' % source)
        return None

    @staticmethod
    def extract_tar(tar_path, output_path, stdio=None):
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        try:
            with tarfile.open(tar_path, 'r') as tar:
                tar.extractall(path=output_path)
        except:
            stdio and getattr(stdio, 'exception', print)('failed to extract tar file %s' % tar_path)
        return None

    @staticmethod
    def rm(path, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('rm %s' % path)
        if not os.path.exists(path):
            return True
        try:
            os.remove(path)
            return True
        except:
            stdio.warn('failed to remove %s' % path)
        return False

    @staticmethod
    def move(src, dst, stdio=None):
        return shutil.move(src, dst)

    @staticmethod
    def share_lock_obj(obj, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('try to get share lock %s' % obj.name)
        fcntl.flock(obj, fcntl.LOCK_SH | fcntl.LOCK_NB)
        return obj

    @classmethod
    def share_lock(cls, path, _type='w', stdio=None):
        return cls.share_lock_obj(cls.open(path, _type=_type, stdio=stdio))

    @staticmethod
    def exclusive_lock_obj(obj, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('try to get exclusive lock %s' % obj.name)
        fcntl.flock(obj, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return obj

    @classmethod
    def exclusive_lock(cls, path, _type='w', stdio=None):
        return cls.exclusive_lock_obj(cls.open(path, _type=_type, stdio=stdio))

    @staticmethod
    def unlock(obj, stdio=None):
        stdio and getattr(stdio, 'verbose', print)('unlock %s' % obj.name)
        fcntl.flock(obj, fcntl.LOCK_UN)
        return obj

    @staticmethod
    def size_format(num, unit="B", output_str=False, stdio=None):
        if num < 0:
            raise ValueError("num cannot be negative!")
        units = ["B", "K", "M", "G", "T"]
        try:
            unit_idx = units.index(unit)
        except KeyError:
            raise ValueError("unit {0} is illegal!".format(unit))
        new_num = float(num) * (1024**unit_idx)
        unit_idx = 0
        while new_num > 1024:
            new_num = float(new_num) / 1024
            unit_idx += 1
        if unit_idx >= len(units):
            raise ValueError("size exceed 1023TB!")
        if output_str:
            return "".join(["%.3f" % new_num, units[unit_idx]])
        return new_num, units[unit_idx]

    @staticmethod
    def show_file_size_tabulate(ssh_client, file_size, stdio=None):
        format_file_size = FileUtil.size_format(int(file_size), output_str=True, stdio=stdio)
        summary_tab = []
        field_names = ["Node", "LogSize"]
        summary_tab.append((ssh_client.get_name(), format_file_size))
        return "\nZipFileInfo:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)

    @staticmethod
    def show_file_list_tabulate(ip, file_list, stdio=None):
        summary_tab = []
        field_names = ["Node", "LogList"]
        summary_tab.append((ip, file_list))
        return "\nFileListInfo:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)

    @staticmethod
    def find_all_file(base, stdio=None):
        file_list = []
        for root, ds, fs in os.walk(base):
            for f in fs:
                fullname = os.path.join(root, f)
                file_list.append(fullname)
        return file_list

    @staticmethod
    def calculate_sha256(filepath, stdio=None):

        sha256 = hashlib.sha256()
        try:
            filepath = os.path.expanduser(filepath)
            with open(filepath, 'rb') as file:
                while True:
                    data = file.read(8192)
                    if not data:
                        break
                    sha256.update(data)
            return sha256.hexdigest()
        except Exception as e:
            return ""

    @staticmethod
    def size(size_str, unit='B', stdio=None):
        unit_size_dict = {
            "b": 1,
            "B": 1,
            "k": 1024,
            "K": 1024,
            "m": 1024 * 1024,
            "M": 1024 * 1024,
            "g": 1024 * 1024 * 1024,
            "G": 1024 * 1024 * 1024,
            "t": 1024 * 1024 * 1024 * 1024,
            "T": 1024 * 1024 * 1024 * 1024,
        }
        unit_str = size_str.strip()[-1]
        if unit_str not in unit_size_dict:
            raise ValueError('unit {0} not in {1}'.format(unit_str, unit_size_dict.keys()))
        real_size = float(size_str.strip()[:-1]) * unit_size_dict[unit_str]
        if real_size < 0:
            raise ValueError('size cannot be negative!')
        return real_size / unit_size_dict[unit]

    @staticmethod
    def write_append(filename, result, stdio=None):
        with io.open(filename, 'a', encoding='utf-8') as fileobj:
            fileobj.write(u'{}'.format(result))

    @staticmethod
    def tar_gz_to_zip(temp_dir, tar_gz_file, output_zip, password, stdio):
        extract_dir = os.path.join(temp_dir, 'extracted_files_{0}'.format(str(uuid.uuid4())[:6]))

        try:
            # 1. Extract the tar.gz file
            with tarfile.open(tar_gz_file, 'r:gz') as tar:
                tar.extractall(path=extract_dir)
            stdio.verbose("tar.gz file extracted to {0}".format(extract_dir))

            # 2. Gather all extracted files and their relative paths
            files_to_compress = []
            base_paths = []
            for root, dirs, files in os.walk(extract_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    base_path = os.path.basename(root)
                    files_to_compress.append(file_path)
                    base_paths.append(base_path)
            stdio.verbose("start pyminizip compress_multiple")
            # 3. Compress the extracted files into a (possibly) encrypted zip file
            if password:
                # Use pyminizip to create the encrypted zip file
                pyminizip.compress_multiple(files_to_compress, base_paths, output_zip, password, 5)  # 5 is the compression level
                stdio.verbose("extracted files compressed into encrypted {0}".format(output_zip))
            else:
                # Create an unencrypted zip file
                pyminizip.compress_multiple(files_to_compress, base_paths, output_zip, None, 5)
                stdio.verbose("extracted files compressed into unencrypted {0}".format(output_zip))

            # 4. Remove the extracted directory
            shutil.rmtree(extract_dir)
            stdio.verbose("extracted directory {0} removed".format(extract_dir))

            # 5. Optionally remove the original tar.gz file
            os.remove(tar_gz_file)
            stdio.verbose("original tar.gz file {0} removed".format(tar_gz_file))

        except tarfile.TarError as te:
            stdio.exception("tar file error: {0}".format(te))
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir)
            return False
        except Exception as e:
            stdio.exception("an error occurred: {0}".format(e))
            if os.path.exists(extract_dir):
                shutil.rmtree(extract_dir)
            return False

        return True


class YamlLoader(YAML):

    def __init__(self, stdio=None, typ=None, pure=False, output=None, plug_ins=None):
        super(YamlLoader, self).__init__(typ=typ, pure=pure, output=output, plug_ins=plug_ins)
        self.stdio = stdio
        if not self.Representer.yaml_multi_representers and self.Representer.yaml_representers:
            self.Representer.yaml_multi_representers = self.Representer.yaml_representers

    def load(self, stream):
        try:
            return super(YamlLoader, self).load(stream)
        except Exception as e:
            if getattr(self.stdio, 'exception', False):
                self.stdio.exception('Parsing error:\n%s' % e)
            raise e

    def loads(self, yaml_content):
        try:
            stream = BytesIO()
            yaml_content = str(yaml_content).encode()
            stream.write(yaml_content)
            stream.seek(0)
            return self.load(stream)
        except Exception as e:
            if getattr(self.stdio, 'exception', False):
                self.stdio.exception('Parsing error:\n%s' % e)
            raise e

    def dump(self, data, stream=None, transform=None):
        try:
            return super(YamlLoader, self).dump(data, stream=stream, transform=transform)
        except Exception as e:
            if getattr(self.stdio, 'exception', False):
                self.stdio.exception('dump error:\n%s' % e)
            raise e

    def dumps(self, data, transform=None):
        try:
            stream = BytesIO()
            self.dump(data, stream=stream, transform=transform)
            stream.seek(0)
            content = stream.read()
            if sys.version_info.major == 2:
                return content
            return content.decode()
        except Exception as e:
            if getattr(self.stdio, 'exception', False):
                self.stdio.exception('dumps error:\n%s' % e)
            raise e


class YamlUtils(object):

    @staticmethod
    def is_yaml_file(path, stdio=None):
        if not os.path.isfile(path):
            return False
        if path.endswith(('.yaml', '.yml')):
            return True
        else:
            return False

    @staticmethod
    def read_yaml_data(file_path, stdio=None):
        if YamlUtils.is_yaml_file(file_path):
            try:
                with open(file_path, 'r') as f:
                    data = yaml.load(f, Loader=yaml.FullLoader)
                return data
            except yaml.YAMLError as exc:
                raise Exception("Error loading YAML from file, error: {0}".format(exc))

    @staticmethod
    def write_yaml_data(data, file_path, stdio=None):
        with open(file_path, 'w') as f:
            yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)

    @staticmethod
    def write_yaml_data_append(data, file_path, stdio=None):
        with open(file_path, 'a+') as f:
            yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


class CommandEnv(SafeStdio):

    def __init__(self):
        self.source_path = None
        self._env = os.environ.copy()
        self._cmd_env = {}

    def load(self, source_path, stdio=None):
        if self.source_path:
            stdio.error("Source path of env already set.")
            return False
        self.source_path = source_path
        try:
            if os.path.exists(source_path):
                with FileUtil.open(source_path, 'r') as f:
                    self._cmd_env = json.load(f)
        except:
            stdio.exception("Failed to load environments from {}".format(source_path))
            return False
        return True

    def save(self, stdio=None):
        if self.source_path is None:
            stdio.error("Command environments need to load at first.")
            return False
        stdio.verbose("save environment variables {}".format(self._cmd_env))
        try:
            with FileUtil.open(self.source_path, 'w', stdio=stdio) as f:
                json.dump(self._cmd_env, f)
        except:
            stdio.exception('Failed to save environment variables')
            return False
        return True

    def get(self, key, default=""):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def set(self, key, value, save=False, stdio=None):
        stdio.verbose("set environment variable {} value {}".format(key, value))
        self._cmd_env[key] = str(value)
        if save:
            return self.save(stdio=stdio)
        return True

    def delete(self, key, save=False, stdio=None):
        stdio.verbose("delete environment variable {}".format(key))
        if key in self._cmd_env:
            del self._cmd_env[key]
        if save:
            return self.save(stdio=stdio)
        return True

    def clear(self, save=True, stdio=None):
        self._cmd_env = {}
        if save:
            return self.save(stdio=stdio)
        return True

    def __getitem__(self, item):
        value = self._cmd_env.get(item)
        if value is None:
            value = self._env.get(item)
        if value is None:
            raise KeyError(item)
        return value

    def __contains__(self, item):
        if item in self._cmd_env:
            return True
        elif item in self._env:
            return True
        else:
            return False

    def copy(self):
        result = dict(self._env)
        result.update(self._cmd_env)
        return result

    def show_env(self):
        return self._cmd_env


class NetUtils(object):

    @staticmethod
    def get_inner_ip(stdio=None):
        localhost_ip = "127.0.0.1"
        try:
            localhost_ip = socket.gethostbyname(socket.gethostname())
            return localhost_ip
        except Exception as e:
            return localhost_ip

    @staticmethod
    def network_connectivity(url="", stdio=None):
        try:
            socket.setdefaulttimeout(3)
            response = requests.get(url, timeout=(3))
            if response.status_code is not None:
                return True
            else:
                return False
        except Exception as e:
            return False

    @staticmethod
    def download_file(url, local_filename, stdio=None):
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        return local_filename


COMMAND_ENV = CommandEnv()


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            # 将datetime对象转换为字符串
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        # 其他类型按默认处理
        return super().default(obj)


class TimeUtils(object):

    @staticmethod
    def parse_time_sec(time_str):
        unit = time_str[-1]
        value = int(time_str[:-1])
        if unit == "s":
            value *= 1
        elif unit == "m":
            value *= 60
        elif unit == "h":
            value *= 3600
        elif unit == "d":
            value *= 3600 * 24
        else:
            raise Exception('%s parse time to second fialed:' % (time_str))
        return value

    @staticmethod
    def get_format_time(time_str, stdio=None):
        try:
            return datetime.datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            stdio.exception('%s parse time fialed, error:\n%s, time format need to be %s' % (time_str, e, '%Y-%m-%d %H:%M:%S'))

    @staticmethod
    def sub_minutes(t, delta, stdio=None):
        try:
            return (t - datetime.timedelta(minutes=delta)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            stdio.exception('%s get time fialed, error:\n%s' % (t, e))

    @staticmethod
    def add_minutes(t, delta, stdio=None):
        try:
            return (t + datetime.timedelta(minutes=delta)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            stdio.exception('%s get time fialed, error:\n%s' % (t, e))

    @staticmethod
    def parse_time_from_to(from_time=None, to_time=None, stdio=None):
        format_from_time = None
        format_to_time = None
        sucess = False
        if from_time:
            format_from_time = TimeUtils.get_format_time(from_time, stdio)
            format_to_time = TimeUtils.get_format_time(to_time, stdio) if to_time else TimeUtils.add_minutes(format_from_time, 30)
        else:
            if to_time:
                format_to_time = TimeUtils.get_format_time(to_time, stdio)
                format_from_time = TimeUtils.sub_minutes(format_to_time, 30)
        if format_from_time and format_to_time:
            sucess = True
        return format_from_time, format_to_time, sucess

    @staticmethod
    def parse_time_since(since=None, stdio=None):
        now_time = datetime.datetime.now()
        format_to_time = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
        try:
            format_from_time = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_sec(since))).strftime('%Y-%m-%d %H:%M:%S')
        except Exception as e:
            stdio.exception('%s parse time fialed, error:\n%s' % (since, e))
            format_from_time = TimeUtils.sub_minutes(format_to_time, 30)
        return format_from_time, format_to_time

    @staticmethod
    def get_current_us_timestamp(stdio=None):
        time_second = time.time()
        return int(time_second * 1000000)

    @staticmethod
    def parse_time_length_to_sec(time_length_str, stdio=None):
        unit = time_length_str[-1]
        if unit != "m" and unit != "h" and unit != "d":
            raise Exception("time length must be format 'n'<m|h|d>")
        value = int(time_length_str[:-1])
        if unit == "m":
            value *= 60
        elif unit == "h":
            value *= 3600
        elif unit == "d":
            value *= 3600 * 24
        else:
            raise Exception("time length must be format 'n'<m|h|d>")
        return int(value)

    @staticmethod
    def datetime_to_timestamp(datetime_str, stdio=None):
        # yyyy-mm-dd hh:mm:ss.uuuuus or yyyy-mm-dd hh:mm:ss
        try:
            if len(datetime_str) > 19:
                dt = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S.%f')
            else:
                dt = datetime.datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
            return int(dt.timestamp() * 1000000)
        except Exception as e:
            return 0

    @staticmethod
    def trans_datetime_utc_to_local(datetime_str, stdio=None):
        utct_date = datetime.datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%S")  # 2020-12-01 03:21:57
        local_date = utct_date + datetime.timedelta(hours=8)  # 加上时区
        local_date_srt = datetime.datetime.strftime(local_date, "%Y-%m-%d %H:%M:%S")  # 2020-12-01 11:21:57
        trans_res = datetime.datetime.strptime(local_date_srt, "%Y-%m-%d %H:%M:%S")
        return str(trans_res)

    @staticmethod
    def timestamp_to_filename_time(timestamp, stdio=None):
        second_timestamp = timestamp / 1000000
        time_obj = time.localtime(int(second_timestamp))
        filename_time_str = time.strftime('%Y%m%d%H%M%S', time_obj)
        return filename_time_str

    @staticmethod
    def parse_time_str(arg_time, stdio=None):
        format_time = ''
        try:
            format_time = datetime.datetime.strptime(arg_time, "%Y-%m-%d %H:%M:%S")
        except ValueError as e:
            raise ValueError("time option {0} must be formatted as {1}".format(arg_time, '"%Y-%m-%d %H:%M:%S"'))
        return format_time

    @staticmethod
    def filename_time_to_datetime(filename_time, stdio=None):
        """transform yyyymmddhhmmss to yyyy-mm-dd hh:mm:ss"""
        if filename_time != "":
            return "{0}-{1}-{2} {3}:{4}:{5}".format(filename_time[0:4], filename_time[4:6], filename_time[6:8], filename_time[8:10], filename_time[10:12], filename_time[12:14])
        else:
            return ""

    @staticmethod
    def extract_filename_time_from_log_name(log_name, stdio=None):
        """eg: xxx.20221226231617"""
        log_name_fields = log_name.split(".")
        if bytes.isdigit(log_name_fields[-1].encode("utf-8")) and len(log_name_fields[-1]) >= 14:
            return log_name_fields[-1]
        return ""

    @staticmethod
    def extract_time_from_log_file_text(log_text, stdio=None):
        # 因为 yyyy-mm-dd hh:mm:ss.000000 的格式已经占了27个字符，所以如果传进来的字符串包含时间信息，那长度一定大于27
        if len(log_text) > 27:
            if log_text.startswith("["):
                time_str = log_text[1 : log_text.find(']')]
            else:
                time_str = log_text[0 : log_text.find(',')]
            time_without_us = time_str[0 : time_str.find('.')]
            try:
                format_time = datetime.datetime.strptime(time_without_us, "%Y-%m-%d %H:%M:%S")
                format_time_str = time.strftime("%Y-%m-%d %H:%M:%S", format_time.timetuple())
            except Exception as e:
                format_time_str = ""
        else:
            format_time_str = ""
        if format_time_str == "":
            time_pattern = r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'
            match = re.search(time_pattern, log_text)
            if match:
                format_time_str = match.group(0)
        return format_time_str

    @staticmethod
    def get_time_rounding(dt, step=0, rounding_level="s", stdio=None):
        """
        计算整分钟，整小时，整天的时间
        :param step: 往前或往后跳跃取整值，默认为0，即当前所在的时间，正数为往后，负数往前。
                    例如：
                    step = 0 时 2022-07-26 17:38:21.869993 取整秒后为 2022-07-26 17:38:21
                    step = 1 时 2022-07-26 17:38:21.869993 取整秒后为 2022-07-26 17:38:22
                    step = -1 时 2022-07-26 17:38:21.869993 取整秒后为 2022-07-26 17:38:20
        :param rounding_level: 字符串格式。
                    "s": 按秒取整；"min": 按分钟取整；"hour": 按小时取整；"days": 按天取整
        :return: 处理后的时间
        """
        if rounding_level == "days":
            td = timedelta(days=-step, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=dt.hour, weeks=0)
            new_dt = dt - td
        elif rounding_level == "hour":
            td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=dt.minute, hours=-step, weeks=0)
            new_dt = dt - td
        elif rounding_level == "min":
            td = timedelta(days=0, seconds=dt.second, microseconds=dt.microsecond, milliseconds=0, minutes=-step, hours=0, weeks=0)
            new_dt = dt - td
        elif rounding_level == "s":
            td = timedelta(days=0, seconds=-step, microseconds=dt.microsecond, milliseconds=0, minutes=0, hours=0, weeks=0)
            new_dt = dt - td
        else:
            new_dt = dt
        return str(new_dt)

    @staticmethod
    def trans_time(size: int):
        """
        将时间单位转化为字符串
        :param size: 时间单位，单位为微秒
        :return: 转化后的字符串
        """
        if size < 0:
            return 'NO_END'
        mapping = [
            (86400000000, 'd'),
            (3600000000, 'h'),
            (60000000, 'm'),
            (1000000, 's'),
            (1000, 'ms'),
            (1, 'μs'),
        ]
        for unit, unit_str in mapping:
            if size >= unit:
                if unit == 1:
                    return '{} {}'.format(size, unit_str)
                else:
                    return '{:.3f} {}'.format(size / unit, unit_str)
        return '0'

    @staticmethod
    def str_2_timestamp(t, stdio=None):
        if isinstance(t, int):
            return t
        temp = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S.%f')
        return int(datetime.datetime.timestamp(temp) * 10**6)


class StringUtils(object):

    @staticmethod
    def parse_mysql_conn(cli_conn_str, stdio=None):
        db_info = {}
        # 处理密码选项，注意区分短选项和长选项的密码
        password_pattern = re.compile(r'(-p|--password=)([^ ]*)')
        password_match = password_pattern.search(cli_conn_str)
        if password_match:
            password = password_match.group(2)
            # 如果'-p'后面没有跟具体值，则设为''
            db_info['password'] = password if password else ''
            # 去除密码部分，避免后续解析出错
            cli_conn_str = cli_conn_str[: password_match.start()] + cli_conn_str[password_match.end() :].strip()

        # 模式匹配短选项
        short_opt_pattern = re.compile(r'-([hPuD])(\S*)')
        for match in short_opt_pattern.finditer(cli_conn_str):
            opt, value = match.groups()
            if opt == 'h':
                db_info['host'] = value
            elif opt == 'u':
                db_info['user'] = value
            elif opt == 'P':
                try:
                    db_info['port'] = int(value)
                except ValueError:
                    if stdio:
                        print("Invalid port number.")
                    return False
            elif opt == 'D':
                db_info['database'] = value

        # 长选项处理
        long_opt_pattern = re.compile(r'--(\w+)=([^ ]+)')
        for match in long_opt_pattern.finditer(cli_conn_str):
            opt, value = match.groups()
            if opt in ['host', 'user', 'port', 'dbname', 'database']:
                db_info[opt if opt != 'dbname' else 'database'] = value

        # 最后一个参数处理，如果未指定数据库名且最后的参数不是选项，则认为是数据库名
        parts = cli_conn_str.split()
        if parts and parts[-1][0] != '-' and 'database' not in db_info:
            db_info['database'] = parts[-1]

        return db_info

    @staticmethod
    def validate_db_info(db_info, stdio=None):
        required_keys = {'database', 'host', 'user', 'port'}
        if not required_keys.issubset(db_info.keys()):
            return False
        if not isinstance(db_info['port'], int):
            return False
        for key, value in db_info.items():
            if key != 'port' and not isinstance(value, str):
                return False
        return True

    @staticmethod
    def parse_env(env_string, stdio=None):
        env_dict = {}
        inner_str = env_string[1:-1].strip()
        pairs = inner_str.split(',')
        for pair in pairs:
            pair = pair.strip()
            key_value = pair.split('=', 1)
            if len(key_value) == 2:
                key, value = key_value
                key = key.strip()
                value = value.strip()
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                env_dict[key] = value
        return env_dict

    @staticmethod
    def parse_env_display(env_list):
        env_dict = {}
        if not env_list:
            return {}
        for env_string in env_list:
            # 分割键和值
            key_value = env_string.split('=', 1)
            if len(key_value) == 2:
                key, value = key_value
                if value.startswith('"') and value.endswith('"'):
                    value = value[1:-1]
                elif value.startswith("'") and value.endswith("'"):
                    value = value[1:-1]
                env_dict[key.strip()] = value.strip()
        return env_dict

    @staticmethod
    def extract_parameters(query_template):
        # 使用正则表达式查找占位符
        pattern = re.compile(r'#\{(\w+)\}')
        parameters = pattern.findall(query_template)
        return parameters

    @staticmethod
    def replace_parameters(query_template, params):
        # 使用正则表达式查找占位符
        pattern = re.compile(r'#\{(\w+)\}')

        # 定义替换函数
        def replacer(match):
            key = match.group(1)
            return str(params.get(key, match.group(0)))

        # 替换占位符
        query = pattern.sub(replacer, query_template)
        return query

    @staticmethod
    def get_observer_ip_port_from_trace_id(trace_id):
        if len(trace_id) >= 50:
            raise ValueError(f"Trace_id({trace_id}) is invalid due to its length.")

        if trace_id[0] == 'Y':
            id_ = trace_id.split('-')[0].split('Y')[1]
            uval = int(id_, 16)
            ip = uval & 0xFFFFFFFF
            port = (uval >> 32) & 0xFFFF
            ip_str = f"{(ip >> 24) & 0xFF}.{(ip >> 16) & 0xFF}.{(ip >> 8) & 0xFF}.{ip & 0xFF}"
            origin_ip_port = f"{ip_str}:{port}"
        else:
            parts = trace_id.split('-')
            processed_parts = [hex(int(t))[2:].upper().zfill(16 if idx == 1 else 0) for idx, t in enumerate(parts)]
            s = 'Y' + '-'.join(processed_parts)
            origin_ip_port = StringUtils.get_observer_ip_port_from_trace_id(s)
        return origin_ip_port

    @staticmethod
    def parse_range_string(range_str, nu, stdio=None):
        # parse_range_string: Determine whether variable 'nu' is within the range of 'range_str'
        # 提取范围字符串中的数字
        nu = int(nu)
        range_str = range_str.replace(" ", "")
        # range_str = range_str.replace(".", "")
        start, end = range_str[1:-1].split(',')
        need_less = True
        need_than = True
        # 将数字转换为整数
        if start.strip() == "*":
            need_less = False
        else:
            start = float(start.strip())
        if end.strip() == "*":
            need_than = False
        else:
            end = float(end.strip())
        stdio and getattr(stdio, 'verbose', print)('range_str is %s' % range_str)

        if need_less:
            if range_str[0] == "(":
                if nu <= start:
                    return False
            elif range_str[0] == "[":
                if nu < start:
                    return False
        if need_than:
            if range_str[-1] == ")":
                if nu >= end:
                    return False
            elif range_str[-1] == "]":
                if nu > end:
                    return False
        return True

    @staticmethod
    def build_str_on_expr_by_dict(expr, variable_dict, stdio=None):
        s = expr
        d = variable_dict

        def replacer(match):
            key = match.group(1)
            return str(d.get(key, match.group(0)))

        return re.sub(r'#\{(\w+)\}', replacer, s)

    @staticmethod
    def build_sql_on_expr_by_dict(expr, variable_dict, stdio=None):
        s = expr
        d = variable_dict

        def replacer(match):
            key = match.group(1)
            value = str(d.get(key, match.group(0)))
            return f'"{value}"'

        return re.sub(r'\$\{(\w+)\}', replacer, s)

    @staticmethod
    def node_cut_passwd_for_log(obj, stdio=None):
        if isinstance(obj, dict):
            new_obj = {}
            for key, value in obj.items():
                if key == "password" or key == "ssh_password":
                    continue
                new_obj[key] = StringUtils.node_cut_passwd_for_log(value)
            return new_obj
        elif isinstance(obj, list):
            return [StringUtils.node_cut_passwd_for_log(item) for item in obj]
        else:
            return obj

    @staticmethod
    def split_ip(ip_str, stdio=None):
        pattern = r'((?:[0-9]{1,3}\.){3}[0-9]{1,3}|(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4})'
        result = re.findall(pattern, ip_str)
        if not result:
            pattern = r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
            result = re.findall(pattern, ip_str)
            return result
        return result

    @staticmethod
    def is_chinese(s, stdio=None):
        try:
            s.encode('ascii')
        except UnicodeEncodeError:
            return True
        else:
            return False

    @staticmethod
    def compare_versions_greater(v1, v2, stdio=None):
        for i, j in zip(map(int, v1.split(".")), map(int, v2.split("."))):
            if i == j:
                continue
            return i > j
        return len(v1.split(".")) > len(v2.split("."))

    @staticmethod
    def compare_versions_lower(v1, v2, stdio=None):
        for i, j in zip(map(int, v1.split(".")), map(int, v2.split("."))):
            if i == j:
                continue
            return i < j
        return len(v1.split(".")) < len(v2.split("."))

    @staticmethod
    def mask_passwords(data):
        # Make a deep copy of the data to avoid modifying the original
        masked_data = copy.deepcopy(data)

        if isinstance(masked_data, dict):
            for key, value in masked_data.items():
                if 'password' in key.lower():
                    if not isinstance(value, str):
                        value = str(value)
                    masked_data[key] = '*' * (len(value) if value else 1)
                elif isinstance(value, (dict, list)):
                    masked_data[key] = StringUtils.mask_passwords(value)
        elif isinstance(masked_data, list):
            for index, item in enumerate(masked_data):
                if isinstance(item, (dict, list)):
                    masked_data[index] = StringUtils.mask_passwords(item)

        return masked_data

    @staticmethod
    def parse_optimization_info(text, stdio):
        # Fixed module names that should not be treated as table names
        module_names = {'Outputs & filters', 'Used Hint', 'Qb name trace', 'Outline Data', 'Optimization Info', 'Plan Type', 'Note'}

        tables = {}
        current_table = None
        lines = text.splitlines()

        for line in lines:
            # Remove leading/trailing whitespace and '|' characters
            line = line.strip().strip('|').strip()
            if not line or line.startswith('-') or line.startswith('|'):
                # Skip empty lines, separator lines, and lines starting with '|'
                continue

            try:
                # Check if it's the start of a new table (contains ':' and ends with it, and is not a module name)
                if ':' in line and line.endswith(':') and line.rstrip(':').strip() not in module_names:
                    current_table = line.rstrip(':').strip()
                    tables[current_table] = None
                elif current_table:
                    match_stats_version = re.search(r'stats version:(\d+)', line)
                    match_stats_info = re.search(r'stats info:\[version=(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+),\s*is_locked=\d+,\s*is_expired=\d+]', line)

                    if match_stats_version:
                        stats_version = int(match_stats_version.group(1))
                        tables[current_table] = {'type': 'version', 'value': stats_version}
                    elif match_stats_info:
                        stats_time_str = match_stats_info.group(1)
                        stats_time = datetime.datetime.strptime(stats_time_str, '%Y-%m-%d %H:%M:%S.%f')
                        tables[current_table] = {'type': 'info', 'value': stats_time}
            except Exception as e:
                return None

        messages = []
        for table, stats_data in tables.items():
            if stats_data is None:
                stdio.verbose(f"Could not find stats version information for the {table} table.")
            else:
                try:
                    if stats_data['type'] == 'version':
                        if stats_data['value'] == 0:
                            message = f"In explain extended [Optimization Info], the [stats version] for the {table} table is 0, indicating that statistics have not been collected. Please collect statistics."
                            stdio.print(message)
                            messages.append(message)
                        else:
                            stats_time = datetime.datetime.utcfromtimestamp(stats_data['value'] // 1000000).strftime('%Y-%m-%d %H:%M:%S')
                            if (datetime.datetime.now().timestamp() - stats_data['value'] / 1000000) > 24 * 60 * 60:
                                message = f"In explain extended [Optimization Info], the [stats version] time for the {table} table is {stats_time}, indicating that statistics are over 24 hours old. Please collect statistics."
                                stdio.print(message)
                                messages.append(message)
                            else:
                                message = f"The statistics are up-to-date. The last collection time for the {table} table was {stats_time}. No action needed."
                                stdio.verbose(message)
                    elif stats_data['type'] == 'info':
                        if (datetime.datetime.now() - stats_data['value']).total_seconds() > 24 * 60 * 60:
                            message = (
                                f"In explain extended [Optimization Info], the [stats version] time for the {table} table is {stats_data['value'].strftime('%Y-%m-%d %H:%M:%S')}, indicating that statistics are over 24 hours old. Please collect statistics."
                            )
                            stdio.print(message)
                            messages.append(message)
                        else:
                            message = f"The statistics are up-to-date. The last collection time for the {table} table is {stats_data['value'].strftime('%Y-%m-%d %H:%M:%S')}，No action needed."
                            stdio.verbose(message)
                except Exception as e:
                    stdio.verbose(f"Error processing {table} table: {e}")
        return "\n".join(messages)

    def generate_numeric_code(length=6):
        """生成指定长度的纯数字验证码"""
        return ''.join(random.choices('0123456789', k=length))

    def generate_alphanum_code(length=8):
        """生成包含大小写字母和数字的随机码"""
        characters = string.ascii_letters + string.digits  # a-zA-Z0-9
        return ''.join(random.choices(characters, k=length))


class Cursor(SafeStdio):

    def __init__(self, ip, port, user='root', tenant='sys', password='', stdio=None):
        self.stdio = stdio
        self.ip = ip
        self.port = port
        self._user = user
        self.tenant = tenant
        self.password = password
        self.cursor = None
        self.db = None
        self._connect()
        self._raise_exception = False
        self._raise_cursor = None

    @property
    def user(self):
        if "@" in self._user:
            return self._user
        if self.tenant:
            return "{}@{}".format(self._user, self.tenant)
        else:
            return self._user

    @property
    def raise_cursor(self):
        if self._raise_cursor:
            return self._raise_cursor
        raise_cursor = copy(self)
        raise_cursor._raise_exception = True
        self._raise_cursor = raise_cursor
        return raise_cursor

    if sys.version_info.major == 2:

        def _connect(self):
            self.stdio.verbose('connect %s -P%s -u%s -p%s' % (self.ip, self.port, self.user, self.password))
            self.db = mysql.connect(host=self.ip, user=self.user, port=int(self.port), passwd=str(self.password))
            self.cursor = self.db.cursor(cursorclass=mysql.cursors.DictCursor)

    else:

        def _connect(self):
            self.stdio.verbose('connect %s -P%s -u%s -p%s' % (self.ip, self.port, self.user, self.password))
            self.db = mysql.connect(host=self.ip, user=self.user, port=int(self.port), password=str(self.password), cursorclass=mysql.cursors.DictCursor)
            self.cursor = self.db.cursor()

    def new_cursor(self, tenant='sys', user='root', password='', ip='', port='', print_exception=True):
        try:
            ip = ip if ip else self.ip
            port = port if port else self.port
            return Cursor(ip=ip, port=port, user=user, tenant=tenant, password=password, stdio=self.stdio)
        except:
            print_exception and self.stdio.exception('')
            self.stdio.verbose('fail to connect %s -P%s -u%s@%s  -p%s' % (ip, port, user, tenant, password))
            return None

    def execute(self, sql, args=None, execute_func=None, raise_exception=False, exc_level='error', stdio=None):
        try:
            stdio.verbose('execute sql: %s. args: %s' % (sql, args))
            self.cursor.execute(sql, args)
            if not execute_func:
                return self.cursor
            return getattr(self.cursor, execute_func)()
        except Exception as e:
            getattr(stdio, exc_level)(EC_SQL_EXECUTE_FAILED.format(sql=sql))
            if raise_exception is None:
                raise_exception = self._raise_exception
            if raise_exception:
                stdio.exception('')
                raise e
            return False

    def fetchone(self, sql, args=None, raise_exception=False, exc_level='error', stdio=None):
        return self.execute(sql, args=args, execute_func='fetchone', raise_exception=raise_exception, exc_level=exc_level, stdio=stdio)

    def fetchall(self, sql, args=None, raise_exception=False, exc_level='error', stdio=None):
        return self.execute(sql, args=args, execute_func='fetchall', raise_exception=raise_exception, exc_level=exc_level, stdio=stdio)

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.db:
            self.db.close()
            self.db = None


class Util(object):

    @staticmethod
    def get_option(options, key, default=None):
        if not hasattr(options, key):
            return default
        value = getattr(options, key)
        if value is None:
            value = default
        return value

    @staticmethod
    def set_option(options, key, value):
        setattr(options, key, value)

    @staticmethod
    def convert_to_number(s, stdio=None):
        if isinstance(s, (int, float)):
            return s
        if isinstance(s, decimal.Decimal):
            try:
                return float(s)
            except:
                return s

        if isinstance(s, str):
            if s.startswith("-"):
                if s[1:].isdigit():
                    return int(s)
                elif s[1:].isdecimal():  # 判断字符串是否全为数字或小数点
                    return float(s)  # 如果是，转换为浮点数
            if s.isdigit():  # 判断字符串是否全为数字
                return int(s)  # 如果是，转换为整数
            elif s.isdecimal():  # 判断字符串是否全为数字或小数点
                return float(s)  # 如果是，转换为浮点数
            try:
                return float(s)
            except Exception:
                pass

        return s

    @staticmethod
    def print_scene(scene_dict, stdio):
        columns_to_print = ['command', 'info_en', 'info_cn']
        keys = columns_to_print
        table_data = [[value[key] for key in keys] for value in scene_dict.values()]
        column_widths = [max(len(str(item)) * (StringUtils.is_chinese(item) or 1) for item in column) for column in zip(*table_data)]
        table_data.insert(0, keys)
        Util.print_line(length=sum(column_widths) + 5, stdio=stdio)
        for i in range(len(table_data)):
            stdio.print(Fore.GREEN + "   ".join(f"{item:<{width}}" for item, width in zip(table_data[i], column_widths)) + Style.RESET_ALL)
            if i == 0:
                Util.print_line(length=sum(column_widths) + 5, stdio=stdio)
        Util.print_line(length=sum(column_widths) + 5, stdio=stdio)

    @staticmethod
    def print_line(char='-', length=50, stdio=None):
        stdio.print(char * length)

    @staticmethod
    def print_title(name, stdio):
        stdio.print("\n[{0}]:".format(name))

    @staticmethod
    def gen_password(length=8, chars=string.ascii_letters + string.digits, stdio=None):
        return ''.join([choice(chars) for i in range(length)])

    @staticmethod
    def retry(retry_count=3, retry_interval=2, stdio=None):
        def real_decorator(decor_method):
            def wrapper(*args, **kwargs):
                for count in range(retry_count):
                    try:
                        return_values = decor_method(*args, **kwargs)
                        return return_values
                    except Exception as e:
                        if getattr(stdio, "Function execution %s retry: %s " % (decor_method.__name__, count + 1), False):
                            stdio.exception('dumps error:\n%s' % e)
                        time.sleep(retry_interval)
                        if count == retry_count - 1:
                            raise e

            return wrapper

        return real_decorator

    @staticmethod
    def get_nodes_list(context, nodes, stdio=None):
        ctx_nodes = context.get_variable("filter_nodes_list", None)
        if ctx_nodes is not None and len(ctx_nodes) > 0:
            new_nodes = []
            for node in nodes:
                if node in ctx_nodes:
                    new_nodes.append(node)
            if len(new_nodes) != len(ctx_nodes) or len(new_nodes) == 0:
                stdio.warn("Warn: no nodes found in context.")
                return None
            return new_nodes
        return None

    @staticmethod
    def check_none_values(config, stdio):
        """
        Check if any values in the given configuration dictionary are None.
        If any value is None, print the specific information and return False.
        If all values are not None, return True.

        :param config: Dictionary containing configuration items
        :return: True if no None values are found, otherwise False
        """
        # First, check the top-level key-value pairs
        for key, value in config.items():
            if value is None:
                stdio.error("The value of '{0}' is None.".format(key))
                return False

            # If the value is a dictionary, recursively check the sub-dictionary
            if isinstance(value, dict):
                if not Util.check_none_values(value, stdio):
                    return False
        return True


class SQLUtil(object):
    re_trace = re.compile(r'''\/\*.*trace_id((?!\/\*).)*rpc_id.*\*\/''', re.VERBOSE)
    re_annotation = re.compile(r'''\/\*((?!\/\*).)*\*\/''', re.VERBOSE)
    re_interval = re.compile(
        r'''interval\s?(\?|\-?\d+)\s?(day|hour|minute|second|microsecond|week|month|quarter|year|second_microsecond|minute_microsecond|minute_second|hour_microsecond|hour_second|hour_minute|day_microsecond|day_second|day_minute|day_hour|year_month)''',
        re.VERBOSE,
    )
    re_force_index = re.compile(r'''force[\s]index[\s][(]\w+[)]''', re.VERBOSE)
    re_cast_1 = re.compile(r'''cast\(.*?\(.*?\)\)''', re.VERBOSE)
    re_cast_2 = re.compile(r'''cast\(.*?\)''', re.VERBOSE)
    re_now = re.compile(r'''now\(\)''', re.VERBOSE)

    def remove_sql_text_affects_parser(self, sql):
        sql = sql.lower().strip()
        sql = self.remove_hint_and_annotate(sql)
        sql = self.remove_force_index(sql)
        sql = self.remove_now_in_insert(sql)
        sql = self.remove_semicolon(sql)
        return sql

    def remove_hint_and_annotate(self, sql):
        sql = sql.lower()
        sql = re.sub(self.re_annotation, '', sql)
        sql = re.sub(self.re_trace, '', sql)
        return sql

    def replace_interval_day(self, sql):
        sql = sql.lower()
        sql = re.sub(self.re_interval, '?', sql)
        return sql

    def remove_force_index(self, sql):
        sql = sql.lower()
        sql = re.sub(self.re_force_index, '', sql)
        return sql

    def remove_cast(self, sql):
        sql = sql.lower()
        sql = re.sub(self.re_cast_1, '?', sql)
        sql = re.sub(self.re_cast_2, '?', sql)
        return sql

    def remove_now_in_insert(self, sql):
        sql = sql.lower().lstrip()
        if sql.startswith('insert'):
            sql = re.sub(self.re_now, '?', sql)
        return sql

    def remove_semicolon(self, sql):
        sql = sql.strip()
        return sql[:-1] if sql[-1] == ';' else sql

    def get_db_id(self, database_alias, user_id):
        return database_alias + '-' + user_id


class SQLTableExtractor:

    def __init__(self):
        self.pattern_db_table = re.compile(r'\b(?:FROM|JOIN|INTO|UPDATE)\s+([^\s.,;]+)(?:\.([^\s.,;]+))?', re.IGNORECASE)

    def parse(self, sql):
        """
        Parse SQL statements and return a list containing tuples of (database name, table name).
        If no database name is specified, the database name will be None.
        :param sql: The SQL statement to be parsed
        :return: A list of tuples containing (database name, table name)
        """
        matches = self.pattern_db_table.findall(sql)
        results = []
        for match in matches:
            db_name, table_name = match
            if not table_name:
                table_name = db_name
                db_name = None
            results.append((db_name, table_name))
        return results


def check_new_obdiag_version(stdio):
    ssl._create_default_https_context = ssl._create_unverified_context
    try:
        work_tag = NetUtils.network_connectivity("https://" + "cn-wan-api.oceanbase.com" + "/wanApi/forum/download/v1/getAllDownloadCenterData")
        if not work_tag:
            return
        conn = http.client.HTTPSConnection("cn-wan-api.oceanbase.com", timeout=1)
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=utf-8',
            'Referer': 'https://www.oceanbase.com/',
        }
        payload = json.dumps({"type": "community"})
        conn.request("POST", "/wanApi/forum/download/v1/getAllDownloadCenterData", payload, headers)
        res = conn.getresponse()
        data = res.read()
        json_data = json.loads(data)
        productList = (
            json_data.get("data", {})
            .get('productCategoryList', [])[3]
            .get(
                "productList",
            )
        )
        for product in productList:
            if "obdiag" in product.get("description", ""):
                latest_version = product.get("recommendVersionVO", {}).get("version", None)
                if latest_version:
                    if StringUtils.compare_versions_greater(latest_version, OBDIAG_VERSION):
                        stdio.print('\nobdiag latest version is {0}, current version is {1}, please update obdiag to the latest version'.format(latest_version, OBDIAG_VERSION))
    except Exception as e:
        stdio.verbose(f"Error: {e}")
        return None


@staticmethod
def timestamp_to_datetime(timestamp):
    return datetime.fromtimestamp(float(timestamp) / 1000000)


@staticmethod
def scn_to_datetime(scn):
    return datetime.fromtimestamp(float(scn) / 1000000000)


@staticmethod
def str_to_datetime(time_str):
    time_str = str(time_str).strip()
    formats = ["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"]  # with microseconds  # standard format  # minute precision
    for fmt in formats:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            continue
    raise ValueError("Failed to parse time string: '%s'. Supported formats: YYYY-MM-DD HH:MM:SS[.fff]" % time_str)


@staticmethod
def parse_log_time(line):
    if CommonUtil.log_time_pattern is None:
        p = "\[(?P<date_time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\]"
        CommonUtil.log_time_pattern = re.compile(p)
    m = CommonUtil.log_time_pattern.search(line)
    t = None
    if m:
        t = datetime.strptime(str(m.group('date_time')), "%Y-%m-%d %H:%M:%S.%f")
    else:
        print("parse_log_time failed {}".format(line))
    return t


@staticmethod
def parse_time(time_s):
    time_str = str(time_s)
    if '.' in time_str:
        date_format = '%Y-%m-%d %H:%M:%S.%f'
    else:
        date_format = '%Y-%m-%d %H:%M:%S'
    return datetime.strptime(time_str, date_format)


@staticmethod
def print_table(data_list):
    if not data_list:
        return ""

    # Convert all items to strings and store in new structure
    str_data = [[str(item) for item in row] for row in data_list]

    # Calculate column widths based on string lengths
    column_widths = [max(len(item) for item in column) for column in zip(*str_data)]

    # Generate formatted rows using list comprehension
    formatted_rows = [" | ".join(item.ljust(width) for item, width in zip(row, column_widths)) for row in str_data]

    # Combine rows with newline separators
    return '\n'.join(formatted_rows)
