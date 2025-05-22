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
@time: 2025/5/21
@file: oss_error.py
@desc:
"""
import json
import os.path
import re
import sys
import uuid

from src.common.command import upload_file
from src.handler.rca.rca_exception import (
    RCAInitException,
    RCAExecuteException,
    RCANotNeedExecuteException,
)
from src.handler.rca.rca_handler import RcaScene, RCA_ResultRecord
from src.common.tool import DateTimeEncoder
from src.common.tool import StringUtils


class OSSError(RcaScene):
    def __init__(self):
        super().__init__()
        self.s3_url_encode_type = None
        self.storage_info = None
        self.backup_file_path = None

    def init(self, context):
        super().init(context)
        self.backup_file_path = self.input_parameters.get("backup_file_path")
        if not self.backup_file_path:
            raise RCAInitException("backup_file_path is not set.")
        self.storage_info = self.input_parameters.get("storage_info")
        if self.backup_file_path.startswith("oss://") or self.backup_file_path.startswith("cos://") or self.backup_file_path.startswith("s3://"):
            if not self.storage_info:
                raise RCAInitException("if the type is [oss, cos, s3] need set storage_info.")
        self.s3_url_encode_type = self.input_parameters.get("s3_url_encode_type")
        if self.backup_file_path.startswith("s3://"):
            if not self.s3_url_encode_type:
                self.stdio.print("if the type is [s3] need set s3_url_encode_type.")

    def execute(self):
        remote_dir = "/tmp" + "/obadmin_tmp_{0}".format(str(uuid.uuid4())[:6])
        try:
            for node in self.observer_nodes:
                ssh_client = node["ssher"]
                try:
                    # check if the remote dir is existed
                    exist_data = ssh_client.exec_cmd("ls {0}".format(remote_dir))
                    if "No such file or directory" not in exist_data:
                        # the remote dir is not existed
                        self.stdio.verbose("remote dir {0} is not exist".format(remote_dir))
                        ssh_client.exec_cmd("mkdir -p {0}".format(remote_dir))
                    # check obadmin is existed
                    obadmin_remote_path = os.path.join(remote_dir, "ob_admin")
                    if not os.path.exists(obadmin_remote_path):
                        self.stdio.verbose("obadmin is not exist")
                        if getattr(sys, 'frozen', False):
                            absPath = os.path.dirname(sys.executable)
                        else:
                            absPath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
                        # check node is x86_64 or aarch64
                        node_arch = ssh_client.exec_cmd("arch")
                        if node_arch == "aarch64" or node_arch == "x86_64":
                            obadmin_local_stored_full_path = os.path.join(absPath, "obadmin_{0}".format(node_arch))
                        else:
                            self.stdio.warn("node:{1} arch {0} not support obadmin. obdiag will try use x86 obadmin".format(node_arch, ssh_client.get_name()))
                            obadmin_local_stored_full_path = os.path.join(absPath, "obadmin_x86_64")
                        upload_file(ssh_client, obadmin_local_stored_full_path, obadmin_remote_path, self.context.stdio)
                        # chmod +x obadmin
                        cmd = "chmod +x {0} ".format(obadmin_remote_path)
                        ssh_client.exec_cmd(cmd)
                        # build data dir
                        remote_obadmin_data_dir = os.path.join(remote_dir, "obadmin_{0}".format(ssh_client.get_name()))
                        self.record.add_record("node:{0} obadmin data dir: {1}".format(ssh_client.get_name(), remote_obadmin_data_dir))
                        ssh_client.exec_cmd("mkdir -p {0}".format(remote_obadmin_data_dir))
                        # execute obadmin
                        home_path = node.get("home_path")
                        cmd = "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:{0}/lib && export OB_ADMIN_LOG_DIR={1}/tmp_log &&".format(home_path, remote_dir)
                        cmd += " {0} test_io_device ".format(obadmin_remote_path)
                        if self.backup_file_path:
                            cmd += "-d {0} ".format(self.backup_file_path)
                        if self.storage_info:
                            cmd += "-s {0} ".format(self.storage_info)
                        if self.s3_url_encode_type:
                            cmd += "-e {0} ".format(self.s3_url_encode_type)
                        cmd += "> {0}/obadmin_{1}.log 2>{0}/obadmin_{1}.log_more".format(remote_obadmin_data_dir, ssh_client.get_name())
                        ssh_client.exec_cmd(cmd)
                        self.record.add_record("node:{0} execute obadmin cmd: {1}".format(ssh_client.get_name(), cmd))
                        # cd the dir of remote_dir and tar remote_dir
                        tar_cmd = "cd {0} && tar -czf {1}.tar.gz {1}/*".format(remote_dir, remote_obadmin_data_dir)
                        ssh_client.exec_cmd(tar_cmd)
                        self.record.add_record("node:{0} tar the dir {1}".format(ssh_client.get_name(), remote_obadmin_data_dir))
                        # download the tar file
                        tar_full_name = "{0}/{1}.tar.gz".format(remote_dir, remote_obadmin_data_dir)
                        self.record.add_record("node:{0} download the tar file tar file: {1}".format(ssh_client.get_name(), tar_full_name))
                        # build the local path
                        self.record.add_record("node:{0} download the tar file to {1}".format(ssh_client.get_name(), os.path.join(self.store_dir, "obadmin_{0}.tar.gz".format(ssh_client.get_name()))))
                        ssh_client.download(tar_full_name, os.path.join(self.store_dir, "obadmin_{0}.tar.gz".format(ssh_client.get_name())))
                        self.record.add_record("node:{0} download the tar file success".format(ssh_client.get_name()))

                except Exception as e:
                    raise RCAExecuteException("node:{0} e:".format(ssh_client.get_name()) + str(e))
                finally:
                    # remove the remote dir
                    ssh_client.exec_cmd("rm -rf {0}".format(remote_dir))
                    self.record.add_record("node:{0} remove the remote dir {1}".format(ssh_client.get_name(), remote_dir))
        except Exception as e:
            raise RCAExecuteException("OSSError : {0}".format(e))
        finally:
            self.record.add_record("all node download obadmin tar file success.you can check the file in {0} or send it to the Oceanbase community.".format(self.store_dir))

    def get_scene_info(self):
        return {
            "name": "oss_error",
            "info_en": "Dependency on obadmin for testing OSS connectivity and permission checks. More info: https://open.oceanbase.com/blog/15444521733",
            "info_cn": "依赖obadmin测试oss连通性及权限检查. 更多信息: https://open.oceanbase.com/blog/15444521733",
        }


oss_error = OSSError()
