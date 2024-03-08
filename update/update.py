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
@time: 2024/2/1
@file: update.py
@desc:
"""
import os
import shutil
import time
from common.constant import const
from common.logger import logger
from utils.file_utils import calculate_sha256, extract_tar
from utils.network_utils import download_file, network_connectivity
from utils.version_utils import OBDIAG_VERSION, compare_versions_greater
import yaml


# for update obdiag files without obdiag
class UpdateHandler:
    def __init__(self):
        self.local_update_file_sha = ""
        self.local_obdiag_version = OBDIAG_VERSION
        self.remote_obdiag_version = ""
        self.remote_tar_sha = ""

    def execute(self, file_path, force=False):
        try:
            remote_server = const.UPDATE_REMOTE_SERVER
            remote_version_file_name = const.UPDATE_REMOTE_VERSION_FILE_NAME
            local_version_file_name = os.path.expanduser('~/.obdiag/remote_version.yaml')
            remote_update_file_name = const.UPDATE_REMOTE_UPDATE_FILE_NAME
            local_update_file_name = os.path.expanduser('~/.obdiag/data.tar')
            local_update_log_file_name = os.path.expanduser('~/.obdiag/data_version.yaml')
            if file_path and file_path != "":
                self.handle_update_offline(file_path)
                return
            if network_connectivity(remote_server) is False:
                logger.warning("[update] network connectivity failed. Please check your network connection.")
                return
            download_file(remote_version_file_name, os.path.expanduser(local_version_file_name))
            with open(local_version_file_name, 'r') as file:
                remote_data = yaml.safe_load(file)
            if remote_data.get("obdiag_version") is None:
                logger.warning("obdiag_version is None. Do not perform the upgrade process.")
                return
            else:
                self.remote_obdiag_version = remote_data["obdiag_version"].strip()
            if compare_versions_greater(self.remote_obdiag_version, self.local_obdiag_version):
                logger.warning(
                    "remote_obdiag_version is {0}. local_obdiag_version is {1}. "
                    "remote_obdiag_version>local_obdiag_version. Unable to update dependency files, please upgrade "
                    "obdiag. Do not perform the upgrade process.".format(
                        self.remote_obdiag_version, self.local_obdiag_version))
                return
            if remote_data.get("remote_tar_sha") is None:
                logger.warning("remote_tar_sha is None. Do not perform the upgrade process.")
                return
            else:
                self.remote_tar_sha = remote_data["remote_tar_sha"]
            # need update?
            # get local sha
            if force is False:
                if os.path.exists(os.path.expanduser(local_update_log_file_name)):
                    with open(os.path.expanduser(local_update_log_file_name), 'r') as file:
                        local_data = yaml.safe_load(file)
                    if local_data.get("remote_tar_sha") is not None and local_data.get(
                            "remote_tar_sha") == self.remote_tar_sha:
                        logger.info("[update] remote_tar_sha as local_tar_sha. No need to update.")
                        return
                    # get data_update_time
                    if local_data.get("data_update_time") is not None and time.time() - local_data[
                        "data_update_time"] < 3600 * 24 * 7:
                        logger.info("[update] data_update_time No need to update.")
                        return
            # download_update_files
            download_file(remote_update_file_name, local_update_file_name)
            # check_sha
            self.local_update_file_sha = calculate_sha256(local_update_file_name)
            if self.remote_tar_sha != self.local_update_file_sha:
                logger.warning(
                    "remote_tar_sha is {0}, but local_tar_sha is {1}. Unable to update dependency files. Do not perform the upgrade process.".format(
                        self.remote_tar_sha, self.local_update_file_sha))
                return
            # move old files
            ## check_old_files
            if os.path.exists(os.path.expanduser("~/.obdiag/check.d")):
                shutil.rmtree(os.path.expanduser("~/.obdiag/check.d"))
            if os.path.exists(os.path.expanduser("~/.obdiag/check")):
                os.rename(os.path.expanduser("~/.obdiag/check"), os.path.expanduser("~/.obdiag/check.d"))
            ## gather
            if os.path.exists(os.path.expanduser("~/.obdiag/gather.d")):
                shutil.rmtree(os.path.expanduser("~/.obdiag/gather.d"))
            if os.path.exists(os.path.expanduser("~/.obdiag/gather")):
                os.rename(os.path.expanduser("~/.obdiag/gather"), os.path.expanduser("~/.obdiag/gather.d"))
            # decompression remote files
            extract_tar(os.path.expanduser(local_update_file_name), os.path.expanduser("~/.obdiag"))
            # update data save
            with open(os.path.expanduser("~/.obdiag/data_version.yaml"), 'w') as f:
                yaml.dump({"data_update_time": int(time.time()), "remote_tar_sha": self.remote_tar_sha}, f)
            logger.info("[update] Successfully updated. The original data is stored in the *. d folder.")
        except Exception as e:
            logger.warning('[update] Failed to update. Error message: {0}'.format(e))

    def handle_update_offline(self, file):
        file = os.path.expanduser(file)

        self.local_update_file_sha = calculate_sha256(file)
        if os.path.exists(file) is False:
            logger.error('{0} does not exist.'.format(file))
            return
        ## check_old_files
        if os.path.exists(os.path.expanduser("~/.obdiag/check.d")):
            shutil.rmtree(os.path.expanduser("~/.obdiag/check.d"))
        if os.path.exists(os.path.expanduser("~/.obdiag/check")):
            os.rename(os.path.expanduser("~/.obdiag/check"), os.path.expanduser("~/.obdiag/check.d"))
        ## gather
        if os.path.exists(os.path.expanduser("~/.obdiag/gather.d")):
            shutil.rmtree(os.path.expanduser("~/.obdiag/gather.d"))
        if os.path.exists(os.path.expanduser("~/.obdiag/gather")):
            os.rename(os.path.expanduser("~/.obdiag/gather"), os.path.expanduser("~/.obdiag/gather.d"))
        # decompression remote files
        extract_tar(os.path.expanduser(file), os.path.expanduser("~/.obdiag"))
        # update data save
        with open(os.path.expanduser("~/.obdiag/data_version.yaml"), 'w') as f:
            yaml.dump({"data_update_time": int(time.time()), "remote_tar_sha": self.remote_tar_sha}, f)
        logger.info("[update] Successfully updated. The original data is stored in the *. d folder.")
