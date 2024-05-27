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
@time: 2024/01/23
@file: rca_list.py
@desc:
"""
import os.path
from common.constant import const
from common.tool import DynamicLoading
from common.tool import Util


class RcaScenesListHandler:
    def __init__(self, context, work_path=const.RCA_WORK_PATH):
        self.context = context
        self.stdio = context.stdio

        if not work_path:
            work_path = const.RCA_WORK_PATH
        if os.path.exists(os.path.expanduser(work_path)):
            self.work_path = os.path.expanduser(work_path)
        else:
            self.stdio.warn("input rca work_path not exists: {0}, use default path {1}".format(work_path, const.RCA_WORK_PATH))
            self.work_path = const.RCA_WORK_PATH

    def get_all_scenes(self):
        # find all rca file
        scenes_files = self.__find_rca_files()
        # get all info
        scene_list = {}
        scene_info_list = {}
        if not scenes_files or len(scenes_files) == 0:
            self.stdio.error("no rca scene found! Please check RCA_WORK_PATH: {0}".format(self.work_path))
            return
        for scene_file in scenes_files:
            lib_path = self.work_path
            module_name = os.path.basename(scene_file)[:-9]
            DynamicLoading.add_lib_path(lib_path)
            module = DynamicLoading.import_module(os.path.basename(scene_file)[:-3], None)
            if not hasattr(module, module_name):
                self.stdio.error("{0} import_module failed".format(module_name))
                continue
            scene_list[module_name] = getattr(module, module_name)

        for scene_name, scene in scene_list.items():
            scene_info = scene.get_scene_info()
            scene_info_list[scene_name] = {"name": scene_name, "command": "obdiag rca run --scene={0}".format(scene_name), "info_en": scene_info["info_en"], "info_cn": scene_info["info_cn"]}
        return scene_info_list, scene_list

    def handle(self):
        try:
            self.stdio.verbose("list rca scenes")
            scene_info_list, scene_itme_list = self.get_all_scenes()
            Util.print_scene(scene_info_list)
        except Exception as e:
            self.stdio.error("RcaScenesListHandler Exception: {0}".format(e))
            raise e

    def __find_rca_files(self):
        files = []
        for file_or_folder in os.listdir(self.work_path):
            full_path = os.path.join(self.work_path, file_or_folder)
            if os.path.isfile(full_path):
                if full_path.endswith('_scene.py') and len(os.path.basename(full_path)) > 7:
                    files.append(full_path)
        return files
