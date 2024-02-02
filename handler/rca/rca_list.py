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
from common.logger import logger
from dataclasses import dataclass
from utils.print_utils import print_scene, print_title

@dataclass
class RegisteredScene:
    name: str
    command: str
    info_en: str
    info_cn: str


scene_list = [
    RegisteredScene(
        'major_hold',
        'obdiag rca run --scene=major_hold',
        '[root cause analysis of major hold]',
        '[针对卡合并场景的根因分析]'
    ),
    RegisteredScene(
        'disconnection',
        'obdiag rca run --scene=disconnection',
        '[root cause analysis of disconnection]',
        '[针对断链接场景的根因分析]'
    ),
    RegisteredScene('lock_conflict', 'obdiag rca run --scene=lock_conflict', '[root cause analysis of lock conflict]', '[针对锁冲突的根因分析]'),
]


class RcaScenesListHandler:
    def handle(self, args):
        logger.debug("list rca scenes")
        scenes_map = self.__get_scenes()
        self.__print_scenes_data(scenes_map)

    def __print_scenes_data(self,scenes):
        print_title("Rca Scenes")
        print_scene(scenes)

    def __get_scenes(self):
        scenes_map = {}
        for scene in scene_list:
            scenes_map[scene.name]={"name": scene.name, "command": scene.command, "info_en": scene.info_en, "info_cn": scene.info_cn}
        return scenes_map