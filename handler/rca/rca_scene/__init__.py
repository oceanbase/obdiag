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
@time: 2023/12/22
@file: __init__.py
@desc:
"""
from handler.rca.rca_scene.disconnection_scene import DisconnectionScene
from handler.rca.rca_scene.lock_conflict_scene import LockConflictScene
from handler.rca.rca_scene.major_hold_scene import MajorHoldScene

rca_map = {}
rca_map["major_hold"] = MajorHoldScene()
rca_map["lock_conflict"] = LockConflictScene()
rca_map["disconnection"] = DisconnectionScene()

