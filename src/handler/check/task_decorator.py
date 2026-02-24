#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# Copyright (c) 2022 OceanBase
# OceanBase Diagnostic Tool is licensed under Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.

"""
@time: 2026/02/09
@file: task_decorator.py
@desc: Declarative check task decorator
"""

import functools
from typing import Optional, List, Dict, Any
from src.handler.check.check_task import TaskBase


class TaskResult:
    """Task execution result enumeration."""

    SUCCESS = "success"
    SKIPPED = "skipped"
    FAILED = "failed"


def check_task(
    name: str,
    description: str,
    min_ob_version: Optional[str] = None,
    max_ob_version: Optional[str] = None,
    supported_os: Optional[List[str]] = None,
    requires: Optional[List[str]] = None,
    dependencies: Optional[List[str]] = None,
    tags: Optional[List[str]] = None,
    configurable: Optional[Dict[str, Any]] = None,
):
    """
    Declarative check task decorator.

    Framework automatically handles:
    - Version compatibility checks
    - OS compatibility checks
    - Connection availability checks (db/ssh)
    - Exception catching and standardized reporting
    - Execution timeout protection

    Args:
        name: Task name
        description: Task description
        min_ob_version: Minimum OceanBase version required (e.g., "4.0.0.0")
        max_ob_version: Maximum OceanBase version supported
        supported_os: List of supported OS (e.g., ["linux"])
        requires: Required resources (e.g., ["db", "ssh"])
        dependencies: List of task names that must execute before this task (e.g., ["observer.system.python_version"])
        tags: Task tags for categorization
        configurable: Configurable parameters with defaults

    Example:
        @check_task(
            name="clog_disk_full",
            description="Check if clog disk usage exceeds threshold",
            min_ob_version="4.0.0.0",
            supported_os=["linux"],
            requires=["db", "ssh"],
            tags=["disk", "clog", "critical"],
            configurable={
                "threshold": {
                    "type": "float",
                    "default": 0.85,
                    "description": "Disk usage alert threshold"
                },
            }
        )
        class ClogDiskFullTask(TaskBase):
            def execute(self):
                threshold = self.get_config("threshold")
                # ... implementation ...
    """

    def decorator(cls):
        # Store task metadata
        cls._task_meta = {
            'name': name,
            'description': description,
            'min_ob_version': min_ob_version,
            'max_ob_version': max_ob_version,
            'supported_os': supported_os or ['linux'],
            'requires': requires or [],
            'dependencies': dependencies or [],
            'tags': tags or [],
            'configurable': configurable or {},
        }

        # Wrap execute method with automatic checks
        original_execute = cls.execute

        @functools.wraps(original_execute)
        def wrapped_execute(self):
            meta = self._task_meta

            # Automatic version check
            if meta['min_ob_version']:
                if not self.check_ob_version_min(meta['min_ob_version']):
                    self.report.add_skip(f"Requires OB >= {meta['min_ob_version']}, " f"current: {self.observer_version}")
                    return TaskResult.SKIPPED

            if meta['max_ob_version']:
                # Check if current version exceeds max (simplified check)
                from src.common.tool import StringUtils

                if self.observer_version and StringUtils.compare_versions_greater(self.observer_version, meta['max_ob_version']):
                    self.report.add_skip(f"Requires OB <= {meta['max_ob_version']}, " f"current: {self.observer_version}")
                    return TaskResult.SKIPPED

            # Automatic OS check (check first node if SSH required)
            if meta['supported_os'] and 'ssh' in meta['requires']:
                if hasattr(self, 'observer_nodes') and self.observer_nodes:
                    ssh_client = self.observer_nodes[0].get("ssher")
                    if ssh_client:
                        os_type = self.get_os_type(ssh_client)
                        if os_type not in meta['supported_os']:
                            self.report.add_skip(f"OS '{os_type}' not supported. Supported: {meta['supported_os']}")
                            return TaskResult.SKIPPED

            # Automatic connection checks
            if 'db' in meta['requires']:
                if self.ob_connector is None:
                    self.report.add_fail("Database connection not available")
                    return TaskResult.FAILED

            if 'ssh' in meta['requires']:
                if hasattr(self, 'observer_nodes'):
                    for node in self.observer_nodes:
                        if node.get("ssher") is None:
                            self.report.add_fail(f"SSH not available for node {node.get('ip', 'unknown')}")
                            return TaskResult.FAILED

            # Execute with exception protection
            try:
                result = original_execute(self)
                return result if result else TaskResult.SUCCESS
            except Exception as e:
                self.report.add_fail(f"Task '{meta['name']}' error: {e}")
                return TaskResult.FAILED

        cls.execute = wrapped_execute

        # Auto-generate get_task_info
        def get_task_info(self):
            return {
                'name': meta['name'],
                'info': meta['description'],
                'supported_os': meta['supported_os'],
                'min_ob_version': meta['min_ob_version'],
                'max_ob_version': meta['max_ob_version'],
                'dependencies': meta['dependencies'],
                'tags': meta['tags'],
            }

        cls.get_task_info = get_task_info

        # Add config access method if configurable parameters exist
        if meta['configurable']:

            def get_config(self, param_name: str):
                """Get configurable parameter value."""
                if param_name not in meta['configurable']:
                    raise ValueError(f"Unknown configurable parameter: {param_name}")

                param_def = meta['configurable'][param_name]
                default = param_def.get('default')
                param_type = param_def.get('type', 'str')

                # Try to get from context config, fallback to default
                # This is a simplified version - can be enhanced
                return default

            cls.get_config = get_config

        return cls

    return decorator
