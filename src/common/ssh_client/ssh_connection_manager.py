#!/usr/bin/env python
# -*- coding: UTF-8 -*-
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
@file: ssh_connection_manager.py
@desc: SSH connection pool for check tasks. Multi-task shared connections per node.
"""
import queue
import threading

from src.common.ssh_client.ssh import SshClient


def _node_key(node):
    """Generate unique key for a node. Same key => same connection pool."""
    return (
        node.get("ip", ""),
        node.get("ssh_username", ""),
        node.get("ssh_port", 22),
    )


class SSHConnectionManager:
    """
    Connection pool for SSH clients. Multiple tasks share connections per node.

    Config:
        max_connections_per_node: Max connections per (ip, user, port)
        idle_timeout: Reserved for future use (connection eviction)
    """

    def __init__(self, context, max_connections_per_node=5, idle_timeout=300):
        """
        Args:
            context: HandlerContext
            max_connections_per_node: Max pooled connections per node
            idle_timeout: Seconds before idle connection can be evicted (reserved)
        """
        self.context = context
        self.stdio = context.stdio if context else None
        self.max_per_node = max(1, max_connections_per_node)
        self.idle_timeout = idle_timeout

        # Per-node: { node_key: {"queue": Queue, "lock": Lock, "count": int } }
        self._pools = {}
        self._pools_lock = threading.Lock()

    def get_connection(self, node):
        """
        Get an SSH connection for the node. Blocks if all connections in use.

        Args:
            node: Node config dict (ip, ssh_username, ssh_port, etc.)

        Returns:
            SshClient or None if connection failed
        """
        key = _node_key(node)
        if not key[0]:
            return None

        with self._pools_lock:
            if key not in self._pools:
                self._pools[key] = {
                    "queue": queue.Queue(),
                    "lock": threading.Lock(),
                    "count": 0,
                }
            pool = self._pools[key]

        # Try to get from pool without blocking
        try:
            ssher = pool["queue"].get_nowait()
            return ssher
        except queue.Empty:
            pass

        # Need to create new or wait
        with pool["lock"]:
            if pool["count"] < self.max_per_node:
                try:
                    ssher = SshClient(self.context, node)
                    pool["count"] += 1
                    if self.stdio:
                        self.stdio.verbose(
                            "SSH connection created for {0} (pool {1}/{2})".format(
                                key[0], pool["count"], self.max_per_node
                            )
                        )
                    return ssher
                except Exception as e:
                    if self.stdio:
                        self.stdio.warn(
                            "SSH connection failed for {0}: {1}".format(key[0], e)
                        )
                    return None

        # Wait for released connection
        try:
            ssher = pool["queue"].get(timeout=60)
            return ssher
        except queue.Empty:
            if self.stdio:
                self.stdio.warn(
                    "SSH connection timeout waiting for {0}".format(key[0])
                )
            return None

    def release_connection(self, ssher):
        """
        Return SSH connection to the pool. Does not close the connection.

        Args:
            ssher: SshClient instance from get_connection
        """
        if ssher is None:
            return
        node = getattr(ssher, "node", None)
        if node is None:
            return
        key = _node_key(node)
        if key not in self._pools:
            return
        pool = self._pools[key]
        pool["queue"].put_nowait(ssher)

    def _close_ssh(self, ssher):
        """Close SSH connection. Used when pool is full on release."""
        try:
            if ssher and hasattr(ssher, "client") and ssher.client:
                if hasattr(ssher.client, "_ssh_fd") and ssher.client._ssh_fd:
                    ssher.client._ssh_fd.close()
        except Exception as e:
            if self.stdio:
                self.stdio.verbose("Failed to close SSH: {0}".format(e))

    def close_all(self):
        """Close all pooled connections. Call when check execution finishes."""
        with self._pools_lock:
            for key, pool in list(self._pools.items()):
                while True:
                    try:
                        ssher = pool["queue"].get_nowait()
                        self._close_ssh(ssher)
                    except queue.Empty:
                        break
                pool["count"] = 0
            self._pools.clear()
        if self.stdio:
            self.stdio.verbose("SSHConnectionManager: all connections closed")
