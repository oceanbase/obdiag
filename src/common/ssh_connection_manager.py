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
@time: 2026/02/03
@file: ssh_connection_manager.py
@desc: SSH connection manager using connection pool
"""

from typing import Dict, List, Optional
from src.common.ssh_connection_pool import SSHConnectionPool
from src.common.context import HandlerContext
from src.common.ssh_client.ssh import SshClient


class SSHConnectionManager:
    """
    Manager for SSH connections using connection pool.

    Provides unified SSH connection management with connection reuse.
    """

    def __init__(self, max_connections_per_node: int = 5, idle_timeout: int = 300):
        """
        Initialize SSH connection manager.

        Args:
            max_connections_per_node: Maximum connections per node
            idle_timeout: Idle timeout in seconds
        """
        self.pool = SSHConnectionPool(max_connections_per_node=max_connections_per_node, idle_timeout=idle_timeout)

    def get_connection(self, context: HandlerContext, node: Dict) -> SshClient:
        """
        Get SSH connection from pool.

        Args:
            context: Handler context
            node: Node configuration dictionary

        Returns:
            SSH client instance
        """
        return self.pool.get_connection(context, node)

    def return_connection(self, client: SshClient):
        """
        Return connection to pool.

        Args:
            client: SSH client instance
        """
        self.pool.return_connection(client)

    def setup_nodes_with_connections(self, context: HandlerContext, nodes: List[Dict], node_type: str = "observer") -> List[Dict]:
        """
        Setup nodes with SSH connections attached.
        
        If some connections fail to create, keep the successfully created ones.
        This is important because some hosts may have limited connection capacity.

        Args:
            context: Handler context
            nodes: List of node configuration dictionaries
            node_type: Type of nodes (observer, obproxy, oms)

        Returns:
            List of nodes with 'ssher' attached (only successfully connected nodes)
        """
        nodes_with_connections = []
        success_count = 0
        failure_count = 0

        for node in nodes:
            try:
                ssh_client = self.get_connection(context, node)
                node["ssher"] = ssh_client
                nodes_with_connections.append(node)
                success_count += 1
            except Exception as e:
                failure_count += 1
                node_ip = node.get('ip', 'unknown')
                if context.stdio:
                    context.stdio.warn(
                        f"Failed to create SSH connection for {node_type} node {node_ip}: {e}. "
                        f"Continuing with available connections."
                    )

        # Log summary
        if context.stdio:
            total_nodes = len(nodes)
            if success_count > 0:
                if failure_count > 0:
                    context.stdio.warn(
                        f"SSH connection setup: {success_count}/{total_nodes} {node_type} nodes connected "
                        f"({failure_count} failed). Will use available connections."
                    )
                else:
                    context.stdio.verbose(f"SSH connection setup: all {success_count} {node_type} nodes connected")
            else:
                context.stdio.warn(
                    f"SSH connection setup: all {total_nodes} {node_type} nodes failed. "
                    f"Connections will be created on-demand."
                )

        # Store in context (only successfully connected nodes)
        context.set_variable(f"{node_type}_nodes", nodes_with_connections)

        return nodes_with_connections

    def cleanup(self):
        """Clean up idle connections"""
        self.pool.cleanup_idle_connections()

    def close_all(self):
        """Close all connections"""
        self.pool.close_all()

    def get_stats(self) -> Dict:
        """Get connection pool statistics"""
        return self.pool.get_stats()
