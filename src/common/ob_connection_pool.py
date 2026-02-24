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
@time: 2026/02/09
@file: ob_connection_pool.py
@desc: Global OceanBase database connection pool
"""

import queue
import threading
from contextlib import contextmanager
from typing import Optional, Dict
from src.common.ob_connector import OBConnector
from src.common.obdiag_exception import OBDIAGDBConnException


class OBConnectionPool:
    """
    Global OceanBase database connection pool.

    This pool manages database connections to avoid creating new connections
    for each operation, improving performance and reducing overhead.
    """

    def __init__(self, context, cluster_config: Dict, max_size: int = 10, timeout: int = 30, health_check_interval: int = 60):
        """
        Initialize OB connection pool.

        Args:
            context: Handler context
            cluster_config: Cluster configuration dictionary
            max_size: Maximum pool size (default: 10)
            timeout: Connection timeout in seconds (default: 30)
            health_check_interval: Health check interval in seconds (default: 60)
        """
        self.context = context
        self.cluster_config = cluster_config
        self.max_size = max_size
        self.timeout = timeout
        self.health_check_interval = health_check_interval
        self._pool = queue.Queue(maxsize=max_size)
        self._lock = threading.Lock()
        self.stdio = context.stdio if context else None

        # Initialize pool with connections
        self._initialize_pool()

    def _initialize_pool(self):
        """
        Initialize pool with initial connections.

        If some connections fail to create, keep the successfully created ones.
        This is important because some hosts may have limited connection capacity.
        """
        try:
            tenant_sys = self.cluster_config.get("tenant_sys", {})
            if not tenant_sys.get("user"):
                if self.stdio:
                    self.stdio.warn("Sys tenant credentials not configured, pool initialization skipped")
                return

            success_count = 0
            failure_count = 0

            for i in range(self.max_size):
                try:
                    conn = self._create_connection()
                    if conn:
                        self._pool.put_nowait(conn)
                        success_count += 1
                    else:
                        failure_count += 1
                        if self.stdio:
                            self.stdio.verbose(f"Failed to create connection {i+1}/{self.max_size}, continuing with available connections")
                except Exception as e:
                    failure_count += 1
                    if self.stdio:
                        self.stdio.warn(f"Exception creating connection {i+1}/{self.max_size}: {e}, continuing with available connections")

            # Log initialization result
            if self.stdio:
                if success_count > 0:
                    if failure_count > 0:
                        self.stdio.warn(f"OBConnectionPool initialized with {success_count}/{self.max_size} connections " f"({failure_count} failed). Pool will use available connections.")
                    else:
                        self.stdio.verbose(f"OBConnectionPool initialized with {success_count} connections")
                else:
                    self.stdio.warn(f"OBConnectionPool initialization: all {self.max_size} connections failed. " f"Pool will create connections on-demand.")
        except Exception as e:
            if self.stdio:
                self.stdio.error(f"OBConnectionPool initialization error: {e}")
                # Even if initialization fails, keep any connections that were successfully created
                if self._pool.qsize() > 0:
                    self.stdio.verbose(f"Keeping {self._pool.qsize()} successfully created connections despite initialization error")

    def _create_connection(self) -> Optional[OBConnector]:
        """Create a new database connection."""
        try:
            tenant_sys = self.cluster_config.get("tenant_sys", {})
            return OBConnector(
                context=self.context,
                ip=self.cluster_config.get("db_host"),
                port=self.cluster_config.get("db_port"),
                username=tenant_sys.get("user"),
                password=tenant_sys.get("password"),
                timeout=self.timeout * 1000,  # Convert to milliseconds
            )
        except Exception as e:
            if self.stdio:
                self.stdio.error(f"Failed to create connection: {e}")
            return None

    def _health_check(self, conn: OBConnector) -> bool:
        """
        Check if connection is still alive.

        Args:
            conn: OBConnector instance

        Returns:
            True if connection is alive, False otherwise
        """
        try:
            if conn is None:
                return False
            # Try to execute a simple query
            conn.execute_sql("SELECT 1")
            return True
        except Exception:
            return False

    def get_connection(self, timeout: Optional[int] = None) -> OBConnector:
        """
        Get a connection from the pool with timeout and health check.

        Args:
            timeout: Timeout in seconds (default: self.timeout)

        Returns:
            OBConnector instance

        Raises:
            OBDIAGDBConnException: If pool is exhausted or connection fails
        """
        timeout = timeout or self.timeout

        try:
            conn = self._pool.get(timeout=timeout)

            # Health check: verify connection is still alive
            if not self._health_check(conn):
                # Connection is dead, create a new one
                if self.stdio:
                    self.stdio.verbose("Connection health check failed, creating new connection")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = self._create_connection()
                if conn is None:
                    raise OBDIAGDBConnException("Failed to create new connection after health check failure")

            return conn
        except queue.Empty:
            raise OBDIAGDBConnException(f"Connection pool exhausted (max={self.max_size}, timeout={timeout}s)")
        except Exception as e:
            raise OBDIAGDBConnException(f"Failed to get connection: {e}")

    def release(self, conn: OBConnector):
        """
        Release a connection back to the pool safely.

        Args:
            conn: OBConnector instance to release
        """
        if conn is None:
            return

        try:
            # Health check before returning to pool
            if self._health_check(conn):
                try:
                    self._pool.put_nowait(conn)
                except queue.Full:
                    # Pool is full, close the connection
                    self._safe_close(conn)
            else:
                # Connection is dead, try to replace it
                self._try_replace(conn)
        except Exception as e:
            if self.stdio:
                self.stdio.warn(f"Error releasing connection: {e}")
            self._safe_close(conn)

    def _try_replace(self, conn: OBConnector):
        """Try to replace a dead connection with a new one."""
        self._safe_close(conn)
        new_conn = self._create_connection()
        if new_conn:
            try:
                self._pool.put_nowait(new_conn)
            except queue.Full:
                # Pool is full, close the new connection
                self._safe_close(new_conn)

    def _safe_close(self, conn: OBConnector):
        """Safely close a connection."""
        try:
            if conn and hasattr(conn, 'close'):
                conn.close()
        except Exception:
            pass

    @contextmanager
    def connection(self):
        """
        Context manager for database connection.

        Usage:
            with pool.connection() as conn:
                result = conn.execute_sql("SELECT 1")

        Yields:
            OBConnector instance
        """
        conn = self.get_connection()
        try:
            yield conn
        finally:
            self.release(conn)

    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            while not self._pool.empty():
                try:
                    conn = self._pool.get_nowait()
                    self._safe_close(conn)
                except queue.Empty:
                    break

    def get_stats(self) -> Dict:
        """
        Get pool statistics.

        Returns:
            Dictionary with pool statistics including:
            - total_connections: Total connections in pool
            - max_size: Maximum pool size
            - available_connections: Available connections
            - utilization_rate: Pool utilization rate (0.0-1.0)
        """
        with self._lock:
            total = self._pool.qsize()
            max_size = self.max_size
            utilization_rate = total / max_size if max_size > 0 else 0.0

            return {"total_connections": total, "max_size": max_size, "available_connections": total, "utilization_rate": utilization_rate, "timeout": self.timeout, "health_check_interval": self.health_check_interval}
