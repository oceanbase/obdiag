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
@time: 2025/12/15
@file: config_check_handler.py
@desc: Handler for checking config validity including DB connection and SSH connection
"""

import threading
from src.common.result_type import ObdiagResult
from src.common.tool import Util
from src.common.ob_connector import OBConnector
from src.common.ssh_client.ssh import SshClient
from colorama import Fore, Style


class ConfigCheckHandler:
    """Handler for checking config validity including DB connection and SSH connection"""

    def __init__(self, context):
        self.context = context
        self.stdio = context.stdio
        self.options = context.options
        self.cluster_config = context.cluster_config
        self.obproxy_config = context.obproxy_config

    def handle(self):
        """Main handler method"""
        try:
            self.stdio.print("\n" + Fore.CYAN + "=" * 70 + Style.RESET_ALL)
            self.stdio.print(Fore.CYAN + "  obdiag Configuration Check" + Style.RESET_ALL)
            self.stdio.print(Fore.CYAN + "=" * 70 + Style.RESET_ALL + "\n")

            results = {"db_connection": None, "observer_nodes": [], "obproxy_nodes": [], "summary": {"success": 0, "failed": 0, "skipped": 0}}

            # 1. Check database connection
            self._check_db_connection(results)

            # 2. Check observer nodes SSH connection
            self._check_observer_nodes(results)

            # 3. Check obproxy nodes SSH connection
            self._check_obproxy_nodes(results)

            # Print summary
            self._print_summary(results)

            if results["summary"]["failed"] > 0:
                return ObdiagResult(ObdiagResult.INPUT_ERROR_CODE, error_data="Configuration check found {0} failed items".format(results["summary"]["failed"]), data=results)

            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data=results)

        except Exception as e:
            self.stdio.error("Config check failed: {0}".format(str(e)))
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data=str(e))

    def _check_db_connection(self, results):
        """Check database connection"""
        self.stdio.print(Fore.YELLOW + "[1/3] Checking Database Connection..." + Style.RESET_ALL)
        self.stdio.print("-" * 70)

        if not self.cluster_config:
            self.stdio.print("  " + Fore.YELLOW + "⚠ SKIPPED" + Style.RESET_ALL + " - No cluster configuration found")
            results["db_connection"] = {"status": "skipped", "message": "No cluster configuration"}
            results["summary"]["skipped"] += 1
            self.stdio.print("")
            return

        db_host = self.cluster_config.get("db_host")
        db_port = self.cluster_config.get("db_port")
        tenant_sys = self.cluster_config.get("tenant_sys", {})
        db_user = tenant_sys.get("user")
        db_password = tenant_sys.get("password", "")

        # Check required fields
        missing_fields = []
        if not db_host:
            missing_fields.append("db_host")
        if not db_port:
            missing_fields.append("db_port")
        if not db_user:
            missing_fields.append("tenant_sys.user")

        if missing_fields:
            msg = "Missing required fields: {0}".format(", ".join(missing_fields))
            self.stdio.print("  " + Fore.RED + "✗ FAILED" + Style.RESET_ALL + " - " + msg)
            results["db_connection"] = {"status": "failed", "message": msg}
            results["summary"]["failed"] += 1
            self.stdio.print("")
            return

        self.stdio.print("  Host: {0}".format(db_host))
        self.stdio.print("  Port: {0}".format(db_port))
        self.stdio.print("  User: {0}".format(db_user))
        self.stdio.print("  Password: {0}".format("*" * len(db_password) if db_password else "(empty)"))

        # Try to connect
        try:
            self.stdio.verbose("Attempting to connect to database...")
            ob_connector = OBConnector(context=self.context, ip=db_host, port=int(db_port), username=db_user, password=db_password, timeout=10)

            # Test connection with a simple query
            result = ob_connector.execute_sql("SELECT 1")
            if result is not None:
                self.stdio.print("  " + Fore.GREEN + "✓ SUCCESS" + Style.RESET_ALL + " - Database connection established")

                # Try to get OB version
                try:
                    version_result = ob_connector.execute_sql("SELECT OB_VERSION()")
                    if version_result and len(version_result) > 0:
                        ob_version = version_result[0][0]
                        self.stdio.print("  " + Fore.GREEN + "  OceanBase Version: {0}".format(ob_version) + Style.RESET_ALL)
                except Exception:
                    pass

                results["db_connection"] = {"status": "success", "host": db_host, "port": db_port}
                results["summary"]["success"] += 1
            else:
                raise Exception("Query returned None")

        except Exception as e:
            error_msg = str(e)
            self.stdio.print("  " + Fore.RED + "✗ FAILED" + Style.RESET_ALL + " - Connection failed")
            self.stdio.print("    " + Fore.RED + "Error: {0}".format(error_msg) + Style.RESET_ALL)
            results["db_connection"] = {"status": "failed", "host": db_host, "port": db_port, "error": error_msg}
            results["summary"]["failed"] += 1

        self.stdio.print("")

    def _check_observer_nodes(self, results):
        """Check observer nodes SSH connection"""
        self.stdio.print(Fore.YELLOW + "[2/3] Checking Observer Nodes SSH Connection..." + Style.RESET_ALL)
        self.stdio.print("-" * 70)

        if not self.cluster_config:
            self.stdio.print("  " + Fore.YELLOW + "⚠ SKIPPED" + Style.RESET_ALL + " - No cluster configuration found")
            results["summary"]["skipped"] += 1
            self.stdio.print("")
            return

        # Note: cluster_config.get("servers") returns a list of nodes directly (already merged with global config)
        nodes = self.cluster_config.get("servers", [])
        global_config = {}  # Already merged in config.py

        if not nodes:
            self.stdio.print("  " + Fore.YELLOW + "⚠ SKIPPED" + Style.RESET_ALL + " - No observer nodes configured")
            results["summary"]["skipped"] += 1
            self.stdio.print("")
            return

        self.stdio.print("  Found {0} observer node(s) to check".format(len(nodes)))
        self.stdio.print("")

        # Check each node in parallel
        threads = []
        node_results = []
        lock = threading.Lock()

        for idx, node in enumerate(nodes):
            node_config = self._merge_node_config(node, global_config)
            thread = threading.Thread(target=self._check_single_node, args=(idx + 1, node_config, "observer", node_results, lock, results))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        results["observer_nodes"] = node_results
        self.stdio.print("")

    def _check_obproxy_nodes(self, results):
        """Check obproxy nodes SSH connection"""
        self.stdio.print(Fore.YELLOW + "[3/3] Checking OBProxy Nodes SSH Connection..." + Style.RESET_ALL)
        self.stdio.print("-" * 70)

        if not self.obproxy_config:
            self.stdio.print("  " + Fore.YELLOW + "⚠ SKIPPED" + Style.RESET_ALL + " - No obproxy configuration found")
            results["summary"]["skipped"] += 1
            self.stdio.print("")
            return

        # Note: obproxy_config.get("servers") returns a list of nodes directly (already merged with global config)
        nodes = self.obproxy_config.get("servers", [])
        global_config = {}  # Already merged in config.py

        if not nodes:
            self.stdio.print("  " + Fore.YELLOW + "⚠ SKIPPED" + Style.RESET_ALL + " - No obproxy nodes configured")
            results["summary"]["skipped"] += 1
            self.stdio.print("")
            return

        self.stdio.print("  Found {0} obproxy node(s) to check".format(len(nodes)))
        self.stdio.print("")

        # Check each node in parallel
        threads = []
        node_results = []
        lock = threading.Lock()

        for idx, node in enumerate(nodes):
            node_config = self._merge_node_config(node, global_config)
            thread = threading.Thread(target=self._check_single_node, args=(idx + 1, node_config, "obproxy", node_results, lock, results))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        results["obproxy_nodes"] = node_results
        self.stdio.print("")

    def _merge_node_config(self, node, global_config):
        """Merge node config with global config"""
        merged = dict(global_config)
        merged.update(node)
        return merged

    def _check_single_node(self, idx, node_config, node_type, node_results, lock, results):
        """Check a single node SSH connection"""
        ip = node_config.get("ip", "unknown")
        ssh_username = node_config.get("ssh_username", "")
        ssh_password = node_config.get("ssh_password", "")
        ssh_port = node_config.get("ssh_port", 22)
        ssh_key_file = node_config.get("ssh_key_file", "")
        home_path = node_config.get("home_path", "")
        data_dir = node_config.get("data_dir", "")
        redo_dir = node_config.get("redo_dir", "")

        node_result = {"index": idx, "ip": ip, "ssh_port": ssh_port, "ssh_username": ssh_username, "home_path": home_path, "status": "unknown", "error": None}

        # Check required fields
        missing_fields = []
        using_default_key = False
        if not ip or ip == "unknown":
            missing_fields.append("ip")
        if not ssh_username:
            missing_fields.append("ssh_username")
        # When both ssh_password and ssh_key_file are empty, will try default keys like ~/.ssh/id_rsa
        if not ssh_password and not ssh_key_file:
            using_default_key = True

        if missing_fields:
            msg = "Missing: {0}".format(", ".join(missing_fields))
            node_result["status"] = "failed"
            node_result["error"] = msg
            with lock:
                node_results.append(node_result)
                results["summary"]["failed"] += 1
                self.stdio.print("  [{0}] {1} ({2})".format(idx, ip, node_type))
                self.stdio.print("      " + Fore.RED + "✗ FAILED" + Style.RESET_ALL + " - " + msg)
            return

        # Try SSH connection
        try:
            ssh_client = SshClient(self.context, node_config)

            # Test connection by executing a simple command
            result = ssh_client.exec_cmd("echo 'obdiag_test'")

            if result is not None and "obdiag_test" in result:
                path_check_display = []
                path_errors = []  # Track path check errors

                # For observer nodes, perform additional path checks
                if node_type == "observer":
                    # Check home_path/bin/observer exists
                    if home_path:
                        observer_bin = "{0}/bin/observer".format(home_path)
                        check_observer = ssh_client.exec_cmd("test -f {0} && echo 'yes' || echo 'no'".format(observer_bin))
                        if check_observer and check_observer.strip() == "yes":
                            path_check_display.append(Fore.GREEN + "      ✓ bin/observer found" + Style.RESET_ALL)
                        else:
                            path_check_display.append(Fore.RED + "      ✗ bin/observer not found in {0}".format(home_path) + Style.RESET_ALL)
                            path_errors.append("bin/observer not found")

                    # Check data_dir/sstable exists
                    if data_dir:
                        sstable_path = "{0}/sstable".format(data_dir)
                        check_sstable = ssh_client.exec_cmd("test -d {0} && echo 'yes' || echo 'no'".format(sstable_path))
                        if check_sstable and check_sstable.strip() == "yes":
                            path_check_display.append(Fore.GREEN + "      ✓ data_dir/sstable found" + Style.RESET_ALL)
                        else:
                            path_check_display.append(Fore.RED + "      ✗ sstable not found in {0}".format(data_dir) + Style.RESET_ALL)
                            path_errors.append("sstable not found")

                    # Check redo_dir/clog exists
                    if redo_dir:
                        clog_path = "{0}/clog".format(redo_dir)
                        check_clog = ssh_client.exec_cmd("test -d {0} && echo 'yes' || echo 'no'".format(clog_path))
                        if check_clog and check_clog.strip() == "yes":
                            path_check_display.append(Fore.GREEN + "      ✓ redo_dir/clog found" + Style.RESET_ALL)
                        else:
                            path_check_display.append(Fore.RED + "      ✗ clog not found in {0}".format(redo_dir) + Style.RESET_ALL)
                            path_errors.append("clog not found")
                else:
                    # For obproxy nodes, check home_path/bin/obproxy exists
                    if home_path:
                        obproxy_bin = "{0}/bin/obproxy".format(home_path)
                        check_obproxy = ssh_client.exec_cmd("test -f {0} && echo 'yes' || echo 'no'".format(obproxy_bin))
                        if check_obproxy and check_obproxy.strip() == "yes":
                            path_check_display.append(Fore.GREEN + "      ✓ bin/obproxy found" + Style.RESET_ALL)
                        else:
                            path_check_display.append(Fore.RED + "      ✗ bin/obproxy not found in {0}".format(home_path) + Style.RESET_ALL)
                            path_errors.append("bin/obproxy not found")

                # Determine node status based on path check results
                if path_errors:
                    node_result["status"] = "failed"
                    node_result["error"] = "; ".join(path_errors)
                else:
                    node_result["status"] = "success"

                with lock:
                    node_results.append(node_result)
                    if path_errors:
                        results["summary"]["failed"] += 1
                    else:
                        results["summary"]["success"] += 1

                    self.stdio.print("  [{0}] {1}:{2} ({3})".format(idx, ip, ssh_port, node_type))
                    ssh_msg = Fore.GREEN + "✓" + Style.RESET_ALL + " SSH connection established"
                    if using_default_key:
                        ssh_msg += Fore.YELLOW + " (using default SSH key)" + Style.RESET_ALL
                    self.stdio.print("      " + ssh_msg)
                    for check_msg in path_check_display:
                        self.stdio.print(check_msg)

                    if path_errors:
                        self.stdio.print("      " + Fore.RED + "✗ FAILED - Path check errors found" + Style.RESET_ALL)
            else:
                raise Exception("SSH test command failed")

        except Exception as e:
            error_msg = str(e)
            node_result["status"] = "failed"
            node_result["error"] = error_msg

            with lock:
                node_results.append(node_result)
                results["summary"]["failed"] += 1
                self.stdio.print("  [{0}] {1}:{2} ({3})".format(idx, ip, ssh_port, node_type))
                self.stdio.print("      " + Fore.RED + "✗ FAILED" + Style.RESET_ALL + " - SSH connection failed")
                if using_default_key:
                    self.stdio.print("      " + Fore.YELLOW + "⚠ Warning: No ssh_password or ssh_key_file configured, tried default SSH key (~/.ssh/id_rsa)" + Style.RESET_ALL)
                self.stdio.print("      " + Fore.RED + "  Error: {0}".format(error_msg[:100]) + Style.RESET_ALL)

    def _print_summary(self, results):
        """Print check summary"""
        self.stdio.print(Fore.CYAN + "=" * 70 + Style.RESET_ALL)
        self.stdio.print(Fore.CYAN + "  Configuration Check Summary" + Style.RESET_ALL)
        self.stdio.print(Fore.CYAN + "=" * 70 + Style.RESET_ALL + "\n")

        summary = results["summary"]
        total = summary["success"] + summary["failed"] + summary["skipped"]

        # Database connection
        db_result = results.get("db_connection", {})
        db_status = db_result.get("status", "skipped") if db_result else "skipped"
        if db_status == "success":
            self.stdio.print("  Database Connection:    " + Fore.GREEN + "✓ SUCCESS" + Style.RESET_ALL)
        elif db_status == "failed":
            self.stdio.print("  Database Connection:    " + Fore.RED + "✗ FAILED" + Style.RESET_ALL)
        else:
            self.stdio.print("  Database Connection:    " + Fore.YELLOW + "⚠ SKIPPED" + Style.RESET_ALL)

        # Observer nodes
        observer_nodes = results.get("observer_nodes", [])
        if observer_nodes:
            success_count = len([n for n in observer_nodes if n.get("status") == "success"])
            failed_count = len([n for n in observer_nodes if n.get("status") == "failed"])
            if failed_count > 0:
                self.stdio.print("  Observer Nodes:         " + Fore.RED + "{0}/{1} FAILED".format(failed_count, len(observer_nodes)) + Style.RESET_ALL)
                for node in observer_nodes:
                    if node.get("status") == "failed":
                        self.stdio.print("                          " + Fore.RED + "  - [{0}] {1}: {2}".format(node.get("index"), node.get("ip"), node.get("error", "Unknown error")[:50]) + Style.RESET_ALL)
            else:
                self.stdio.print("  Observer Nodes:         " + Fore.GREEN + "✓ ALL {0} NODES OK".format(len(observer_nodes)) + Style.RESET_ALL)
        else:
            self.stdio.print("  Observer Nodes:         " + Fore.YELLOW + "⚠ SKIPPED (no nodes configured)" + Style.RESET_ALL)

        # OBProxy nodes
        obproxy_nodes = results.get("obproxy_nodes", [])
        if obproxy_nodes:
            success_count = len([n for n in obproxy_nodes if n.get("status") == "success"])
            failed_count = len([n for n in obproxy_nodes if n.get("status") == "failed"])
            if failed_count > 0:
                self.stdio.print("  OBProxy Nodes:          " + Fore.RED + "{0}/{1} FAILED".format(failed_count, len(obproxy_nodes)) + Style.RESET_ALL)
                for node in obproxy_nodes:
                    if node.get("status") == "failed":
                        self.stdio.print("                          " + Fore.RED + "  - [{0}] {1}: {2}".format(node.get("index"), node.get("ip"), node.get("error", "Unknown error")[:50]) + Style.RESET_ALL)
            else:
                self.stdio.print("  OBProxy Nodes:          " + Fore.GREEN + "✓ ALL {0} NODES OK".format(len(obproxy_nodes)) + Style.RESET_ALL)
        else:
            self.stdio.print("  OBProxy Nodes:          " + Fore.YELLOW + "⚠ SKIPPED (no nodes configured)" + Style.RESET_ALL)

        self.stdio.print("")
        self.stdio.print("-" * 70)

        if summary["failed"] > 0:
            self.stdio.print("  Result: " + Fore.RED + "FAILED" + Style.RESET_ALL + " ({0} success, {1} failed, {2} skipped)".format(summary["success"], summary["failed"], summary["skipped"]))
            self.stdio.print("")
            self.stdio.print(Fore.YELLOW + "  Please check your configuration file (~/.obdiag/config.yml) and fix the errors above." + Style.RESET_ALL)
        else:
            self.stdio.print("  Result: " + Fore.GREEN + "ALL CHECKS PASSED" + Style.RESET_ALL + " ({0} success, {1} skipped)".format(summary["success"], summary["skipped"]))

        self.stdio.print("-" * 70 + "\n")
