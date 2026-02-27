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
@time: 2023/01/12
@file: gather_perf.py
@desc: Gather perf data and optionally generate flame graph SVG (see github.com/oceanbase/obdiag/issues/95).
"""
import os
import shutil
import subprocess
import sys
import tarfile
import time
import datetime

import tabulate

from src.common.base_handler import BaseHandler
from src.common.command import get_observer_pid, mkdir, zip_dir, get_file_size, download_file, delete_file_force, is_empty_dir, is_empty_file
from src.common.ssh_client.ssh import SshClient
from src.common.constant import const
from src.common.tool import Util
from src.common.tool import DirectoryUtil
from src.common.tool import FileUtil
from src.common.tool import NetUtils
from src.common.tool import TimeUtils
from src.common.result_type import ObdiagResult


class GatherPerfHandler(BaseHandler):
    def _init(self, gather_pack_dir='./', is_scene=False, **kwargs):
        """Subclass initialization"""
        self.is_ssh = True
        self.local_stored_path = gather_pack_dir
        self.remote_stored_path = None
        self.ob_install_dir = None
        self.is_scene = is_scene
        self.scope = "all"
        self.config_path = const.DEFAULT_CONFIG_PATH

        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

        # Initialize config
        self.nodes = self.context.cluster_config['servers']
        new_nodes = Util.get_nodes_list(self.context, self.nodes, self.stdio)
        if new_nodes:
            self.nodes = new_nodes

        # Use ConfigAccessor if available (file_number_limit is from BaseHandler @property)
        if self.config:
            self.file_size_limit = self.config.gather_file_size_limit
            self.config_path = self.config.basic_config_path
        else:
            # Fallback to direct config access
            if self.context.inner_config is None:
                self.file_size_limit = 2 * 1024 * 1024 * 1024
            else:
                basic_config = self.context.inner_config['obdiag']['basic']
                self.file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
                self.config_path = basic_config['config_path']

        # Initialize options
        count_option = self._get_option('count')
        if self.context.get_variable("gather_perf_sample_count", None):
            count_option = self.context.get_variable("gather_perf_sample_count")
        if isinstance(count_option, str):
            try:
                count_option = int(count_option)
            except ValueError:
                try:
                    count_option = float(count_option)
                except ValueError:
                    count_option = None
        if isinstance(count_option, (int, float)) and count_option >= 1000000:
            count_option = count_option
        else:
            count_option = 100000000
        self.count_option = count_option

        # Use BaseHandler template method for store directory initialization
        self.local_stored_path = self._init_store_dir(default='./')

        scope_option = self._get_option('scope')
        if scope_option:
            self.scope = scope_option

    def handle(self) -> ObdiagResult:
        """Main handle logic"""
        self._validate_initialized()

        try:
            if self.is_scene:
                pack_dir_this_command = self.local_stored_path
            else:
                pack_dir_this_command = os.path.join(self.local_stored_path, f"obdiag_gather_{TimeUtils.timestamp_to_filename_time(self.gather_timestamp)}")
            self._log_verbose(f"Use {pack_dir_this_command} as pack dir.")
            gather_tuples = []

            def handle_from_node(node):
                st = time.time()
                resp = self.__handle_from_node(node, pack_dir_this_command)
                file_size = ""
                # Determine if this is an error: skip or not successful
                is_err = resp.get("skip", False) or not resp.get("success", False)

                # Build error message: include warnings if present
                error_msg = resp.get("error", "")
                if resp.get("warnings") and error_msg:
                    # If both warnings and errors exist, combine them
                    error_msg = error_msg + "; Warnings: " + "; ".join(resp["warnings"])
                elif resp.get("warnings") and not error_msg:
                    # If only warnings exist (partial success), show them as warnings in status
                    error_msg = "Partial success: " + "; ".join(resp["warnings"])

                if resp.get("success") and resp.get("gather_pack_path") and os.path.exists(resp["gather_pack_path"]):
                    try:
                        file_size = os.path.getsize(resp["gather_pack_path"])
                    except Exception as e:
                        self._log_warn(f"Failed to get file size for {resp.get('gather_pack_path', '')}: {e}")
                        file_size = ""
                gather_tuples.append((node.get("ip"), is_err, error_msg, file_size, int(time.time() - st), resp.get("gather_pack_path", "")))

            exec_tag = False
            if self.is_ssh:
                for node in self.nodes:
                    if node.get("ssh_type") == "docker" or node.get("ssh_type") == "kubernetes":
                        self._log_warn(f"Skip gather from node {node.get('ip')} because it is a docker or kubernetes node")
                        continue
                    handle_from_node(node)
                    exec_tag = True
            else:
                local_ip = NetUtils.get_inner_ip(self.stdio)
                node = self.nodes[0]
                node["ip"] = local_ip
                handle_from_node(node)
                exec_tag = True

            if not exec_tag:
                self._log_verbose("No node to gather from, skip")
                return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

            summary_tuples = self.__get_overall_summary(gather_tuples)
            # Note: summary_tuples is now a string from _generate_summary_table
            self._log_info(summary_tuples)
            # Persist the summary results to a file
            FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)
            last_info = f"For result details, please run cmd \033[32m' cat {os.path.join(pack_dir_this_command, 'result_summary.txt')} '\033[0m\n"
            self._log_info(last_info)
            return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"store_dir": pack_dir_this_command})

        except Exception as e:
            return self._handle_error(e)

    def __handle_from_node(self, node, local_stored_path):
        """
        Handle perf data gathering from a single node.

        Returns:
            dict: Response dictionary with keys:
                - success: bool, True if data collection succeeded (even with warnings)
                - skip: bool, True if node was skipped
                - error: str, Error message if failed, empty string if succeeded
                - warnings: list, List of warning messages (partial failures)
                - gather_pack_path: str, Path to the collected data package
        """
        resp = {"success": False, "skip": False, "error": "", "warnings": [], "gather_pack_path": ""}
        remote_ip = node.get("ip") if self.is_ssh else NetUtils.get_inner_ip(self.stdio)
        remote_user = node.get("ssh_username")
        self._log_verbose(f"Sending Collect Shell Command to node {remote_ip} ...")
        DirectoryUtil.mkdir(path=local_stored_path, stdio=self.stdio)
        now_time = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        remote_dir_name = f"perf_{node.get('ip').replace(':', '_')}_{now_time}"
        remote_dir_full_path = f"/tmp/{remote_dir_name}"
        ssh_client = None

        try:
            ssh_client = SshClient(self.context, node)
        except Exception as e:
            self.stdio.exception(f"ssh {remote_user}@{remote_ip}: failed, Please check the node conf.")
            resp["skip"] = True
            resp["success"] = False
            resp["error"] = f"SSH connection failed: {str(e)}"
            return resp

        try:
            mkdir(ssh_client, remote_dir_full_path, self.stdio)
            pid_observer_list = get_observer_pid(ssh_client, node.get("home_path"), self.stdio)
            if len(pid_observer_list) == 0:
                resp["success"] = False
                resp["error"] = "can't find observer"
                # Clean up remote directory
                try:
                    ssh_client.exec_cmd("rm -rf {0}".format(remote_dir_full_path))
                except Exception as e:
                    self.stdio.verbose("Failed to clean up remote directory {0}: {1}".format(remote_dir_full_path, e))
                return resp

            gather_errors = []
            for pid_observer in pid_observer_list:
                if self.__perf_checker(ssh_client):
                    if self.scope == "sample":
                        if not self.__gather_perf_sample(ssh_client, remote_dir_full_path, pid_observer):
                            gather_errors.append("Failed to gather perf sample for PID {0}".format(pid_observer))
                    elif self.scope == "flame":
                        if not self.__gather_perf_flame(ssh_client, remote_dir_full_path, pid_observer):
                            gather_errors.append("Failed to gather perf flame for PID {0}".format(pid_observer))
                    else:
                        if not self.__gather_perf_sample(ssh_client, remote_dir_full_path, pid_observer):
                            gather_errors.append("Failed to gather perf sample for PID {0}".format(pid_observer))
                        if not self.__gather_perf_flame(ssh_client, remote_dir_full_path, pid_observer):
                            gather_errors.append("Failed to gather perf flame for PID {0}".format(pid_observer))
                # Always try to gather top info
                self.__gather_top(ssh_client, remote_dir_full_path, pid_observer)

            # Package files even if some perf gathering failed
            tar_cmd = "cd /tmp && tar -czf {0}.tar.gz {0}/*".format(remote_dir_name)
            tar_cmd_request = ssh_client.exec_cmd(tar_cmd)
            self.stdio.verbose("tar request is {0}".format(tar_cmd_request))

            # Fix path calculation: remote_tar_file_path should be just the filename
            remote_tar_file_path = "{0}.tar.gz".format(remote_dir_name)
            remote_tar_full_path = "/tmp/{0}".format(remote_tar_file_path)

            try:
                file_size_str = get_file_size(ssh_client, remote_tar_full_path, self.stdio)
                # get_file_size returns a string, need to strip whitespace and check if it's a valid number
                if file_size_str:
                    file_size_str = file_size_str.strip()
                    # Check if the string contains only digits (may have newlines)
                    if file_size_str.replace('\n', '').replace('\r', '').isdigit():
                        file_size = int(file_size_str)
                    else:
                        self.stdio.warn("Invalid file size format: '{0}', file may not exist".format(file_size_str))
                        file_size = 0
                else:
                    file_size = 0
            except (ValueError, TypeError, Exception) as e:
                self.stdio.warn("Failed to get file size for {0}: {1}".format(remote_tar_full_path, e))
                file_size = 0

            if file_size > 0 and file_size < self.file_size_limit:
                local_file_path = "{0}/{1}.tar.gz".format(local_stored_path, remote_dir_name)
                try:
                    download_file(ssh_client, remote_tar_full_path, local_file_path, self.stdio)
                    self.__generate_flame_graph_svg(local_file_path, remote_dir_name, local_stored_path)
                    resp["gather_pack_path"] = local_file_path
                    resp["success"] = True
                    # Store partial failures as warnings, not errors
                    if gather_errors:
                        resp["warnings"] = gather_errors
                        resp["error"] = ""  # Success with warnings
                    else:
                        resp["error"] = ""
                except Exception as e:
                    resp["success"] = False
                    resp["error"] = "Failed to download file: {0}".format(str(e))
                    if gather_errors:
                        resp["warnings"] = gather_errors
                        resp["error"] += "; " + "; ".join(gather_errors)
            elif file_size >= self.file_size_limit:
                resp["success"] = False
                resp["error"] = "File too large ({0} bytes, limit: {1} bytes)".format(file_size, self.file_size_limit)
                if gather_errors:
                    resp["warnings"] = gather_errors
                    resp["error"] += "; " + "; ".join(gather_errors)
            else:
                resp["success"] = False
                resp["error"] = "Failed to create tar file or file size is 0"
                if gather_errors:
                    resp["warnings"] = gather_errors
                    resp["error"] += "; " + "; ".join(gather_errors)

            # Clean up remote tar file
            try:
                delete_file_force(ssh_client, remote_tar_full_path, self.stdio)
            except Exception as e:
                self.stdio.warn("Failed to delete remote tar file {0}: {1}".format(remote_tar_full_path, e))

            # Clean up remote directory
            try:
                ssh_client.exec_cmd("rm -rf {0}".format(remote_dir_full_path))
            except Exception as e:
                self.stdio.warn("Failed to clean up remote directory {0}: {1}".format(remote_dir_full_path, e))

        except Exception as e:
            self.stdio.exception("Unexpected error in __handle_from_node: {0}".format(e))
            resp["success"] = False
            resp["error"] = "Unexpected error: {0}".format(str(e))
        finally:
            # Close SSH connection
            if ssh_client:
                try:
                    ssh_client.ssh_close()
                except Exception as e:
                    self.stdio.verbose("Failed to close SSH connection: {0}".format(e))

        return resp

    def __get_flamegraph_scripts(self):
        """
        Get stackcollapse-perf.pl and flamegraph.pl paths from const (dependencies/bin/).
        Returns (stackcollapse_pl, flamegraph_pl) or (None, None) if not found.
        """
        # Get obdiag installation directory (similar to gather_obstack2.py)
        if getattr(sys, 'frozen', False):
            # PyInstaller packaged environment
            absPath = os.path.dirname(sys.executable)
        else:
            # Development environment
            absPath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

        # Build absolute paths based on installation directory
        # Remove leading './' from relative paths
        stackcollapse_rel_path = const.FLAMEGRAPH_STACKCOLLAPSE_PL.lstrip('./')
        flamegraph_rel_path = const.FLAMEGRAPH_FLAMEGRAPH_PL.lstrip('./')
        stackcollapse_pl = os.path.join(absPath, stackcollapse_rel_path)
        flamegraph_pl = os.path.join(absPath, flamegraph_rel_path)

        if os.path.isfile(stackcollapse_pl) and os.path.isfile(flamegraph_pl):
            return (stackcollapse_pl, flamegraph_pl)
        return (None, None)

    def __generate_flame_graph_svg(self, local_tar_path, remote_dir_name, local_stored_path):
        """
        Extract perf pack tar, generate flame graph SVG from flame.viz if present, re-pack tar.
        See https://github.com/oceanbase/obdiag/issues/95
        """
        stackcollapse_pl, flamegraph_pl = self.__get_flamegraph_scripts()
        if not stackcollapse_pl or not flamegraph_pl:
            self.stdio.verbose("FlameGraph scripts not found at {0}, {1}; skip generating flame graph SVG.".format(const.FLAMEGRAPH_STACKCOLLAPSE_PL, const.FLAMEGRAPH_FLAMEGRAPH_PL))
            return
        extract_dir = os.path.join(local_stored_path, remote_dir_name)
        try:
            with tarfile.open(local_tar_path, "r:gz") as tar:
                tar.extractall(path=local_stored_path)
            flame_viz = os.path.join(extract_dir, "flame.viz")
            if not os.path.isfile(flame_viz):
                self.stdio.verbose("No flame.viz in pack, skip flame graph SVG.")
                shutil.rmtree(extract_dir, ignore_errors=True)
                return
            started_loading = False
            try:
                self.stdio.start_loading("generate flame graph SVG")
                started_loading = True
                with open(flame_viz, "r", encoding="utf-8", errors="ignore") as f_in:
                    p1 = subprocess.Popen(
                        ["perl", stackcollapse_pl],
                        stdin=f_in,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=extract_dir,
                    )
                    p2 = subprocess.Popen(
                        ["perl", flamegraph_pl],
                        stdin=p1.stdout,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        cwd=extract_dir,
                    )
                    p1.stdout.close()
                    out, err = p2.communicate(timeout=120)
                if p2.returncode != 0:
                    self.stdio.verbose("flamegraph.pl failed: {0}".format(err.decode("utf-8", errors="ignore") if err else "unknown"))
                    return
                flame_svg = os.path.join(extract_dir, "flame.svg")
                with open(flame_svg, "wb") as f_out:
                    f_out.write(out)
                self.stdio.verbose("flame graph SVG written into pack: {0}".format(flame_svg))
                self.stdio.print("flame graph SVG generated: open flame.svg in the pack to view.")
                with tarfile.open(local_tar_path, "w:gz") as tar:
                    for name in os.listdir(extract_dir):
                        tar.add(os.path.join(extract_dir, name), arcname=name)
            except Exception as e:
                self.stdio.verbose("generate flame graph SVG failed: {0}".format(e))
            finally:
                if started_loading:
                    self.stdio.stop_loading("succeed")
        except Exception as e:
            self.stdio.verbose("extract or flame graph failed: {0}".format(e))
        finally:
            shutil.rmtree(extract_dir, ignore_errors=True)

    def __gather_perf_sample(self, ssh_client, gather_path, pid_observer):
        """
        Gather perf sample data.

        Args:
            ssh_client: SSH client instance
            gather_path: Remote directory path for gathering data
            pid_observer: Observer process PID

        Returns:
            bool: True if successful, False otherwise
        """
        started_loading = False
        try:
            self.stdio.start_loading('gather perf sample')
            started_loading = True
            cmd = "cd {gather_path} && perf record -o sample.data -e cycles -c {count_option} -p {pid} -g -- sleep 20".format(gather_path=gather_path, count_option=self.count_option, pid=pid_observer)
            self.stdio.verbose("gather perf sample, run cmd = [{0}]".format(cmd))
            ssh_client.exec_cmd(cmd)

            generate_data = "cd {gather_path} && perf script -i sample.data -F ip,sym -f > sample.viz".format(gather_path=gather_path)
            self.stdio.verbose("generate perf sample data, run cmd = [{0}]".format(generate_data))
            ssh_client.exec_cmd(generate_data)

            self.is_ready(ssh_client, os.path.join(gather_path, 'sample.viz'))
            self.stdio.stop_loading('gather perf sample')
            return True
        except KeyboardInterrupt:
            self.stdio.warn("Gather perf sample interrupted by user")
            raise
        except SystemExit:
            raise
        except Exception as e:
            self.stdio.error("generate perf sample data on server [{0}] failed: {1}".format(ssh_client.get_name(), str(e)))
            return False
        finally:
            if started_loading:
                try:
                    self.stdio.stop_loading('gather perf sample')
                except Exception as e:
                    # Loading indicator cleanup failure is non-critical, log verbosely
                    self.stdio.verbose("Failed to stop loading indicator: {0}".format(e))

    def __perf_checker(self, ssh_client):
        """
        Check if perf command is available on the remote server.

        Args:
            ssh_client: SSH client instance

        Returns:
            bool: True if perf is installed, False otherwise
        """
        cmd = "command -v perf"
        result = ssh_client.exec_cmd(cmd)

        if result:
            self.stdio.verbose("perf is installed at [{0}] on server [{1}]".format(result, ssh_client.get_name()))
            return True
        else:
            self.stdio.error("perf is not installed on server [{0}]. gather perf information will be skipped. Please install perf manually. ".format(ssh_client.get_name()))
            return False

    def __gather_perf_flame(self, ssh_client, gather_path, pid_observer):
        """
        Gather perf flame data.

        Args:
            ssh_client: SSH client instance
            gather_path: Remote directory path for gathering data
            pid_observer: Observer process PID

        Returns:
            bool: True if successful, False otherwise
        """
        started_loading = False
        try:
            self.stdio.start_loading('gather perf flame')
            started_loading = True
            perf_cmd = "cd {gather_path} && perf record -o flame.data -F 99 -p {pid} -g -- sleep 20".format(gather_path=gather_path, pid=pid_observer)
            self.stdio.verbose("gather perf, run cmd = [{0}]".format(perf_cmd))
            ssh_client.exec_cmd(perf_cmd)

            generate_data = "cd {gather_path} && perf script -i flame.data > flame.viz".format(gather_path=gather_path)
            self.stdio.verbose("generate perf data, run cmd = [{0}]".format(generate_data))
            ssh_client.exec_cmd(generate_data)

            self.is_ready(ssh_client, os.path.join(gather_path, 'flame.viz'))
            self.stdio.stop_loading('gather perf flame')
            return True
        except KeyboardInterrupt:
            self.stdio.warn("Gather perf flame interrupted by user")
            raise
        except SystemExit:
            raise
        except Exception as e:
            self.stdio.error("generate perf data on server [{0}] failed: {1}".format(ssh_client.get_name(), str(e)))
            return False
        finally:
            if started_loading:
                try:
                    self.stdio.stop_loading('gather perf flame')
                except Exception as e:
                    # Loading indicator cleanup failure is non-critical, log verbosely
                    self.stdio.verbose("Failed to stop loading indicator: {0}".format(e))

    def __gather_top(self, ssh_client, gather_path, pid_observer):
        """
        Gather top information for observer process.

        Args:
            ssh_client: SSH client instance
            gather_path: Remote directory path for gathering data
            pid_observer: Observer process PID

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cmd = "cd {gather_path} && top -Hp {pid} -b -n 1 > top.txt".format(gather_path=gather_path, pid=pid_observer)
            self.stdio.verbose("gather top, run cmd = [{0}]".format(cmd))
            ssh_client.exec_cmd(cmd)
            return True
        except KeyboardInterrupt:
            self.stdio.warn("Gather top interrupted by user")
            raise
        except SystemExit:
            raise
        except Exception as e:
            self.stdio.error("gather top on server [{0}] failed: {1}".format(ssh_client.get_name(), str(e)))
            return False

    @Util.retry(3, 5)
    def is_ready(self, ssh_client, remote_path):
        """
        Check if the file is ready (not empty).

        Args:
            ssh_client: SSH client instance
            remote_path: Remote file path to check

        Raises:
            Exception: If file is empty or doesn't exist
        """
        try:
            self.stdio.verbose("check whether the file {remote_path} is empty".format(remote_path=remote_path))
            is_empty_file_res = is_empty_file(ssh_client, remote_path, self.stdio)
            if is_empty_file_res:
                error_msg = "The server {host_ip} file {remote_path} is empty, waiting for the collection to complete".format(host_ip=ssh_client.get_name(), remote_path=remote_path)
                self.stdio.verbose(error_msg)
                raise Exception(error_msg)
        except Exception as e:
            # Re-raise the exception to trigger retry mechanism
            raise e

    def __get_overall_summary(self, node_summary_tuple):
        """
        Generate summary table from gather tuples using BaseHandler template method.

        Args:
            node_summary_tuple: List of tuples (node_ip, is_err, error_msg, file_size, consume_time, pack_path)

        Returns:
            str: Formatted summary table string
        """
        summary_tab = []
        field_names = ["Node", "Status", "Size", "Time", "PackPath"]
        for tup in node_summary_tuple:
            node = tup[0]
            is_err = tup[1]
            error_msg = tup[2]
            file_size = tup[3]
            consume_time = tup[4]
            pack_path = tup[5]
            try:
                if isinstance(file_size, str):
                    file_size = int(file_size) if file_size.isdigit() else 0
                format_file_size = FileUtil.size_format(num=file_size, output_str=True)
            except (ValueError, TypeError, Exception):
                format_file_size = FileUtil.size_format(num=0, output_str=True)

            status = "Error: " + error_msg if is_err else "Completed"
            summary_tab.append((node, status, format_file_size, "{0} s".format(int(consume_time)), pack_path))

        # Use BaseHandler template method
        return self._generate_summary_table(field_names, summary_tab, "Gather Perf Summary")
