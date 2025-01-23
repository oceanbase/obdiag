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
@time: 2024/9/18
@file: analyze_memory.py
@desc:
"""
import os
import time
import plotly.graph_objects as go
import plotly.io as pio
import datetime
import tabulate
import threading
import uuid
from src.common.command import get_observer_version
from src.common.tool import DirectoryUtil, TimeUtils, Util, NetUtils, FileUtil
from src.common.obdiag_exception import OBDIAGFormatException
from src.common.constant import const
from src.common.command import download_file, get_logfile_name_list, mkdir, delete_file
from src.common.command import SshClient
from src.common.ssh_client.local_client import LocalClient
from src.common.result_type import ObdiagResult


class AnalyzeMemoryHandler(object):
    def __init__(self, context):
        super(AnalyzeMemoryHandler, self).__init__()
        self.context = context
        self.stdio = context.stdio
        self.ob_cluster = self.context.cluster_config
        self.directly_analyze_files = False
        self.analyze_files_list = []
        self.is_ssh = True
        self.gather_timestamp = None
        self.gather_ob_log_temporary_dir = const.GATHER_LOG_TEMPORARY_DIR_DEFAULT
        self.gather_pack_dir = None
        self.ob_log_dir = None
        self.from_time_str = None
        self.to_time_str = None
        self.grep_args = None
        self.scope = 'observer'
        self.zip_encrypt = False
        self.config_path = const.DEFAULT_CONFIG_PATH
        self.version = None

    def init_config(self):
        self.nodes = self.context.cluster_config['servers']
        self.inner_config = self.context.inner_config
        if self.inner_config is None:
            self.file_number_limit = 20
            self.file_size_limit = 2 * 1024 * 1024 * 1024
        else:
            basic_config = self.inner_config['obdiag']['basic']
            self.file_number_limit = int(basic_config["file_number_limit"])
            self.file_size_limit = int(FileUtil.size(basic_config["file_size_limit"]))
            self.config_path = basic_config['config_path']
        if self.version is None:
            self.version = self.get_version()
        return True

    def init_option(self):
        options = self.context.options
        from_option = Util.get_option(options, 'from')
        version = Util.get_option(options, 'version')
        to_option = Util.get_option(options, 'to')
        since_option = Util.get_option(options, 'since')
        store_dir_option = Util.get_option(options, 'store_dir')
        grep_option = Util.get_option(options, 'grep')
        files_option = Util.get_option(options, 'files')
        temp_dir_option = Util.get_option(options, 'temp_dir')
        if files_option:
            self.is_ssh = False
            self.directly_analyze_files = True
            self.analyze_files_list = files_option
            if version:
                self.version = version
            else:
                self.stdio.error('the option --files requires the --version option to be specified')
                return False
        if from_option is not None and to_option is not None:
            try:
                from_timestamp = TimeUtils.parse_time_str(from_option)
                to_timestamp = TimeUtils.parse_time_str(to_option)
                self.from_time_str = from_option
                self.to_time_str = to_option
            except OBDIAGFormatException:
                self.stdio.exception('Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. from_datetime={0}, to_datetime={1}'.format(from_option, to_option))
                return False
            if to_timestamp <= from_timestamp:
                self.stdio.exception('Error: from datetime is larger than to datetime, please check.')
                return False
        elif (from_option is None or to_option is None) and since_option is not None:
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            if not self.directly_analyze_files:
                self.stdio.print('analyze memory from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.print('No time option provided, default processing is based on the last 30 minutes')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_option is not None:
                self.from_time_str = (now_time - datetime.timedelta(seconds=TimeUtils.parse_time_length_to_sec(since_option))).strftime('%Y-%m-%d %H:%M:%S')
            else:
                self.from_time_str = (now_time - datetime.timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
            if not self.directly_analyze_files:
                self.stdio.print('analyze log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option is not None:
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        if grep_option is not None:
            self.grep_args = grep_option
        if temp_dir_option:
            self.gather_ob_log_temporary_dir = temp_dir_option
        return True

    def get_version(self):
        observer_version = ""
        try:
            observer_version = get_observer_version(self.context)
        except Exception as e:
            self.stdio.exception("failed to get observer version:{0}".format(e))
        self.stdio.verbose("get observer version: {0}".format(observer_version))
        return observer_version

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init option failed")
        if not self.init_config():
            self.stdio.error('init config failed')
            return ObdiagResult(ObdiagResult.SERVER_ERROR_CODE, error_data="init config failed")
        local_store_parent_dir = os.path.join(self.gather_pack_dir, "obdiag_analyze_pack_{0}".format(TimeUtils.timestamp_to_filename_time(TimeUtils.get_current_us_timestamp())))
        self.stdio.verbose("Use {0} as pack dir.".format(local_store_parent_dir))
        analyze_tuples = []

        # analyze_thread default thread nums is 3
        analyze_thread_nums = int(self.context.inner_config.get("analyze", {}).get("thread_nums") or 3)
        pool_sema = threading.BoundedSemaphore(value=analyze_thread_nums)

        def handle_from_node(node):
            with pool_sema:
                st = time.time()
                resp = self.__handle_from_node(node, local_store_parent_dir)
                analyze_tuples.append((node.get("ip"), resp["skip"], resp["error"], int(time.time() - st), resp["result_pack_path"]))

        nodes_threads = []
        self.stdio.print("analyze nodes's log start. Please wait a moment...")
        self.stdio.start_loading('analyze memory start')
        for node in self.nodes:
            if self.directly_analyze_files:
                if nodes_threads:
                    break
                node["ip"] = '127.0.0.1'
            else:
                if not self.is_ssh:
                    local_ip = NetUtils.get_inner_ip()
                    node = self.nodes[0]
                    node["ip"] = local_ip
            node_threads = threading.Thread(target=handle_from_node, args=(node,))
            node_threads.start()
            nodes_threads.append(node_threads)
        for node_thread in nodes_threads:
            node_thread.join()

        summary_tuples = self.__get_overall_summary(analyze_tuples)
        self.stdio.stop_loading('analyze memory sucess')
        self.stdio.print(summary_tuples)
        FileUtil.write_append(os.path.join(local_store_parent_dir, "result_summary.txt"), summary_tuples)
        analyze_info = ""
        with open(os.path.join(local_store_parent_dir, "result_summary.txt"), "r", encoding="utf-8") as f:
            analyze_info = f.read()
        return ObdiagResult(ObdiagResult.SUCCESS_CODE, data={"result": analyze_info})

    def __handle_from_node(self, node, local_store_parent_dir):
        resp = {"skip": False, "error": "", "result_pack_path": ""}
        remote_ip = node.get("ip") if self.is_ssh else '127.0.0.1'
        try:
            ssh_client = SshClient(self.context, node)
            self.stdio.verbose("Sending Collect Shell Command to node {0} ...".format(remote_ip))
            DirectoryUtil.mkdir(path=local_store_parent_dir, stdio=self.stdio)
            local_store_dir = "{0}/{1}".format(local_store_parent_dir, ssh_client.get_name())
            DirectoryUtil.mkdir(path=local_store_dir, stdio=self.stdio)
        except Exception as e:
            resp["skip"] = True
            resp["error"] = "Please check the node conf about {0}".format(remote_ip)
            raise Exception("Please check the node conf about {0}".format(remote_ip))

        from_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.from_time_str))
        to_datetime_timestamp = TimeUtils.timestamp_to_filename_time(TimeUtils.datetime_to_timestamp(self.to_time_str))
        gather_dir_name = "ob_log_{0}_{1}_{2}".format(ssh_client.get_name(), from_datetime_timestamp, to_datetime_timestamp)
        gather_dir_full_path = "{0}/{1}_{2}".format(self.gather_ob_log_temporary_dir, gather_dir_name, str(uuid.uuid4())[:6])
        mkdir(ssh_client, gather_dir_full_path)
        log_list, resp = self.__handle_log_list(ssh_client, node, resp)
        result_pack_path = "./{0}".format(os.path.relpath(local_store_dir, self.gather_pack_dir))
        resp["result_pack_path"] = result_pack_path
        if resp["skip"]:
            return resp
        self.stdio.print(FileUtil.show_file_list_tabulate(remote_ip, log_list, self.stdio))
        self.stdio.print("analyze log. Please wait a moment...")
        self.stdio.start_loading("analyze memory start")
        for log_name in log_list:
            if self.directly_analyze_files:
                self.__pharse_offline_log_file(ssh_client, log_name=log_name, local_store_dir=local_store_dir)
                analyze_log_full_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
            else:
                self.__pharse_log_file(ssh_client, node=node, log_name=log_name, gather_path=gather_dir_full_path, local_store_dir=local_store_dir)
                analyze_log_full_path = "{0}/{1}".format(local_store_dir, log_name)
            self.stdio.verbose("local file storage path: {0}".format(analyze_log_full_path))

        tenant_memory_info_dict = dict()
        for log_name in log_list:
            memory_info = dict()
            if self.directly_analyze_files:
                analyze_log_full_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
            else:
                analyze_log_full_path = "{0}/{1}".format(local_store_dir, log_name)
            self.__parse_log_lines(analyze_log_full_path, memory_info)
            for sample_time in memory_info:
                for tenant in memory_info[sample_time]:
                    if tenant in tenant_memory_info_dict:
                        tenant_memory_info_dict[tenant][sample_time] = memory_info[sample_time][tenant]
                    else:
                        tenant_memory_info_dict[tenant] = dict()
                        tenant_memory_info_dict[tenant][sample_time] = memory_info[sample_time][tenant]
            self.stdio.verbose("rm local file storage path: {0}".format(analyze_log_full_path))
            FileUtil.rm(analyze_log_full_path, self.stdio)
        self.stdio.stop_loading("analyze memory succeed")
        try:
            fig = go.Figure()
            colors = ['blue', 'orange', 'green', 'red', 'purple', 'cyan', 'magenta', 'yellow', 'black', 'brown', 'pink', 'gray', 'lime', 'teal', 'navy']
            if len(tenant_memory_info_dict) == 0:
                resp["skip"] = True
                resp["error"] = "failed to analyze memory data from the log"
            elif len(tenant_memory_info_dict) < 20 and len(tenant_memory_info_dict) > 0:
                i = 0
                x_lines = []
                x_vals = []
                for tenant_id in tenant_memory_info_dict:
                    fig_tenant = go.Figure()
                    fig_ctx = go.Figure()
                    fig_mod = go.Figure()

                    color = colors[i % len(colors)]
                    i += 1
                    if not x_lines:
                        x_lines = [t.split(' ')[1] for t in sorted(tenant_memory_info_dict[tenant_id].keys())]
                        x_interval = 12
                        x_n = 0
                        for n in range(len(x_lines)):
                            if x_n == 0:
                                x_vals.append(x_lines[n])
                            x_n = x_n + 1
                            if x_n == x_interval:
                                x_n = 0
                        if x_n < 6:
                            x_vals[-1] = x_lines[-1]
                        else:
                            if x_vals[-1] != x_lines[-1]:
                                x_vals.append(x_lines[-1])
                    tenant_hold_lines = [round(int(tenant_memory_info_dict[tenant_id][t]['hold']) / 1024 / 1024) for t in sorted(tenant_memory_info_dict[tenant_id].keys())]
                    fig_tenant.add_trace(go.Scatter(x=x_lines, y=tenant_hold_lines, mode='lines'))
                    fig_tenant.update_layout(title='租户-{0} hold内存曲线图'.format(tenant_id), xaxis_title='时间', yaxis_title='值(MB)')
                    fig_tenant.update_xaxes(tickvals=x_vals, ticktext=[str(x) for x in x_vals])
                    fig_tenant.update_yaxes(tickformat='.0f')
                    ctx_memory_info_dict = dict()
                    mod_memory_info_dict = dict()
                    for t in sorted(tenant_memory_info_dict[tenant_id].keys()):
                        for ctx in tenant_memory_info_dict[tenant_id][t]['ctx_info']:
                            ctx_name = ctx['ctx_name']
                            if ctx_name in ctx_memory_info_dict:
                                ctx_memory_info_dict[ctx_name][t] = ctx['hold_bytes']
                            else:
                                ctx_memory_info_dict[ctx_name] = dict()
                                ctx_memory_info_dict[ctx_name][t] = ctx['hold_bytes']
                            if 'mod_info' in ctx:
                                mod_info = ctx['mod_info']
                                for mod in mod_info:
                                    mod_name = mod['mod_name']
                                    key_name = ctx_name + '-' + mod_name
                                    if key_name in mod_memory_info_dict:
                                        mod_memory_info_dict[key_name][t] = mod['mod_hold_bytes']
                                    else:
                                        mod_memory_info_dict[key_name] = dict()
                                        mod_memory_info_dict[key_name][t] = mod['mod_hold_bytes']
                    for ctx_name in ctx_memory_info_dict:
                        ctx_hold_lines = [round(int(ctx_memory_info_dict[ctx_name][t]) / 1024 / 1024) for t in sorted(ctx_memory_info_dict[ctx_name].keys())]
                        fig_ctx.add_trace(go.Scatter(x=x_lines, y=ctx_hold_lines, mode='lines', name='{0}'.format(ctx_name)))
                        fig_ctx.update_layout(title='租户-{0} ctx hold内存曲线图'.format(tenant_id), xaxis_title='时间', yaxis_title='值(MB)')
                        fig_ctx.update_xaxes(tickvals=x_vals, ticktext=[str(x) for x in x_vals])
                        fig_ctx.update_yaxes(tickformat='.0f')
                    if len(mod_memory_info_dict) > 10:
                        mod_avg_memory_info_dict = dict()
                        for mod_name in mod_memory_info_dict:
                            if '-SUMMARY' not in mod_name:
                                mod_list = list(mod_memory_info_dict[mod_name].values())
                                mod_avg = sum(mod_list) / len(mod_list)
                                mod_avg_memory_info_dict[mod_name] = mod_avg
                        top_10_keys = sorted(mod_avg_memory_info_dict, key=mod_avg_memory_info_dict.get, reverse=True)[:10]
                        for key in top_10_keys:
                            mod_hold_lines = [round(mod_memory_info_dict[key][t] / 1024 / 1024) for t in sorted(mod_memory_info_dict[key].keys())]
                            fig_mod.add_trace(go.Scatter(x=x_lines, y=mod_hold_lines, mode='lines', name='{0}'.format(key)))
                            fig_mod.update_layout(title='租户-{0} top10 mod hold内存曲线图'.format(tenant_id), xaxis_title='时间', yaxis_title='值(MB)')
                            fig_mod.update_xaxes(tickvals=x_vals, ticktext=[str(x) for x in x_vals])
                            fig_mod.update_yaxes(tickformat='.0f')
                    else:
                        for key in mod_memory_info_dict:
                            if '-SUMMARY' not in key:
                                mod_hold_lines = [round(mod_memory_info_dict[key][t] / 1024 / 1024) for t in sorted(mod_memory_info_dict[key].keys())]
                                fig_mod.add_trace(go.Scatter(x=x_lines, y=mod_hold_lines, mode='lines', name='{0}'.format(key)))
                                fig_mod.update_layout(title='租户-{0} top10 mod hold内存曲线图'.format(tenant_id), xaxis_title='时间', yaxis_title='值(MB)')
                                fig_mod.update_xaxes(tickvals=x_vals, ticktext=[str(x) for x in x_vals])
                                fig_mod.update_yaxes(tickformat='.0f')
                    html_fig_tenant = pio.to_html(fig_tenant, full_html=False)
                    html_fig_ctx = pio.to_html(fig_ctx, full_html=False)
                    html_fig_mod = pio.to_html(fig_mod, full_html=False)
                    html_combined = '''
                            <html>
                            <head>
                                <title>tenant-{0}_hold_memory</title>
                                    <style>
                                    body {{
                                        padding-top: 60px;
                                        font: 16px/1.8 -apple-system, blinkmacsystemfont, "Helvetica Neue", helvetica, segoe ui, arial, roboto, "PingFang SC", "miui", "Hiragino Sans GB", "Microsoft Yahei", sans-serif;
                                        background: #f4f6fa linear-gradient(180deg, #006aff 0%, #006aff00 100%) no-repeat;
                                        background-size: auto 120px;
                                    }}
                                    header {{
                                        padding: 1em;
                                        margin: -60px auto 0;
                                        max-width: 1280px;
                                    }}

                                    header>svg {{
                                        margin-left: -2em;
                                    }}
                                </style>
                            </head>
                            <body>
                                <header>
                                    <svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="160"
                                        viewBox="0 0 122 16">
                                        <defs>
                                            <path id="a" d="M0 0h12.019v4.626H0z" />
                                        </defs>
                                        <g fill="none" fill-rule="evenodd">
                                            <path fill="#FFF"
                                                d="M64.285 9.499 66.2 5.746l.633 3.753zm.935-7.086-6.08 11.273h3.027l.985-1.96h4.054l.317 1.96h3.025l-2.23-11.273zM37.52 9.29a3.912 3.912 0 0 1-1.937 1.636 3.334 3.334 0 0 1-1.22.233 2.75 2.75 0 0 1-1.14-.233 2.363 2.363 0 0 1-.866-.65 2.511 2.511 0 0 1-.497-.986 2.91 2.91 0 0 1-.035-1.242c.077-.447.23-.861.458-1.24a4 4 0 0 1 .835-.987c.327-.279.69-.495 1.087-.65a3.355 3.355 0 0 1 1.22-.231c.414 0 .795.077 1.14.23.346.156.636.372.874.651.238.28.405.608.504.988.1.378.11.792.035 1.24-.077.448-.23.862-.458 1.24m2.446-5.471a4.538 4.538 0 0 0-1.701-1.264c-.692-.303-1.477-.455-2.355-.455-.888 0-1.727.152-2.517.455a7.173 7.173 0 0 0-2.12 1.264 7.525 7.525 0 0 0-1.568 1.891 6.882 6.882 0 0 0-.847 2.34c-.143.837-.127 1.616.05 2.34a4.72 4.72 0 0 0 .92 1.891c.44.538 1.003.959 1.69 1.263.685.304 1.473.455 2.36.455a6.96 6.96 0 0 0 2.51-.455 7.215 7.215 0 0 0 2.134-1.263 7.384 7.384 0 0 0 1.575-1.891 6.893 6.893 0 0 0 .85-2.34c.141-.837.125-1.617-.05-2.34a4.667 4.667 0 0 0-.93-1.891M59.105 11.203h-5.744l.332-1.943h5.556l.424-2.481h-5.556l.322-1.884h5.744l.424-2.482h-8.583L50.1 13.686l8.586-.002zM78.872 9.176l-3.315-6.764H72.72l-1.925 11.273h2.839l1.176-6.763 3.314 6.763h2.824l1.925-11.273H80.05zM88.09 11.129c-.342.19-.869.284-1.584.284h-.997l.409-2.392h.997c.714 0 1.21.095 1.485.284.278.19.38.493.308.912-.071.418-.277.723-.618.912m-1.426-6.474h.895c.888 0 1.27.365 1.145 1.092-.125.727-.63 1.091-1.518 1.091h-.895zm4.877 5.757c.058-.34.074-.659.048-.957a1.935 1.935 0 0 0-.246-.807 1.752 1.752 0 0 0-.59-.607 2.881 2.881 0 0 0-.974-.365c.45-.26.806-.576 1.068-.95.261-.374.44-.845.537-1.413.16-.936.022-1.654-.414-2.153-.435-.498-1.156-.748-2.16-.748h-4.602l-1.923 11.273h4.934c.579 0 1.112-.07 1.6-.21a3.9 3.9 0 0 0 1.286-.627c.371-.28.68-.623.929-1.032.248-.409.418-.876.507-1.404M108.454 6.808c-.218-.08-.44-.154-.664-.224a3.082 3.082 0 0 1-.595-.247 1.178 1.178 0 0 1-.4-.336c-.092-.13-.121-.293-.088-.494.049-.288.206-.523.467-.702.263-.18.576-.27.944-.27.29 0 .577.063.865.188.289.124.565.316.829.574l1.52-2.286a6.084 6.084 0 0 0-1.577-.68 6.259 6.259 0 0 0-1.656-.231c-.578 0-1.117.088-1.615.268-.5.179-.939.431-1.317.755a4.32 4.32 0 0 0-.952 1.166 4.66 4.66 0 0 0-.527 1.518c-.095.558-.089 1.018.017 1.382.107.364.278.665.512.904.234.24.515.431.842.576.328.145.666.278 1.012.396.29.11.535.21.738.3.202.089.361.187.478.29a.75.75 0 0 1 .23.344.972.972 0 0 1 .013.442c-.048.28-.205.527-.469.748-.264.219-.628.328-1.09.328-.406 0-.8-.095-1.182-.284-.383-.189-.754-.478-1.113-.867l-1.618 2.363c1.033.847 2.24 1.27 3.619 1.27.666 0 1.277-.092 1.834-.276a4.687 4.687 0 0 0 1.466-.778c.42-.333.762-.735 1.03-1.203.268-.47.453-.991.55-1.57.147-.858.051-1.552-.287-2.086-.339-.533-.944-.958-1.816-1.278M48.175 2.099c-.763 0-1.516.147-2.262.44a7.259 7.259 0 0 0-2.04 1.227 7.56 7.56 0 0 0-1.578 1.868 6.757 6.757 0 0 0-.878 2.385c-.147.867-.125 1.666.068 2.4.194.732.507 1.365.942 1.899.436.532.973.946 1.613 1.24a4.93 4.93 0 0 0 2.09.44c.366 0 .743-.037 1.133-.111a9.502 9.502 0 0 0 1.276-.35l.308-.107.592-3.467c-.86.798-1.744 1.196-2.651 1.196-.415 0-.788-.08-1.118-.24a2.27 2.27 0 0 1-.821-.658 2.452 2.452 0 0 1-.454-.986 3.183 3.183 0 0 1-.012-1.241c.074-.438.219-.847.434-1.227.213-.378.474-.704.782-.978a3.61 3.61 0 0 1 1.044-.65c.39-.16.795-.24 1.222-.24.965 0 1.704.415 2.22 1.24l.596-3.497a6.472 6.472 0 0 0-1.249-.441 5.513 5.513 0 0 0-1.257-.142M101.474 11.32c-.46-.092-1.36-.142-2.892.223l.349 2.185h3.025z" />
                                            <path fill="#FFF"
                                                d="m101.35 10.66-.492-2.483c-.731.012-1.647.123-2.784.41-.124.032-.252.065-.382.101-.63.173-1.215.29-1.744.368l1.66-3.267.356 2.186c1.137-.287 2.053-.398 2.785-.411l-1.023-5.108h-3.097l-6.08 11.272h3.025l1.091-2.146c.91-.064 2.014-.223 3.269-.567.182-.05.355-.093.523-.133 1.533-.365 2.432-.314 2.892-.223M116.537 6.871c-2.251.59-3.965.534-4.713.463l-.4 2.423c.213.017.461.03.746.036 1.11.021 2.738-.08 4.701-.595 1.863-.487 2.96-.457 3.524-.364l.402-2.433c-.945-.1-2.322-.037-4.26.47M117.12 2.51c-2.152.562-3.812.537-4.607.472l-.601 3.72c.527.048 1.526.09 2.852-.094l.212-1.297c.743-.09 1.575-.239 2.478-.475 1.986-.519 3.1-.45 3.628-.344l.403-2.44c-.947-.116-2.353-.07-4.366.457M115.994 11.076c-.711.186-1.369.308-1.96.385l.195-1.19a16.13 16.13 0 0 1-2.116.107 13.007 13.007 0 0 1-.733-.035l-.604 3.61c.235.02.519.038.85.045 1.11.02 2.74-.08 4.703-.595 1.756-.46 2.831-.458 3.42-.378l.401-2.428c-.94-.085-2.287-.011-4.156.479M13.32 16a21.931 21.931 0 0 1 2.705-.943 22.178 22.178 0 0 1 8.428-.686v-2.878a25.035 25.035 0 0 0-9.87 1.006c-.246.075-.49.154-.734.24-.48.163-.952.34-1.415.53z" />
                                            <path fill="#FFF"
                                                d="M24.453 2.157v8.618a25.783 25.783 0 0 0-10.837 1.286A34.304 34.304 0 0 1 0 13.842V5.225a25.741 25.741 0 0 0 10.835-1.285 34.33 34.33 0 0 1 13.617-1.781" />
                                            <g>
                                                <mask id="b" fill="#fff">
                                                    <use xlink:href="#a" />
                                                </mask>
                                                <path fill="#FFF"
                                                    d="M11.132 0a21.931 21.931 0 0 1-2.704.942A22.178 22.178 0 0 1 0 1.628v2.878A25.035 25.035 0 0 0 9.87 3.5a24.633 24.633 0 0 0 2.15-.77z"
                                                    mask="url(#b)" />
                                            </g>
                                        </g>
                                    </svg>
                                </header>
                                <hr>
                                {1}
                                <hr>
                                {2}
                                <hr>
                                {3}
                            </body>
                            </html>
                            '''.format(
                        tenant_id, html_fig_tenant, html_fig_ctx, html_fig_mod
                    )
                    with open('{0}/tenant-{1}_hold_memory.html'.format(local_store_dir, tenant_id), 'w') as f:
                        f.write(html_combined)
                    fig.add_trace(go.Scatter(x=x_lines, y=tenant_hold_lines, mode='lines', name='tenant-{0}'.format(tenant_id), line=dict(color=color)))
                fig.update_layout(title='TOP 15租户hold内存曲线图', xaxis_title='时间', yaxis_title='值(MB)')
                fig.update_xaxes(tickvals=x_vals, ticktext=[str(x) for x in x_vals])
                fig.update_yaxes(tickformat='.0f')
                html_fig = pio.to_html(fig, full_html=False)
                html_top15_combined = '''
                    <html>
                    <head>
                        <title>TOP 15租户hold内存曲线图</title>
                            <style>
                            body {{
                                padding-top: 60px;
                                font: 16px/1.8 -apple-system, blinkmacsystemfont, "Helvetica Neue", helvetica, segoe ui, arial, roboto, "PingFang SC", "miui", "Hiragino Sans GB", "Microsoft Yahei", sans-serif;
                                background: #f4f6fa linear-gradient(180deg, #006aff 0%, #006aff00 100%) no-repeat;
                                background-size: auto 120px;
                            }}
                            header {{
                                padding: 1em;
                                margin: -60px auto 0;
                                max-width: 1280px;
                            }}

                            header>svg {{
                                margin-left: -2em;
                            }}
                        </style>
                    </head>
                    <body>
                        <header>
                            <svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="160"
                                viewBox="0 0 122 16">
                                <defs>
                                    <path id="a" d="M0 0h12.019v4.626H0z" />
                                </defs>
                                <g fill="none" fill-rule="evenodd">
                                    <path fill="#FFF"
                                        d="M64.285 9.499 66.2 5.746l.633 3.753zm.935-7.086-6.08 11.273h3.027l.985-1.96h4.054l.317 1.96h3.025l-2.23-11.273zM37.52 9.29a3.912 3.912 0 0 1-1.937 1.636 3.334 3.334 0 0 1-1.22.233 2.75 2.75 0 0 1-1.14-.233 2.363 2.363 0 0 1-.866-.65 2.511 2.511 0 0 1-.497-.986 2.91 2.91 0 0 1-.035-1.242c.077-.447.23-.861.458-1.24a4 4 0 0 1 .835-.987c.327-.279.69-.495 1.087-.65a3.355 3.355 0 0 1 1.22-.231c.414 0 .795.077 1.14.23.346.156.636.372.874.651.238.28.405.608.504.988.1.378.11.792.035 1.24-.077.448-.23.862-.458 1.24m2.446-5.471a4.538 4.538 0 0 0-1.701-1.264c-.692-.303-1.477-.455-2.355-.455-.888 0-1.727.152-2.517.455a7.173 7.173 0 0 0-2.12 1.264 7.525 7.525 0 0 0-1.568 1.891 6.882 6.882 0 0 0-.847 2.34c-.143.837-.127 1.616.05 2.34a4.72 4.72 0 0 0 .92 1.891c.44.538 1.003.959 1.69 1.263.685.304 1.473.455 2.36.455a6.96 6.96 0 0 0 2.51-.455 7.215 7.215 0 0 0 2.134-1.263 7.384 7.384 0 0 0 1.575-1.891 6.893 6.893 0 0 0 .85-2.34c.141-.837.125-1.617-.05-2.34a4.667 4.667 0 0 0-.93-1.891M59.105 11.203h-5.744l.332-1.943h5.556l.424-2.481h-5.556l.322-1.884h5.744l.424-2.482h-8.583L50.1 13.686l8.586-.002zM78.872 9.176l-3.315-6.764H72.72l-1.925 11.273h2.839l1.176-6.763 3.314 6.763h2.824l1.925-11.273H80.05zM88.09 11.129c-.342.19-.869.284-1.584.284h-.997l.409-2.392h.997c.714 0 1.21.095 1.485.284.278.19.38.493.308.912-.071.418-.277.723-.618.912m-1.426-6.474h.895c.888 0 1.27.365 1.145 1.092-.125.727-.63 1.091-1.518 1.091h-.895zm4.877 5.757c.058-.34.074-.659.048-.957a1.935 1.935 0 0 0-.246-.807 1.752 1.752 0 0 0-.59-.607 2.881 2.881 0 0 0-.974-.365c.45-.26.806-.576 1.068-.95.261-.374.44-.845.537-1.413.16-.936.022-1.654-.414-2.153-.435-.498-1.156-.748-2.16-.748h-4.602l-1.923 11.273h4.934c.579 0 1.112-.07 1.6-.21a3.9 3.9 0 0 0 1.286-.627c.371-.28.68-.623.929-1.032.248-.409.418-.876.507-1.404M108.454 6.808c-.218-.08-.44-.154-.664-.224a3.082 3.082 0 0 1-.595-.247 1.178 1.178 0 0 1-.4-.336c-.092-.13-.121-.293-.088-.494.049-.288.206-.523.467-.702.263-.18.576-.27.944-.27.29 0 .577.063.865.188.289.124.565.316.829.574l1.52-2.286a6.084 6.084 0 0 0-1.577-.68 6.259 6.259 0 0 0-1.656-.231c-.578 0-1.117.088-1.615.268-.5.179-.939.431-1.317.755a4.32 4.32 0 0 0-.952 1.166 4.66 4.66 0 0 0-.527 1.518c-.095.558-.089 1.018.017 1.382.107.364.278.665.512.904.234.24.515.431.842.576.328.145.666.278 1.012.396.29.11.535.21.738.3.202.089.361.187.478.29a.75.75 0 0 1 .23.344.972.972 0 0 1 .013.442c-.048.28-.205.527-.469.748-.264.219-.628.328-1.09.328-.406 0-.8-.095-1.182-.284-.383-.189-.754-.478-1.113-.867l-1.618 2.363c1.033.847 2.24 1.27 3.619 1.27.666 0 1.277-.092 1.834-.276a4.687 4.687 0 0 0 1.466-.778c.42-.333.762-.735 1.03-1.203.268-.47.453-.991.55-1.57.147-.858.051-1.552-.287-2.086-.339-.533-.944-.958-1.816-1.278M48.175 2.099c-.763 0-1.516.147-2.262.44a7.259 7.259 0 0 0-2.04 1.227 7.56 7.56 0 0 0-1.578 1.868 6.757 6.757 0 0 0-.878 2.385c-.147.867-.125 1.666.068 2.4.194.732.507 1.365.942 1.899.436.532.973.946 1.613 1.24a4.93 4.93 0 0 0 2.09.44c.366 0 .743-.037 1.133-.111a9.502 9.502 0 0 0 1.276-.35l.308-.107.592-3.467c-.86.798-1.744 1.196-2.651 1.196-.415 0-.788-.08-1.118-.24a2.27 2.27 0 0 1-.821-.658 2.452 2.452 0 0 1-.454-.986 3.183 3.183 0 0 1-.012-1.241c.074-.438.219-.847.434-1.227.213-.378.474-.704.782-.978a3.61 3.61 0 0 1 1.044-.65c.39-.16.795-.24 1.222-.24.965 0 1.704.415 2.22 1.24l.596-3.497a6.472 6.472 0 0 0-1.249-.441 5.513 5.513 0 0 0-1.257-.142M101.474 11.32c-.46-.092-1.36-.142-2.892.223l.349 2.185h3.025z" />
                                    <path fill="#FFF"
                                        d="m101.35 10.66-.492-2.483c-.731.012-1.647.123-2.784.41-.124.032-.252.065-.382.101-.63.173-1.215.29-1.744.368l1.66-3.267.356 2.186c1.137-.287 2.053-.398 2.785-.411l-1.023-5.108h-3.097l-6.08 11.272h3.025l1.091-2.146c.91-.064 2.014-.223 3.269-.567.182-.05.355-.093.523-.133 1.533-.365 2.432-.314 2.892-.223M116.537 6.871c-2.251.59-3.965.534-4.713.463l-.4 2.423c.213.017.461.03.746.036 1.11.021 2.738-.08 4.701-.595 1.863-.487 2.96-.457 3.524-.364l.402-2.433c-.945-.1-2.322-.037-4.26.47M117.12 2.51c-2.152.562-3.812.537-4.607.472l-.601 3.72c.527.048 1.526.09 2.852-.094l.212-1.297c.743-.09 1.575-.239 2.478-.475 1.986-.519 3.1-.45 3.628-.344l.403-2.44c-.947-.116-2.353-.07-4.366.457M115.994 11.076c-.711.186-1.369.308-1.96.385l.195-1.19a16.13 16.13 0 0 1-2.116.107 13.007 13.007 0 0 1-.733-.035l-.604 3.61c.235.02.519.038.85.045 1.11.02 2.74-.08 4.703-.595 1.756-.46 2.831-.458 3.42-.378l.401-2.428c-.94-.085-2.287-.011-4.156.479M13.32 16a21.931 21.931 0 0 1 2.705-.943 22.178 22.178 0 0 1 8.428-.686v-2.878a25.035 25.035 0 0 0-9.87 1.006c-.246.075-.49.154-.734.24-.48.163-.952.34-1.415.53z" />
                                    <path fill="#FFF"
                                        d="M24.453 2.157v8.618a25.783 25.783 0 0 0-10.837 1.286A34.304 34.304 0 0 1 0 13.842V5.225a25.741 25.741 0 0 0 10.835-1.285 34.33 34.33 0 0 1 13.617-1.781" />
                                    <g>
                                        <mask id="b" fill="#fff">
                                            <use xlink:href="#a" />
                                        </mask>
                                        <path fill="#FFF"
                                            d="M11.132 0a21.931 21.931 0 0 1-2.704.942A22.178 22.178 0 0 1 0 1.628v2.878A25.035 25.035 0 0 0 9.87 3.5a24.633 24.633 0 0 0 2.15-.77z"
                                            mask="url(#b)" />
                                    </g>
                                </g>
                            </svg>
                        </header>
                        <hr>
                        {0}
                    </body>
                    </html>
                    '''.format(
                    html_fig
                )
                with open('{0}/TOP15_tenant_hold_memory.html'.format(local_store_dir), 'w') as f:
                    f.write(html_top15_combined)
        except Exception as e:
            self.stdio.exception('write html result failed, error: {0}'.format(e))
        delete_file(ssh_client, gather_dir_full_path, self.stdio)
        ssh_client.ssh_close()
        return resp

    def __handle_log_list(self, ssh_client, node, resp):
        if self.directly_analyze_files:
            log_list = self.__get_log_name_list_offline()
        else:
            log_list = self.__get_log_name_list(ssh_client, node)
        if len(log_list) > self.file_number_limit:
            self.stdio.warn("{0} The number of log files is {1}, out of range (0,{2}]".format(node.get("ip"), len(log_list), self.file_number_limit))
            resp["skip"] = (True,)
            resp["error"] = "Too many files {0} > {1}, Please adjust the analyze time range".format(len(log_list), self.file_number_limit)
            if self.directly_analyze_files:
                resp["error"] = "Too many files {0} > {1}, " "Please adjust the number of incoming files".format(len(log_list), self.file_number_limit)
            return log_list, resp
        elif len(log_list) == 0:
            self.stdio.warn("{0} The number of observer.log*  files is {1}, No files found".format(node.get("ip"), len(log_list)))
            resp["skip"] = (True,)
            resp["error"] = "No observer.log* found"
            return log_list, resp
        return log_list, resp

    def __get_log_name_list(self, ssh_client, node):
        """
        :param ssh_client:
        :return: log_name_list
        """
        home_path = node.get("home_path")
        log_path = os.path.join(home_path, "log")
        get_oblog = "ls -1 -F %s/*%s.log* |grep -v wf|awk -F '/' '{print $NF}'" % (log_path, self.scope)
        log_name_list = []
        log_files = ssh_client.exec_cmd(get_oblog)
        if log_files:
            log_name_list = get_logfile_name_list(ssh_client, self.from_time_str, self.to_time_str, log_path, log_files, self.stdio)
        else:
            self.stdio.error("Unable to find the log file. Please provide the correct --ob_install_dir, the default is [/home/admin/oceanbase]")
        return log_name_list

    def __get_log_name_list_offline(self):
        """
        :param:
        :return: log_name_list
        """
        log_name_list = []
        if self.analyze_files_list and len(self.analyze_files_list) > 0:
            for path in self.analyze_files_list:
                if os.path.exists(path):
                    if os.path.isfile(path):
                        if os.path.basename(path).startswith('observer.log'):
                            log_name_list.append(path)
                    else:
                        log_names = FileUtil.find_all_file(path)
                        if log_names:
                            filtered_logs = [name for name in log_names if os.path.basename(name).startswith('observer.log')]
                            log_name_list.extend(filtered_logs)
        self.stdio.verbose("get log list {}".format(log_name_list))
        return log_name_list

    def __pharse_log_file(self, ssh_client, node, log_name, gather_path, local_store_dir):
        home_path = node.get("home_path")
        log_path = os.path.join(home_path, "log")
        local_store_path = "{0}/{1}".format(local_store_dir, log_name)
        if self.grep_args is not None:
            grep_cmd = "grep -e '{grep_args}' {log_dir}/{log_name} >> {gather_path}/{log_name} ".format(grep_args=self.grep_args, gather_path=gather_path, log_name=log_name, log_dir=log_path)
            self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
            ssh_client.exec_cmd(grep_cmd)
            log_full_path = "{gather_path}/{log_name}".format(log_name=log_name, gather_path=gather_path)
            download_file(ssh_client, log_full_path, local_store_path, self.stdio)
        else:
            real_time_logs = ["observer.log", "rootservice.log", "election.log", "trace.log", "observer.log.wf", "rootservice.log.wf", "election.log.wf", "trace.log.wf"]
            if log_name in real_time_logs:
                cp_cmd = "cp {log_dir}/{log_name} {gather_path}/{log_name} ".format(gather_path=gather_path, log_name=log_name, log_dir=log_path)
                self.stdio.verbose("copy files, run cmd = [{0}]".format(cp_cmd))
                ssh_client.exec_cmd(cp_cmd)
                log_full_path = "{gather_path}/{log_name}".format(log_name=log_name, gather_path=gather_path)
                download_file(ssh_client, log_full_path, local_store_path, self.stdio)
            else:
                log_full_path = "{log_dir}/{log_name}".format(log_name=log_name, log_dir=log_path)
                download_file(ssh_client, log_full_path, local_store_path, self.stdio)

    def __pharse_offline_log_file(self, ssh_client, log_name, local_store_dir):
        """
        :param ssh_helper, log_name
        :return:
        """

        ssh_client = LocalClient(context=self.context, node={"ssh_type": "local"})
        local_store_path = "{0}/{1}".format(local_store_dir, str(log_name).strip(".").replace("/", "_"))
        if self.grep_args is not None:
            grep_cmd = "grep -e '{grep_args}' {log_name} >> {local_store_path} ".format(grep_args=self.grep_args, log_name=log_name, local_store_path=local_store_path)
            self.stdio.verbose("grep files, run cmd = [{0}]".format(grep_cmd))
            ssh_client.exec_cmd(grep_cmd)
        else:
            download_file(ssh_client, log_name, local_store_path, self.stdio)

    def __parse_memory_label(self, file_full_path):
        ssh_client = LocalClient(context=self.context, node={"ssh_type": "local"})
        if self.version >= '4.3':
            grep_cmd = 'grep -n "memory_dump.*statistics" ' + file_full_path
        elif self.version >= '4.0' and self.version < '4.3':
            grep_cmd = 'grep -n "runTimerTask.*MemDumpTimer" ' + file_full_path
        else:
            grep_cmd = 'grep -n "Run print tenant memstore usage task" ' + file_full_path
        memory_begin_str = ssh_client.exec_cmd(grep_cmd)
        memory_begin_list = memory_begin_str.split('\n')
        memory_print_line_list = []
        for row in memory_begin_list:
            try:
                print_begin_line = row.split(':')[0]
                if print_begin_line:
                    print_begin_line = int(print_begin_line)
                    memory_print_line_list.append(print_begin_line)
            except ValueError:
                continue
            except Exception as e:
                self.stdio.warn('parse memory label failed, error: {0}'.format(e))
                continue
        if len(memory_print_line_list) == 0 and self.directly_analyze_files:
            self.stdio.warn('failed to get memory information. Please confirm that the file:{0} and version:{1} you are passing are consistent'.format(file_full_path, self.version))
        return sorted(memory_print_line_list)

    def __convert_string_bytes_2_int_bytes(self, string_bytes):
        if ',' in string_bytes:
            bytes_list = string_bytes.split(',')
            string_bytes_no_comma = ''.join(bytes_list)
            bytes_int = int(string_bytes_no_comma)
        else:
            bytes_int = int(string_bytes)
        return bytes_int

    def __parse_log_lines(self, file_full_path, memory_dict):
        """
        Process the observer's log line by line
        :param file_full_path
        :return:
        """
        self.stdio.verbose("start parse log {0}".format(file_full_path))
        memory_print_line_list = self.__parse_memory_label(file_full_path)
        tenant_dict = dict()
        if memory_print_line_list:
            with open(file_full_path, 'r', encoding='utf8', errors='replace') as file:
                line_num = 0
                memory_print_begin_line = memory_print_line_list[0]
                memory_print_line_list.remove(memory_print_begin_line)
                try:
                    in_parse_ctx = False
                    ctx_name = None
                    in_parse_module = False
                    ctx_info = None
                    for line in file:
                        line_num = line_num + 1
                        line = line.strip()
                        if line_num < memory_print_begin_line:
                            continue
                        else:
                            if self.version >= '4.3':
                                if 'MemoryDump' in line and 'statistics' in line:
                                    time_str = self.__get_time_from_ob_log_line(line)
                                    memory_print_time = time_str.split('.')[0]
                                    memory_dict[memory_print_time] = dict()
                            elif self.version > '4.0' and self.version < '4.3':
                                if 'runTimerTask' in line and 'MemDumpTimer' in line:
                                    time_str = self.__get_time_from_ob_log_line(line)
                                    memory_print_time = time_str.split('.')[0]
                                    memory_dict[memory_print_time] = dict()
                            else:
                                if 'Run print tenant memstore usage task' in line:
                                    time_str = self.__get_time_from_ob_log_line(line)
                                    memory_print_time = time_str.split('.')[0]
                                    memory_dict[memory_print_time] = dict()
                            if self.version >= '4.3':
                                if 'print_tenant_usage' in line and 'ServerGTimer' in line and 'CHUNK_MGR' in line:
                                    if memory_print_line_list:
                                        memory_print_begin_line = memory_print_line_list[0]
                                        memory_print_line_list.remove(memory_print_begin_line)
                                    else:
                                        break
                            elif self.version >= '4.0' and self.version < '4.3':
                                if 'print_tenant_usage' in line and 'MemDumpTimer' in line and 'CHUNK_MGR' in line:
                                    if memory_print_line_list:
                                        memory_print_begin_line = memory_print_line_list[0]
                                        memory_print_line_list.remove(memory_print_begin_line)
                                    else:
                                        break
                            else:
                                if 'CHUNK_MGR' in line:
                                    if memory_print_line_list:
                                        memory_print_begin_line = memory_print_line_list[0]
                                        memory_print_line_list.remove(memory_print_begin_line)
                                    else:
                                        break
                            if '[MEMORY]' in line or 'MemDump' in line or 'ob_tenant_ctx_allocator' in line:
                                if '[MEMORY] tenant:' in line:
                                    tenant_id = line.split('tenant:')[1].split(',')[0].strip()
                                    if 'rpc_' in line:
                                        hold_bytes = line.split('hold:')[1].split('rpc_')[0].strip()
                                        rpc_hold_bytes = line.split('rpc_hold:')[1].split('cache_hold')[0].strip()
                                        tenant_dict['rpc_hold'] = self.__convert_string_bytes_2_int_bytes(rpc_hold_bytes)
                                    else:
                                        hold_bytes = line.split('hold:')[1].split('cache_')[0].strip()
                                    cache_hold_bytes = line.split('cache_hold:')[1].split('cache_used')[0].strip()
                                    cache_used_bytes = line.split('cache_used:')[1].split('cache_item_count')[0].strip()
                                    cache_item_count = line.split('cache_item_count:')[1].strip()
                                    tenant_dict['hold'] = self.__convert_string_bytes_2_int_bytes(hold_bytes)
                                    tenant_dict['cache_hold'] = self.__convert_string_bytes_2_int_bytes(cache_hold_bytes)
                                    tenant_dict['cache_used'] = self.__convert_string_bytes_2_int_bytes(cache_used_bytes)
                                    tenant_dict['cache_item_count'] = self.__convert_string_bytes_2_int_bytes(cache_item_count)
                                    memory_dict[memory_print_time][tenant_id] = tenant_dict
                                    continue
                                if '[MEMORY] tenant_id=' in line:
                                    if self.version > '4.0':
                                        if not in_parse_ctx:
                                            in_parse_ctx = True
                                        ctx_name = line.split('ctx_id=')[1].split('hold')[0].strip()
                                        hold_bytes = self.__convert_string_bytes_2_int_bytes(line.split('hold=')[1].split('used')[0].strip())
                                        used_bytes = self.__convert_string_bytes_2_int_bytes(line.split('used=')[1].split('limit')[0].strip())
                                        continue
                                    else:
                                        if not in_parse_ctx:
                                            in_parse_ctx = True
                                        ctx_name = line.split('ctx_id=')[1].split('hold')[0].strip()
                                        hold_bytes = self.__convert_string_bytes_2_int_bytes(line.split('hold=')[1].split('used')[0].strip())
                                        used_bytes = self.__convert_string_bytes_2_int_bytes(line.split('used=')[1].split('limit')[0].strip())
                                        if in_parse_ctx:
                                            ctx_info = dict()
                                            ctx_info['ctx_name'] = ctx_name
                                            ctx_info['hold_bytes'] = hold_bytes
                                            ctx_info['used_bytes'] = used_bytes
                                        continue
                                if '[MEMORY] idle_size=' in line:
                                    if in_parse_ctx:
                                        idle_size = self.__convert_string_bytes_2_int_bytes(line.split('idle_size=')[1].split('free_size')[0].strip())
                                        free_size = self.__convert_string_bytes_2_int_bytes(line.split('free_size=')[1].strip())
                                        continue
                                if '[MEMORY] wash_related_chunks=' in line:
                                    if in_parse_ctx:
                                        wash_related_chunks = self.__convert_string_bytes_2_int_bytes(line.split('wash_related_chunks=')[1].split('washed_blocks')[0].strip())
                                        washed_blocks = self.__convert_string_bytes_2_int_bytes(line.split('washed_blocks=')[1].split('washed_size')[0].strip())
                                        washed_size = self.__convert_string_bytes_2_int_bytes(line.split('washed_size=')[1].strip())
                                        ctx_info = dict()
                                        ctx_info['ctx_name'] = ctx_name
                                        ctx_info['hold_bytes'] = hold_bytes
                                        ctx_info['used_bytes'] = used_bytes
                                        ctx_info['idle_size'] = idle_size
                                        ctx_info['free_size'] = free_size
                                        ctx_info['wash_related_chunks'] = wash_related_chunks
                                        ctx_info['washed_blocks'] = washed_blocks
                                        ctx_info['washed_size'] = washed_size
                                        continue
                                if '[MEMORY] hold=' in line:
                                    if not in_parse_module:
                                        in_parse_module = True
                                    mod_name = line.split('mod=')[1].strip()
                                    if mod_name == 'SUMMARY':
                                        mod_hold_bytes = self.__convert_string_bytes_2_int_bytes(line.split('hold=')[1].split('used')[0].strip())
                                        mod_used_bytes = self.__convert_string_bytes_2_int_bytes(line.split('used=')[1].split('count')[0].strip())
                                        mod_used_block_cnt = self.__convert_string_bytes_2_int_bytes(line.split('count=')[1].split('avg_used')[0].strip())
                                        mod_avg_used_bytes = self.__convert_string_bytes_2_int_bytes(line.split('avg_used=')[1].split('mod')[0].strip())
                                    else:
                                        mod_hold_bytes = self.__convert_string_bytes_2_int_bytes(line.split('hold=')[1].split('used')[0].strip())
                                        mod_used_bytes = self.__convert_string_bytes_2_int_bytes(line.split('used=')[1].split('count')[0].strip())
                                        mod_used_block_cnt = self.__convert_string_bytes_2_int_bytes(line.split('count=')[1].split('avg_used')[0].strip())
                                        if self.version > '4.0':
                                            mod_avg_used_bytes = self.__convert_string_bytes_2_int_bytes(line.split('avg_used=')[1].split('block_cnt')[0].strip())
                                            mod_block_cnt = self.__convert_string_bytes_2_int_bytes(line.split('block_cnt=')[1].split('chunk_cnt')[0].strip())
                                            mod_chunk_cnt = self.__convert_string_bytes_2_int_bytes(line.split('chunk_cnt=')[1].split('mod')[0].strip())
                                        else:
                                            mod_avg_used_bytes = self.__convert_string_bytes_2_int_bytes(line.split('avg_used=')[1].split('mod')[0].strip())
                                    mod_info = dict()
                                    mod_info['mod_name'] = mod_name
                                    mod_info['mod_hold_bytes'] = mod_hold_bytes
                                    mod_info['mod_used_bytes'] = mod_used_bytes
                                    mod_info['mod_used_block_cnt'] = mod_used_block_cnt
                                    mod_info['mod_avg_used_bytes'] = mod_avg_used_bytes
                                    if self.version > '4.0':
                                        mod_info['mod_block_cnt'] = mod_block_cnt
                                        mod_info['mod_chunk_cnt'] = mod_chunk_cnt
                                    if 'mod_info' in ctx_info:
                                        ctx_info['mod_info'].append(mod_info)
                                    else:
                                        ctx_info['mod_info'] = []
                                        ctx_info['mod_info'].append(mod_info)
                                if '[MEMORY] hold=' not in line and in_parse_module:
                                    in_parse_module = False
                                if not in_parse_module and in_parse_ctx:
                                    in_parse_ctx = False
                                    if 'ctx_info' in tenant_dict:
                                        tenant_dict['ctx_info'].append(ctx_info)
                                    else:
                                        tenant_dict['ctx_info'] = []
                                        tenant_dict['ctx_info'].append(ctx_info)
                except Exception as e:
                    self.stdio.exception('parse log failed, error: {0}'.format(e))
        self.stdio.verbose("complete parse log {0}".format(file_full_path))
        return

    def __get_time_from_ob_log_line(self, log_line):
        """
        Get the time from the observer's log line
        :param log_line
        :return: time_str
        """
        time_str = ""
        if len(log_line) >= 28:
            time_str = log_line[1 : log_line.find(']')]
        return time_str

    @staticmethod
    def __get_overall_summary(node_summary_tuple):
        """
        generate overall summary from all node summary tuples
        :param node_summary_tuple: (node, is_err, err_msg, size, consume_time, node_summary) for each node
        :return: a string indicating the overall summary
        """
        summary_tab = []
        field_names = ["Node", "Status"]
        field_names.append("Time")
        field_names.append("ResultPath")
        for tup in node_summary_tuple:
            node = tup[0]
            is_err = tup[2]
            consume_time = tup[3]
            pack_path = tup[4] if not is_err else None
            summary_tab.append((node, "Error:" + tup[2] if is_err else "Completed", "{0} s".format(consume_time), pack_path))
        return "\nAnalyze Ob Log Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
