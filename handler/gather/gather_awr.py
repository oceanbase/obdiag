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
@time: 2022/6/24
@file: gather_awr.py
@desc:
"""
import os
import threading
import time
import datetime
import tabulate
import requests
from common.obdiag_exception import OBDIAGFormatException
from common.tool import DirectoryUtil
from common.tool import FileUtil
from common.tool import Util
from common.tool import TimeUtils
from common.ocp import ocp_task, ocp_api


class GatherAwrHandler(object):
    def __init__(self, context, gather_pack_dir='./'):
        self.context = context
        self.stdio = context.stdio
        self.gather_pack_dir = gather_pack_dir
        self.cluster_name = None
        self.cluster_id = None
        if self.context.get_variable("gather_timestamp", None):
            self.gather_timestamp = self.context.get_variable("gather_timestamp")
        else:
            self.gather_timestamp = TimeUtils.get_current_us_timestamp()

    def init_config(self):
        ocp = self.context.ocp_config
        self.ocp_user = ocp["user"]
        self.ocp_password = ocp["password"]
        self.ocp_url = ocp["url"]
        self.auth = (self.ocp_user, self.ocp_password)
        return True

    def handle(self):
        if not self.init_option():
            self.stdio.error('init option failed')
            return False
        if not self.init_config():
            self.stdio.error('init config failed')
            return False
        # example of the format of pack dir for this command: (gather_pack_dir)/gather_pack_20190610123344
        pack_dir_this_command = os.path.join(self.gather_pack_dir, "gather_pack_{0}".format(TimeUtils.timestamp_to_filename_time(self.gather_timestamp)))
        self.stdio.verbose("Use {0} as pack dir.".format(pack_dir_this_command))
        DirectoryUtil.mkdir(path=pack_dir_this_command, stdio=self.stdio)
        gather_tuples = []
        gather_pack_path_dict = {}

        def handle_awr_from_ocp(ocp_url, cluster_name):
            """
            handler awr from ocp
            :param args: ocp url, ob cluster name, command args
            :return:
            """
            st = time.time()
            # step 1: generate awr report
            self.stdio.start_loading('generate awr report')
            report_name = self.__generate_awr_report()
            if report_name:
                self.stdio.stop_loading('generate awr report sucess')
            else:
                self.stdio.stop_loading('generate awr failed')
                return

            # step 2: get awr report_id
            report_id = self.__get_awr_report_id(report_name)

            # step 3: hand gather report from ocp
            resp = self.__download_report(pack_dir_this_command, report_name, report_id)
            if resp["skip"]:
                return
            if resp["error"]:
                gather_tuples.append((ocp_url, True, resp["error_msg"], 0, int(time.time() - st), "Error:{0}".format(resp["error_msg"]), ""))
                return
            gather_pack_path_dict[(cluster_name, ocp_url)] = resp["gather_pack_path"]
            gather_tuples.append((cluster_name, False, "", os.path.getsize(resp["gather_pack_path"]), int(time.time() - st), resp["gather_pack_path"]))

        ocp_threads = [threading.Thread(None, handle_awr_from_ocp(self.ocp_url, self.cluster_name), args=())]
        list(map(lambda x: x.start(), ocp_threads))
        list(map(lambda x: x.join(), ocp_threads))
        summary_tuples = self.__get_overall_summary(gather_tuples)
        self.stdio.print(summary_tuples)
        # 将汇总结果持久化记录到文件中
        FileUtil.write_append(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)

        return gather_tuples, gather_pack_path_dict

    def __download_report(self, store_path, name, report_id):
        """
        the handler for one ocp
        :param args: command args
        :param target_ocp: the agent object
        :return: a resp dict, indicating the information of the response
        """
        resp = {
            "skip": False,
            "error": False,
        }

        self.stdio.verbose("Sending Status Request to cluster {0} ...".format(self.cluster_name))

        path = ocp_api.cluster + "/%s/performance/workload/reports/%s" % (self.cluster_id, report_id)
        save_path = os.path.join(store_path, name + ".html")
        self.stdio.start_loading('download AWR report')
        pack_path = self.download(self.ocp_url + path, save_path, self.auth)
        self.stdio.stop_loading('download AWR report')
        self.stdio.verbose("cluster {0} response. analysing...".format(self.cluster_name))

        resp["gather_pack_path"] = pack_path
        if resp["error"]:
            return resp
        return resp

    def download(self, url, as_file_path, auth, timeout=300):
        with open(as_file_path, "wb") as write_fd:
            write_fd.write(requests.get(url, auth=auth, timeout=timeout).content)
        return as_file_path

    def __generate_awr_report(self):
        """
        call OCP API to generate awr report
        :param args: command args
        :return: awr report name
        """
        snapshot_list = self.__get_snapshot_list()
        if len(snapshot_list) <= 1:
            self.stdio.warn("AWR report at least need 2 snapshot, cluster now only have %s, please adjusted to --from/to or --since", len(snapshot_list))
            return None
        else:
            start_sid, start_time = snapshot_list[0]
            end_sid, end_time = snapshot_list[-1]

        path = ocp_api.cluster + "/%s/performance/workload/reports" % self.cluster_id

        start_time = datetime.datetime.strptime(TimeUtils.trans_datetime_utc_to_local(start_time.split(".")[0]), "%Y-%m-%d %H:%M:%S")
        end_time = datetime.datetime.strptime(TimeUtils.trans_datetime_utc_to_local(end_time.split(".")[0]), "%Y-%m-%d %H:%M:%S")
        params = {"name": "OBAWR_obcluster_%s_%s_%s" % (self.cluster_name, start_time.strftime("%Y%m%d%H%M%S"), end_time.strftime("%Y%m%d%H%M%S")), "startSnapshotId": start_sid, "endSnapshotId": end_sid}

        response = requests.post(self.ocp_url + path, auth=self.auth, data=params)

        task_instance_id = response.json()["data"]["taskInstanceId"]
        task_instance = ocp_task.Task(self.ocp_url, self.auth, task_instance_id)
        # 生成awr报告是触发了一个任务，需要等待任务完成
        ocp_task.Task.wait_done(task_instance)
        return response.json()["data"]["name"]

    def __get_snapshot_list(self):
        """
        Retrieves the snapshot list from the OCP (OpenShift Container Platform).
        :return: A list containing snapshot IDs and their corresponding timestamps.
        """
        snapshot_id_list = []
        path = ocp_api.cluster + "/%s/performance/workload/snapshots" % self.cluster_id
        try:
            response = requests.get(self.ocp_url + path, auth=self.auth)
            # Validate the response status code
            response.raise_for_status()
            from_datetime_timestamp = TimeUtils.datetime_to_timestamp(self.from_time_str)
            to_datetime_timestamp = TimeUtils.datetime_to_timestamp(self.to_time_str)

            # If the user-specified time interval is less than one hour,
            # adjust the times to ensure snapshots can be retrieved.
            if from_datetime_timestamp + 3 * 3600000000 >= to_datetime_timestamp:
                # Round the start time to the nearest hour
                from_datetime_timestamp = TimeUtils.datetime_to_timestamp(TimeUtils.get_time_rounding(dt=TimeUtils.parse_time_str(self.from_time_str), step=0, rounding_level="hour"))

                # Set the end time to one hour and three minutes after the rounded start time
                # (the three-minute offset ensures snapshots can be obtained)
                to_datetime_timestamp = from_datetime_timestamp + 3 * 3600000000 + 3 * 60000000

            if "data" not in response.json() or "contents" not in response.json()["data"]:
                raise ValueError("Invalid response structure. Missing 'data' or 'contents' key.")

            for info in response.json()["data"]["contents"]:
                try:
                    snapshot_time = TimeUtils.datetime_to_timestamp(TimeUtils.trans_datetime_utc_to_local(str(info["snapshotTime"]).split(".")[0]))
                    if from_datetime_timestamp <= snapshot_time <= to_datetime_timestamp:
                        snapshot_id_list.append((info["snapshotId"], info["snapshotTime"]))
                except KeyError:
                    self.stdio.error(f"Malformed snapshot data: {info}")
                    continue
                except Exception as e:
                    continue
        except requests.exceptions.RequestException as e:
            self.stdio.error(f"Failed to fetch snapshot list from OCP: {e}")
            return []
        self.stdio.verbose(f"Retrieved snapshot list: {snapshot_id_list}")
        return snapshot_id_list

    def __get_awr_report_id(self, report_name):
        """
        get awr report from ocp
        :param args: awr report name
        :return: int
        """
        path = ocp_api.cluster + "/%s/performance/workload/reports" % self.cluster_id
        response = requests.get(self.ocp_url + path, auth=self.auth)
        for info in response.json()["data"]["contents"]:
            if info["name"] == report_name:
                return info["id"]
        return 0

    def init_option(self):
        options = self.context.options
        store_dir_option = Util.get_option(options, 'store_dir')
        from_option = Util.get_option(options, 'from')
        to_option = Util.get_option(options, 'to')
        since_option = Util.get_option(options, 'since')
        cluster_name_option = Util.get_option(options, 'cluster_name')
        if cluster_name_option:
            self.cluster_name = cluster_name_option
        else:
            self.stdio.error("--cluster_name option need provided")
            return False
        cluster_id_option = Util.get_option(options, 'cluster_id')
        if cluster_id_option:
            self.cluster_id = cluster_id_option
        else:
            self.stdio.error("--cluster_id option need provided")
            return False
        if from_option is not None and to_option is not None:
            try:
                self.from_time_str = from_option
                self.to_time_str = to_option
                from_timestamp = TimeUtils.datetime_to_timestamp(from_option)
                to_timestamp = TimeUtils.datetime_to_timestamp(to_option)
            except OBDIAGFormatException:
                self.stdio.error("Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. " "from_datetime={0}, to_datetime={1}".format(from_option, to_option))
                return False
            if to_timestamp <= from_timestamp:
                self.stdio.error("Error: from datetime is larger than to datetime, please check.")
                return False
            self.stdio.print('gather log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        elif (from_option is None or to_option is None) and since_option is not None:
            # the format of since must be 'n'<m|h|d>
            try:
                since_to_seconds = TimeUtils.parse_time_length_to_sec(since_option)
            except ValueError:
                self.stdio.error("Error: the format of since must be 'n'<m|h|d>")
                return False
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            if since_to_seconds < 3 * 3600:
                self.stdio.warn('The --since requires a value greater than 3h. your provided is less than 3h, adjusted to 3h.')
                since_to_seconds = 3 * 3600
            self.from_time_str = (now_time - datetime.timedelta(seconds=since_to_seconds)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        else:
            self.stdio.warn('No time option provided, default processing is based on the last 3 h')
            now_time = datetime.datetime.now()
            self.to_time_str = (now_time + datetime.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S')
            self.from_time_str = (now_time - datetime.timedelta(seconds=3 * 3600)).strftime('%Y-%m-%d %H:%M:%S')
            self.stdio.print('gather log from_time: {0}, to_time: {1}'.format(self.from_time_str, self.to_time_str))
        if store_dir_option and store_dir_option != "./":
            if not os.path.exists(os.path.abspath(store_dir_option)):
                self.stdio.warn('warn: args --store_dir [{0}] incorrect: No such directory, Now create it'.format(os.path.abspath(store_dir_option)))
                os.makedirs(os.path.abspath(store_dir_option))
            self.gather_pack_dir = os.path.abspath(store_dir_option)
        return True

    @staticmethod
    def __get_overall_summary(node_summary_tuple):
        """
        generate overall summary from ocp summary tuples
        :param ocp_summary_tuple: (cluster, is_err, err_msg, size, consume_time)
        :return: a string indicating the overall summary
        """
        summary_tab = []
        field_names = ["Cluster", "Status", "Size", "Time", "PackPath"]
        for tup in node_summary_tuple:
            cluster = tup[0]
            is_err = tup[2]
            file_size = tup[3]
            consume_time = tup[4]
            pack_path = tup[5]
            format_file_size = FileUtil.size_format(num=file_size, output_str=True)
            summary_tab.append((cluster, "Error" if is_err else "Completed", format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather AWR Summary:\n" + tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
