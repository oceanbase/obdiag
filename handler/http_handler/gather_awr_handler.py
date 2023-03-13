#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@time: 2022/6/24
@file: gather_awr_handler.py
@desc:
"""
import os
import threading
import time
import datetime
import tabulate
import requests

from handler.base_http_handler import BaseHttpHandler
from common.obdiag_exception import OBDIAGInvalidArgs, OBDIAGArgsNotFoundException
from common.obdiag_exception import OBDIAGFormatException
from common.logger import logger
from utils.file_utils import mkdir_if_not_exist, size_format, write_result_append_to_file
from utils.time_utils import datetime_to_timestamp
from utils.time_utils import trans_datetime_utc_to_local
from utils.time_utils import timestamp_to_filename_time
from utils.time_utils import parse_time_length_to_sec
from utils.time_utils import get_time_rounding
from utils.time_utils import parse_time_str
from ocp import ocp_api
from ocp import ocp_task
from ocp import ocp_cluster
from ocp import ocp_base


class GatherAwrHandler(BaseHttpHandler):
    def __init__(self, ocp, gather_pack_dir, gather_timestamp):
        super(GatherAwrHandler, self).__init__(ocp)
        self.ocp = ocp
        self.gather_pack_dir = gather_pack_dir
        self.gather_timestamp = gather_timestamp

    def handle(self, args):
        """
        the overall handler for the gather command
        :param args: command args
        :return: the summary should be displayed
        """
        # check args first
        if not self.__check_valid_and_parse_args(args):
            raise OBDIAGInvalidArgs("Invalid args, args={0}".format(args))
        # example of the format of pack dir for this command: (gather_pack_dir)/gather_pack_20190610123344
        pack_dir_this_command = os.path.join(self.gather_pack_dir,
                                             "gather_pack_{0}".format(timestamp_to_filename_time(
                                                 self.gather_timestamp)))
        logger.info("Use {0} as pack dir.".format(pack_dir_this_command))
        mkdir_if_not_exist(pack_dir_this_command)
        gather_tuples = []
        gather_pack_path_dict = {}

        def handle_awr_from_ocp(ocp_url, cluster_name, arg):
            """
            handler awr from ocp
            :param args: ocp url, ob cluster name, command args
            :return:
            """
            st = time.time()
            # step 1: generate awr report
            report_name = self.__generate_awr_report(arg)

            # step 2: get awr report_id
            report_id = self.__get_awr_report_id(report_name)

            # step 3: hand gather report from ocp
            resp = self.__download_report(pack_dir_this_command, report_name, report_id)
            if resp["skip"]:
                return
            if resp["error"]:
                gather_tuples.append((ocp_url, True,
                                      resp["error_msg"], 0, int(time.time() - st),
                                      "Error:{0}".format(resp["error_msg"]), ""))
                return
            gather_pack_path_dict[(cluster_name, ocp_url)] = resp["gather_pack_path"]
            gather_tuples.append((cluster_name, False, "",
                                  os.path.getsize(resp["gather_pack_path"]),
                                  int(time.time() - st), resp["gather_pack_path"]))

        ocp_threads = [threading.Thread(None, handle_awr_from_ocp(self.ocp_url, self.cluster_name, args), args=())]
        list(map(lambda x: x.start(), ocp_threads))
        list(map(lambda x: x.join(), ocp_threads))
        summary_tuples = self.__get_overall_summary(gather_tuples)
        print(summary_tuples)
        # 将汇总结果持久化记录到文件中
        write_result_append_to_file(os.path.join(pack_dir_this_command, "result_summary.txt"), summary_tuples)

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

        logger.info(
            "Sending Status Request to cluster {0} ...".format(self.cluster_name))

        path = ocp_api.cluster + "/%s/performance/workload/reports/%s" % (self.cluster_id, report_id)
        save_path = os.path.join(store_path, name + ".html")
        pack_path = self.download(self.ocp_url + path, save_path, self.auth)
        logger.info(
            "cluster {0} response. analysing...".format(self.cluster_name))

        resp["gather_pack_path"] = pack_path
        if resp["error"]:
            return resp
        return resp

    def __generate_awr_report(self, args):
        """
        call OCP API to generate awr report
        :param args: command args
        :return: awr report name
        """
        snapshot_list = self.__get_snapshot_list(args)
        if len(snapshot_list) <= 1:
            raise Exception("AWR report at least need 2 snapshot, cluster now only have %s", len(snapshot_list))
        else:
            start_sid, start_time = snapshot_list[0]
            end_sid, end_time = snapshot_list[-1]

        path = ocp_api.cluster + "/%s/performance/workload/reports" % self.cluster_id

        start_time = datetime.datetime.strptime(trans_datetime_utc_to_local(start_time.split(".")[0]),
                                                "%Y-%m-%d %H:%M:%S")
        end_time = datetime.datetime.strptime(trans_datetime_utc_to_local(end_time.split(".")[0]),
                                              "%Y-%m-%d %H:%M:%S")
        params = {
            "name": "OBAWR_obcluster_%s_%s_%s" % (
                self.cluster_name, start_time.strftime("%Y%m%d%H%M%S"), end_time.strftime("%Y%m%d%H%M%S")),
            "startSnapshotId": start_sid,
            "endSnapshotId": end_sid
        }

        response = requests.post(self.ocp_url + path, auth=self.auth, data=params)

        task_instance_id = response.json()["data"]["taskInstanceId"]
        task_instance = ocp_task.Task(self.ocp_url, self.auth, task_instance_id)
        # 生成awr报告是触发了一个任务，需要等待任务完成
        ocp_task.Task.wait_done(task_instance)
        return response.json()["data"]["name"]

    def __get_snapshot_list(self, args):
        """
        get snapshot list from ocp
        :param args: command args
        :return: list
        """
        snapshot_id_list = []
        path = ocp_api.cluster + "/%s/performance/workload/snapshots" % self.cluster_id
        response = requests.get(self.ocp_url + path, auth=self.auth)
        from_datetime_timestamp = datetime_to_timestamp(self.from_time_str)
        to_datetime_timestamp = datetime_to_timestamp(self.to_time_str)
        # 如果用户给定的时间间隔不足一个小时，为了能够获取到snapshot，需要将时间进行调整
        if from_datetime_timestamp + 3600000000 >= to_datetime_timestamp:
            # 起始时间取整点
            from_datetime_timestamp = datetime_to_timestamp(get_time_rounding(dt=parse_time_str(self.from_time_str), step=0, rounding_level="hour"))
            # 结束时间在起始时间的基础上增加一个小时零三分钟(三分钟是给的偏移量，确保能够获取到快照)
            to_datetime_timestamp = from_datetime_timestamp + 3600000000 + 3*60000000
        for info in response.json()["data"]["contents"]:
            try:
                snapshot_time = datetime_to_timestamp(
                    trans_datetime_utc_to_local(str(info["snapshotTime"]).split(".")[0]))
                if from_datetime_timestamp <= snapshot_time <= to_datetime_timestamp:
                    snapshot_id_list.append((info["snapshotId"], info["snapshotTime"]))
            except:
                logger.error("get snapshot failed, pass")
        logger.info("get snapshot list {0}".format(snapshot_id_list))
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

    def __check_valid_and_parse_args(self, args):
        """
        chech whether command args are valid. If invalid, stop processing and print the error to the user
        :param args: command args
        :return: boolean. True if valid, False if invalid.
        """
        if getattr(args, "cluster_name") is not None:
            # 1: cluster_name must be must be provided, if not be valid
            try:
                self.cluster_name = getattr(args, "cluster_name")[0]
            except OBDIAGArgsNotFoundException:
                logger.error("Error: cluster_name must be must be provided")
                return False

            try:
                ocp_base_init = ocp_base.OcpBase(self.ocp_url, self.ocp_user, self.ocp_password)
                ocp_base_init.check_ocp_site()
            except Exception as e:
                raise Exception("check login ocp failed, please check whether conf/config.yml is set correctly"
                                .format(e))

            # 2. get cluster id from ocp
            try:
                self.ob_cluster = ocp_cluster.ObCluster(self.ocp_url, self.auth, None)
                self.cluster_id = self.ob_cluster.get_cluster_id_by_name(getattr(args, "cluster_name"))
            except Exception as e:
                raise Exception("get cluster id from ocp failed, Exception:{0}, please check cluster_name".format(e))
            # 3: to timestamp must be larger than from timestamp, otherwise be valid
        if getattr(args, "from") is not None and getattr(args, "to") is not None:
            try:
                self.from_time_str = getattr(args, "from")
                self.to_time_str = getattr(args, "to")
                from_timestamp = datetime_to_timestamp(getattr(args, "from"))
                to_timestamp = datetime_to_timestamp(getattr(args, "to"))
            except OBDIAGFormatException:
                logger.error("Error: Datetime is invalid. Must be in format yyyy-mm-dd hh:mm:ss. " \
                             "from_datetime={0}, to_datetime={1}".format(getattr(args, "from"), args.to))
                return False
            if to_timestamp <= from_timestamp:
                logger.error("Error: from datetime is larger than to datetime, please check.")
                return False
        elif (getattr(args, "from") is None or getattr(args, "to") is None) and args.since is not None:
            # 3: the format of since must be 'n'<m|h|d>
            try:
                since_to_seconds = parse_time_length_to_sec(args.since)
            except ValueError:
                logger.error("Error: the format of since must be 'n'<m|h|d>")
                return False
            now_time = datetime.datetime.now()
            self.to_time_str = now_time.strftime('%Y-%m-%d %H:%M:%S')
            if since_to_seconds < 3600:
                since_to_seconds = 3600
            self.from_time_str = (now_time - datetime.timedelta(seconds=since_to_seconds)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            raise OBDIAGInvalidArgs(
                "Invalid args, you need input since or from and to datetime, args={0}".format(args))
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
            format_file_size = size_format(file_size, output_str=True)
            summary_tab.append((cluster, "Error" if is_err else "Completed",
                                format_file_size, "{0} s".format(int(consume_time)), pack_path))
        return "\nGather AWR Summary:\n" + \
               tabulate.tabulate(summary_tab, headers=field_names, tablefmt="grid", showindex=False)
