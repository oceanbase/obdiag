import os

from prettytable import PrettyTable
from textwrap import fill

from common.logger import logger


class scene_base:
    def __init__(self):
        self.env = None
        self.observer_nodes = None
        self.ob_cluster = None
        self.result_path = None
        self.cluster = None
        self.obproxy_nodes = None
        self.Result = Result()

    def init(self, cluster, nodes, obproxy_nodes, env, result_path):
        self.cluster = cluster
        self.obproxy_nodes = obproxy_nodes
        self.observer_nodes = nodes
        self.env = env
        self.ob_cluster = cluster
        self.Result.set_save_path(result_path)
        pass

    def info(self):
        pass

    def execute(self):
        pass


class Result:

    def __init__(self):
        # self.suggest = ""
        self.procedure = None
        self.records = []
        self.save_path = "./"

    def set_save_path(self, save_path):
        self.save_path = os.path.expanduser(save_path)
        if os.path.exists(save_path):
            self.save_path = save_path
        else:
            os.makedirs(save_path)
            self.save_path = save_path
        logger.info("rca result save_path is :{0}".format(self.save_path))

    def export(self):
        record_file_name = "{0}/{1}".format(self.save_path, "record")
        logger.info("save record to {0}".format(record_file_name))
        with open(record_file_name, 'w') as f:
            for record in self.records:
                record_data = record.export_record()
                f.write(record_data.get_string())
                f.write("\n")
                f.write(record.export_suggest())
                f.write("\n")


class RCA_ResultRecord:
    def __init__(self):
        self.records = []
        self.suggest = "The suggest: "

    def add_record(self, record):
        logger.info("add_record:{0}".format(record))
        self.records.append(record)

    def add_suggest(self, suggest):
        logger.info("add_suggest:{0}".format(suggest))
        self.suggest += suggest

    def export_suggest(self):
        return self.suggest

    def export_record(self):
        record_tb = PrettyTable(["step", "info"])
        record_tb.align["info"] = "l"
        record_tb.title = "record"
        i = 0
        while i < len(self.records):
            record_tb.add_row([i + 1, fill(self.records[i], width=100)])
            i += 1
        logger.debug(record_tb)
        return record_tb
