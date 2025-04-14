import re
from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class ClogSyncTimeWarnThreshold(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        self.expected_value = "100ms"  # expected_value
        self.param_name = "clog_sync_time_warn_threshold"  # param_name

    def execute(self):
        try:
            # check clog_sync_time_warn_threshold
            if self.ob_connector is None:
                return self.report.add_fail("can't build obcluster connection")
            sql = "select * from oceanbase.GV$OB_PARAMETERS where name='{0}';".format(self.param_name)
            result = self.ob_connector.execute_sql_return_cursor_dictionary(sql).fetchall()
            if not result:
                return self.report.add_warning("can't find this  param_name")
            # gather svr_ip
            non_compliant_nodes = []

            for data_item in result:
                svr_ip = data_item['SVR_IP']
                value = data_item['VALUE']
                num_value = int(re.search(r'\d+', value).group())
                expected_num = int(re.search(r'\d+', self.expected_value).group())
                if num_value > expected_num:
                    non_compliant_nodes.append(svr_ip)
            if non_compliant_nodes:
                nodes_str = ", ".join(non_compliant_nodes)
                return self.report.add_warning(f"this server's clog_sync_time_warn_threshold>100ms, please check: {nodes_str}")
        except Exception as e:
            self.stdio.error("execute error {0}".format(e))
            return self.report.add_fail("execute error {0}".format(e))

    def get_task_info(self):
        return {"name": "clog_sync_time_warn_threshold", "info": "Check the clog synchronization time warning threshold. If the synchronization time exceeds this value, a WARN log is generated. It is recommended to set it to 100ms. issue#793"}

    def get_scene_info(self):
        pass


clog_sync_time_warn_threshold = ClogSyncTimeWarnThreshold()
