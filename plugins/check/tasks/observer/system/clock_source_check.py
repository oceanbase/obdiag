import re
from src.common.tool import StringUtils
from src.handler.checker.check_task import TaskBase


class ClockSourceCheck(TaskBase):

    def init(self, context, report):
        super().init(context, report)
        self.clock_sources = {}

    def execute(self):
        try:
            cmd = "cat /etc/chrony.conf | grep -v '^#' | grep iburst"

            for node in self.observer_nodes:
                ssh_client = node.get("ssher")
                node_ip = node["ip"]
                output = ssh_client.exec_cmd(cmd)

                sources = []
                for line in output.splitlines():
                    match = re.search(r'server\s+(\S+)\s+iburst', line.strip())
                    if match:
                        sources.append(match.group(1))

                sources_sorted = tuple(sorted(sources))
                if sources_sorted not in self.clock_sources:
                    self.clock_sources[sources_sorted] = []
                self.clock_sources[sources_sorted].append(node_ip)

            most_common_config = max(self.clock_sources.items(), key=lambda x: len(x[1]))[0]
            non_compliant_nodes = []

            for config, ips in self.clock_sources.items():
                if config != most_common_config:
                    non_compliant_nodes.extend(ips)

            if non_compliant_nodes:
                nodes_str = ", ".join(non_compliant_nodes)
                return self.report.add_warning(f"Found nodes with inconsistent clock sources (expected: {list(most_common_config)}): {nodes_str}")
        except Exception as e:
            return self.report.add_fail(f"Execute error: {e}")

    def get_task_info(self):
        return {"name": "clock_source_check", "info": "It is recommended to add inspection items to check whether the configuration file server IP of the ob node clock source is consistent.issue #781"}

    def get_scene_info(self):
        pass


clock_source_check = ClockSourceCheck()
