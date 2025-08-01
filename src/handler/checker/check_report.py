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
@time: 2023/9/26
@file: check_report.py
@desc:
"""

# report module is used to comprehensive system report generated by a single inspection and summarize it through the
# replies of task execution. When report summarizes information, it will be based on the content of the task. The
# first level is task_name. If all tasks are passed, it will be marked as 'pass'. If any task is not passed,
# it will generate separate reports for each step at the second level. The dimension of the second level is step,
# and generally, only the steps with exceptions will be summarized, but it can also handle them as needed.
from prettytable import PrettyTable
import datetime
import os
import yaml
import xmltodict
import json
from io import open

from src.common.command import get_obproxy_version, get_observer_version, get_observer_commit_id
from src.handler.checker.check_exception import CheckException
from src.telemetry.telemetry import telemetry
from jinja2 import Template
from src.common.version import OBDIAG_VERSION


class CheckReport:
    def __init__(self, context, report_target="observer", export_report_path="./check_report/", export_report_type="table"):
        self.context = context
        self.stdio = context.stdio
        self.tasks = []
        self.export_report_path = export_report_path
        try:
            if not os.path.exists(export_report_path):
                os.makedirs(export_report_path)
        except Exception as e:
            self.stdio.error("init check_report {0}".format(e))
            raise CheckrReportException("int check_report {0}".format(e))
        self.export_report_type = export_report_type

        now = datetime.datetime.now()
        date_format = now.strftime("%Y-%m-%d-%H-%M-%S")
        self.report_time = now.strftime("%Y-%m-%d %H:%M:%S")

        file_name = "obdiag_check_report_{0}_".format(report_target) + date_format
        self.report_target = report_target

        report_path = os.path.join(self.export_report_path, file_name)
        self.report_path = report_path
        self.stdio.verbose("export report to {0}".format(report_path))

    def add_task_report(self, task_report):
        self.tasks.append(task_report)

    def export_report(self):
        self.stdio.verbose("export report to {0}.{1}, export type is {1}".format(self.report_path, self.export_report_type))
        try:
            if self.export_report_type == "table":
                self.export_report_table()
            elif self.export_report_type == "json":
                self.export_report_json()
            elif self.export_report_type == "xml":
                self.export_report_xml()
            elif self.export_report_type == "yaml":
                self.export_report_yaml()
            elif self.export_report_type == "html":
                self.export_report_html()
            else:
                raise CheckrReportException("export_report_type: {0} is not support".format(self.export_report_type))
            self.export_report_path = self.export_report_path + "." + self.export_report_type
        except Exception as e:
            self.stdio.error("export_report Exception : {0}".format(e))
            raise CheckrReportException(e)

    def get_report_path(self):
        return self.report_path + "." + self.export_report_type

    def export_report_xml(self):
        allMap = self.report_tobeMap()
        with open(self.report_path + ".xml", 'w', encoding='utf-8') as f:
            allreport = {"report": allMap}
            json_str = json.dumps(allreport)
            xml_str = xmltodict.unparse(json.loads(json_str))
            f.write(xml_str)
            f.close()

    def export_report_yaml(self):
        allMap = self.report_tobeMap()
        with open(self.report_path + ".yaml", 'w', encoding='utf-8') as f:
            yaml.dump(allMap, f)

    def export_report_json(self):
        allMap = self.report_tobeMap()
        self.stdio.verbose("export_report_json allMap: {0}".format(allMap))
        with open(self.report_path + ".json", 'w', encoding='utf-8') as f:
            json.dump(allMap, f, ensure_ascii=False)

    def report_tobeMap(self):
        failMap = {}
        criticalMap = {}
        warningMap = {}
        allInfoMap = {}
        allMap = {}
        for task in self.tasks:
            if len(task.all_fail()) != 0:
                failMap[task.name] = task.all_fail()
            if len(task.all_critical()) != 0:
                criticalMap[task.name] = task.all_critical()
            if len(task.all_warning()) != 0:
                warningMap[task.name] = task.all_warning()
            if len(task.all()) != 0:
                allInfoMap[task.name] = task.all()
            if len(task.all_fail()) == 0 and len(task.all_critical()) == 0 and len(task.all_warning()) == 0:
                allInfoMap[task.name] = ["all pass"]

        allMap["fail"] = failMap
        allMap["critical"] = criticalMap
        allMap["warning"] = warningMap
        allMap["all"] = allInfoMap
        telemetry.push_check_info(self.report_target, {"fail_cases": list(failMap), "critical_cases": list(criticalMap), "warning_cases": list(warningMap)})
        return allMap

    def export_report_table(self):
        try:
            report_fail_tb = PrettyTable(["task", "task_report"])
            report_fail_tb.align["task_report"] = "l"
            report_fail_tb.title = "fail-tasks-report"

            report_critical_tb = PrettyTable(["task", "task_report"])
            report_critical_tb.align["task_report"] = "l"
            report_critical_tb.title = "critical-tasks-report"

            report_warning_tb = PrettyTable(["task", "task_report"])
            report_warning_tb.align["task_report"] = "l"
            report_warning_tb.title = "warning-tasks-report"

            report_all_tb = PrettyTable(["task", "task_report"])
            report_all_tb.align["task_report"] = "l"
            report_all_tb.title = "all-tasks-report"
            self.stdio.verbose("export report start")
            failMap = []
            criticalMap = []
            warningMap = []

            for task in self.tasks:
                if len(task.all_fail()) != 0:
                    report_fail_tb.add_row([task.name, '\n'.join(task.all_fail())])
                    failMap.append(task.name)
                if len(task.all_critical()) != 0:
                    report_critical_tb.add_row([task.name, '\n'.join(task.all_critical())])
                    criticalMap.append(task.name)
                if len(task.all_warning()) != 0:
                    report_warning_tb.add_row([task.name, '\n'.join(task.all_warning())])
                    warningMap.append(task.name)
                if len(task.all()) != 0:
                    report_all_tb.add_row([task.name, '\n'.join(task.all())])
                if len(task.all_fail()) == 0 and len(task.all_critical()) == 0 and len(task.all_warning()) == 0:
                    report_all_tb.add_row([task.name, "all pass"])
            telemetry.push_check_info(self.report_target, {"fail_cases": list(set(failMap)), "critical_cases": list(set(criticalMap)), "warning_cases": list(set(warningMap))})

            fp = open(self.report_path + ".table", 'a+', encoding='utf-8')
            fp.write("obdiag version: {0}\n".format(OBDIAG_VERSION))
            fp.write("report time: {0}\n\n".format(self.report_time))
            if self.report_target == "observer":
                observer_version = get_observer_version(self.context)
                if observer_version:
                    fp.write("observer version: {0}\n".format(observer_version))
                observer_version_commit_id = get_observer_commit_id(self.context)
                if observer_version_commit_id:
                    fp.write("observer commit id: {0}\n".format(observer_version_commit_id))
            elif self.report_target == "obproxy":
                obproxy_version = get_obproxy_version(self.context)
                if obproxy_version:
                    fp.write("obproxy version: {0}\n".format(obproxy_version))

            if len(report_fail_tb._rows) != 0:
                self.stdio.verbose(report_fail_tb)
                fp.write(report_fail_tb.get_string() + "\n")
            if len(report_critical_tb._rows) != 0:
                self.stdio.verbose(report_critical_tb)
                fp.write(report_critical_tb.get_string() + "\n")
            if len(report_warning_tb._rows) != 0:
                self.stdio.verbose(report_warning_tb)
                fp.write(report_warning_tb.get_string() + "\n")
            if len(report_all_tb._rows) != 0:
                self.stdio.verbose(report_all_tb)
                fp.write(report_all_tb.get_string() + "\n")
            fp.close()
            self.stdio.verbose("export report end")
        except Exception as e:
            raise CheckrReportException("export report {0}".format(e))

    def export_report_html(self):
        try:
            html_template_head = """
                <!DOCTYPE html>
                <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>{{ report_title }}</title>
                    <style>
                        body {
                            padding-top: 60px;
                            font: 16px/1.8 -apple-system, blinkmacsystemfont, "Helvetica Neue", helvetica, segoe ui, arial, roboto, "PingFang SC", "miui", "Hiragino Sans GB", "Microsoft Yahei", sans-serif;
                            background: #f4f6fa linear-gradient(180deg, #006aff 0%, #006aff00 100%) no-repeat;
                            background-size: auto 120px;
                        }

                        section {
                            background: #fff;
                            padding: 2em;
                            margin: 0 auto 2em;
                            max-width: 1280px;
                        }

                        header {
                            padding: 1em;
                            margin: -60px auto 0;
                            max-width: 1280px;
                        }

                        header>svg {
                            margin-left: -2em;
                        }

                        .line{ border-bottom:1px solid;}

                        .titleClass {
                            display: block;
                            white-space: nowrap;
                            margin-bottom: 1em;
                            font-weight: 500;
                            font-size: 1.25em;
                            text-align: left;
                            background: transparent;
                        }

                        table {
                            border-collapse: collapse;
                        }

                        th,
                        td {
                            border: 1px solid #f0f0f0;
                            padding: 8px;
                            text-align: left;
                        }

                        td+td {
                            font-family: 'Courier New', 'Consolas';
                        }

                        th {
                            background-color: #fafafa;
                        }
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
            """
            html_template_tail = """
                </body>
                </html>
            """
            html_template_report_info_table = """
                    <section>
                        <table>
                            <div style="font-weight: bold;font-size: 24px;">{{ report_title }}</div>
                            <p class="line"></p>
                            <tr>
                                <th>Report Time</th>
                                <th>obdiag Version</th>
                                <th>OB Cluster Ip</th>
                                <th>OB Version</th>
                                <th>OB commit_id</th>
                            </tr>
                            <tr>
                                <td>{{ report_time }}</td>
                                <td>{{ obdiag_version }}</td>
                                <td>{{ ob_cluster_ip }}</td>
                                <td>{{ ob_version }}</td>
                                <td>{{ ob_commit_id }}</td>
                            </tr>
                        </table>
                    </section>
            """
            html_template_data_table = """
                    <section>
                        <table>
                            <caption class="titleClass">{{ task_name }}</caption>
                            <tr>
                                <th>Id</th>
                                <th>Task</th>
                                <th>Task Report</th>
                            </tr>
                            {% for task in tasks %}
                            <tr id="row-{{ loop.index0 }}">
                                <td>{{ loop.index0 }}</td>
                                <td>{{ task.task }}</td>
                                <td>{{ task.task_report }}</td>
                            </tr>
                            {% endfor %}
                        </table>
                    </section>
            """
            self.stdio.verbose("export report start")
            fail_map_html = []
            critical_map_html = []
            warning_map_html = []
            report_all_html = []

            for task in self.tasks:
                if len(task.all_fail()) != 0:
                    fail_map_html.append({"task": task.name, "task_report": '<br>'.join([item.replace("\\n", "<br>") for item in task.all_fail()])})
                if len(task.all_critical()) != 0:
                    critical_map_html.append({"task": task.name, "task_report": '<br>'.join([item.replace("\\n", "<br>") for item in task.all_critical()])})
                if len(task.all_warning()) != 0:
                    warning_map_html.append({"task": task.name, "task_report": '<br>'.join([item.replace("\\n", "<br>") for item in task.all_warning()])})
                if len(task.all()) != 0:
                    report_all_html.append({"task": task.name, "task_report": '<br>'.join([item.replace("\\n", "<br>") for item in task.all()])})
                if len(task.all_fail()) == 0 and len(task.all_critical()) == 0 and len(task.all_warning()) == 0:
                    report_all_html.append({"task": task.name, "task_report": "all pass"})

            report_title_str = "obdiag Check Report"
            if self.report_target == "observer":
                report_title_str = "obdiag observer Check Report"
            elif self.report_target == "obproxy":
                report_title_str = "obdiag obproxy Check Report"
            fp = open(self.report_path + ".html", 'a+', encoding='utf-8')
            template_head = Template(html_template_head)
            template_table = Template(html_template_data_table)
            fp.write(template_head.render(report_title=report_title_str) + "\n")
            template_report_info_table = Template(html_template_report_info_table)
            cluster_ips = ""
            ob_commit_id = get_observer_commit_id(self.context)
            for server in self.context.cluster_config["servers"]:
                cluster_ips += server["ip"]
                cluster_ips += ";"
            fp.write(
                template_report_info_table.render(report_title=report_title_str, report_time=self.report_time, obdiag_version=OBDIAG_VERSION, ob_cluster_ip=cluster_ips, ob_commit_id=ob_commit_id, ob_version=self.context.cluster_config["version"]) + "\n"
            )

            if len(fail_map_html) != 0:
                rendered_fail_map_html = template_table.render(task_name="Fail Tasks Report", tasks=fail_map_html)
                fp.write(rendered_fail_map_html + "\n")
            if len(critical_map_html) != 0:
                rendered_critical_map_html = template_table.render(task_name="Critical Tasks Report", tasks=critical_map_html)
                fp.write(rendered_critical_map_html + "\n")
            if len(warning_map_html) != 0:
                rendered_warning_map_html = template_table.render(task_name="Warning Tasks Report", tasks=warning_map_html)
                fp.write(rendered_warning_map_html + "\n")
            if len(report_all_html) != 0:
                rendered_report_all_html = template_table.render(task_name="All Tasks Report", tasks=report_all_html)
                fp.write(rendered_report_all_html + "\n")

            template_tail = Template(html_template_tail)
            fp.write(template_tail.render())
            fp.close()
            self.stdio.verbose("export report end")
        except Exception as e:
            raise CheckrReportException("export report {0}".format(e))


class TaskReport:
    def __init__(self, context, task_name, level="normal"):
        self.context = context
        self.stdio = context.stdio
        self.steps = []
        self.name = task_name
        self.level = level
        self.normal = []
        # "warning" usually indicates a non-critical issue;
        # "critical" indicates a serious error that is triggered;
        # "fail" is triggered only when an execution fails.
        self.warning = []

        self.critical = []

        self.fail = []

    def add(self, info, level="normal"):
        self.stdio.verbose("add task_report {0} ,{1}".format(info, level))
        if level == "normal":
            self.add_normal(info)
        elif level == "warning":
            self.add_warning(info)
        elif level == "critical":
            self.add_critical(info)
        elif level == "fail":
            self.add_fail(info)
        else:
            self.stdio.warn("report level is not support: " + str(level))
            self.add_normal(info)

    def add_normal(self, normal):
        self.normal.append("[normal] " + str(normal))
        self.normal = list(set(self.normal))

    def add_warning(self, tip):
        self.warning.append("[warning] " + str(tip))
        self.warning = list(set(self.warning))

    def add_critical(self, critical):
        self.critical.append("[critical] " + str(critical))
        self.critical = list(set(self.critical))

    def add_fail(self, fail):
        self.fail.append("[fail] " + str(fail))
        self.fail = list(set(self.fail))

    def all(self):
        list = self.fail + self.critical + self.warning + self.normal
        return list

    def all_fail(self):
        return self.fail

    def all_critical(self):
        return self.critical

    def all_warning(self):
        return self.warning

    def all_normal(self):
        return self.normal


class CheckrReportException(CheckException):
    def __init__(self, msg=None, obj=None):
        super(CheckrReportException, self).__init__(msg, obj)
