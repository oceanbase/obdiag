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
@time: 2022/11/29
@file: html_meta.py
@desc:
"""


class GlobalHtmlMeta:
    _html_dict = {}

    def _init(self):
        global _html_dict
        self._sql_dict = {}

    def set_value(self, key, value):
        self._html_dict[key] = value

    def get_value(self, key):
        try:
            return self._html_dict[key]
        except:
            print('get' + key + 'failed\r\n')

    def rm_value(self, key):
        try:
            return self._html_dict.pop(key)
        except:
            print('delete' + key + 'failed\r\n')


html_dict = GlobalHtmlMeta()

html_dict.set_value(
    "sql_plan_monitor_report_header",
    '''
    <!doctype html>
    <html lang="en">
      <head>
        <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="./resources/web/bootstrap.min.css" >
    <script src="./resources/web/jquery-3.2.1.min.js" ></script>
    <script src="./resources/web/popper.min.js"></script>
    <script src="./resources/web/bootstrap.min.js" ></script>
    <script>window.jQuery || alert('请将 sql_plan_monitor_report.html 文件放到 resources 相同目录后再访问。否则无法显示可视化图表。'); </script>
    <style>
    body{ padding:10px; padding-bottom:50px;}
    table {font-family: Consolas,"Courier New",Courier,FreeMono,monospace !important;}
    .fixed{ position:fixed; right:20px; bottom:0px; width:200px; height:50px; background-color:#fef8e9; z-index:9999;border-radius:10px;padding:5px;margin: 0 auto;}
    h2 { text-decoration:underline;color:blue; cursor:pointer; margin-bottom:20px;}
    .diff {display:none;margin-left:20px;padding:5px;background-color:#fef8e9;color:black;border-radius:5px;position:absolute;}
    .graph-table tr {padding:0px;line-height:0.4em;}
    .graph-table>tr>td, .graph-table>tr>th {padding:0px !important;line-height:0.4em !important;vertical-align:middle !important;}
    .graph-table>tr>.lastline {padding:0px !important;line-height:1.4em !important;vertical-align:middle !important;}
    .graph-table {font-size:10px;line-height:0.6em;width:1000px;}
    *{margin: 0; padding: 0;}
    .b {height: 14px;}
    .c {background-color:#ffcc66;}
    .empty { height: 14px; background: rgba(200,200,200,0.2);}
    .bar { margin-left:5%; margin-top: 20px; margin-bottom:100px; }
    .help {background-color:#fef8e9;width:100%;border-left:6px solid orange;padding:30px 20px;}
    .shortcut {color:gray;text-align:right;}
    </style>
    </head>
    <body>
    <div class='help'><h1>SQL Monitor Report</h1></div>
    <script>
    $(function() {
      $('table').addClass('table-bordered');
      $('.v table').addClass('table table-bordered table-striped');
      $('#schema_anchor').click(function() {
        $('#schema').toggle();
      });
      $('#agg_table_anchor').click(function() {
        $('#agg_table').toggle();
      });
      $('#svr_agg_table_anchor').click(function() {
        $('#svr_agg_table').toggle();
      });
      $('#detail_table_anchor').click(function() {
        $('#detail_table').toggle();
      });
      $('#sql_audit_table_anchor').click(function() {
        $('#sql_audit_table').toggle();
      });
      setTimeout(function() {
        $('#debug').hide();
      }, 30*1000);

    });

    //获取随机安全色
    function getSafeColor(n) {
      var base = ['00','33','66','99','CC','FF'];     //基础色代码
      var len = base.length;                          //基础色长度
      var bg = new Array();                           //返回的结果
      var random = Math.ceil( n * 17 % 200 + 13);    //获取1-216之间的随机数
      var res;
      for(var r = 0; r  <  len; r++){
        for(var g = 0; g  <  len; g++){
          for(var b = 0; b  <  len; b++){
            bg.push('#'+base[r].toString()+base[g].toString()+base[b].toString());
          }
        };
      };
      for(var i=0;i < bg.length;i++){
        res =  bg[random];
      }
      return res;
    }

    var colors = [];
    for (var n = 0; n < 1000; ++n) {
      colors[n] = getSafeColor(n);
    }

    function padding(n) {
      return "";
    }

    function generate_graph(type, serial, topnode) {
      var max = 0;
      var min = 999999999999999;
      for (var i = 0; i < serial.length; ++i) {
        if (serial[i].start > 0) {
          max = Math.max(max, serial[i].end);
          min = Math.min(min, serial[i].start);
        }
      }

      var total = max - min;

      // normalize

      for (var i = 0; i < serial.length; ++i) {
         if (serial[i].start > 0) {
            serial[i].start_relative = serial[i].start - min;
            serial[i].length = serial[i].end - serial[i].start;

            serial[i].a = Math.round(serial[i].start_relative * 100 / total);
            serial[i].b = Math.max(0.1, Math.round(serial[i].length * 100 / total));
            serial[i].c = Math.round(100 - serial[i].a - serial[i].b);
         }
      }
       console.log(topnode, "my", serial);


      var c1 = (undefined == serial[0] || serial[0].tag == 'op') ? '线程ID' : '线程数';
      var ext_header = "";
      var ext_footer = "";
      switch(type) {
      case "dfo":
        ext_header = "<td width='5%'>估行</td>";
        ext_footer = "<td></td>";
        break;
      case "detail":
        ext_header = "<td width='5%'>RESCAN</td>";
        ext_footer = "<td></td>";
        break;
      default:
        break;
      }
      var table = "<table class='graph-table'><tr class='lastline' style='line-height:1.4em;'><td class='b'>" + c1 + "</td><td>算子</td>" + ext_header + "<td>吐行</td><td>执行吐行时间线</td><td>Ext</td></tr>";
      for (var i = 0; i < serial.length - 1; ++i) {
        var ext_data = "";
        ext_data += (serial[i].est_rows === undefined ? "" : "<td>" + serial[i].est_rows + "</td>");
        ext_data += (serial[i].rescan === undefined ? "" : "<td>" + serial[i].rescan + "</td>");
        var row = "<tr><td width='5%'>" +  serial[i].tid + "</td><td width='10%'>" + "&nbsp;".repeat(serial[i].depth) + serial[i].op + "(" + serial[i].opid + ")</td>" + ext_data + "<td width='5%' style='text-align:right'>" +  serial[i].rows + "</td>";
        if (serial[i].start > 0) {
          row += "<td width='80%'><div tabindex='" + i + "' class='b graphrow' data-toggle='popover' data-trigger='focus' data-placement='bottom' data-content='" + JSON.stringify(serial[i]).replace(/,/g,'\\n') + "' style='margin-left:" + serial[i].a + "%;width:" + serial[i].b + "%;background-color:" + colors[serial[i].opid] + "' title='" + serial[i].op + "(" + serial[i].diff + "s, " + serial[i].svr_ip +")'><span class='diff'>" +  serial[i].op  + "(" + serial[i].opid + ")" + ' ' + serial[i].diff + "s</span></div></td>";
        } else {
          row += "<td><div class='empty' style='width:100%;'></div></td>";
        }
        // extra data
        if (undefined !== serial[i].otherstat) {
          if (serial[i].otherstat.length <= 1) {
            row += "<td></td>";
          } else {
            row += "<td width='500px'><div tabindex='" + i + "' class='b graphrow c' data-html='true' data-toggle='popover' data-trigger='focus' data-placement='bottom' data-content='" + serial[i].otherstat  + "'><span></span></div></td>";
          }
        } else {
          row += "<td></td>";
        }
        row += "</tr>";
        table += row;
      }
      table += "<tr><td class='lastline'><button style='line-height:1.4em' class='enlarge'>缩放</button></td><td></td>"+ext_footer+"<td>总时间</td><td class='b' style='text-align:center;'>" + (Math.round(total * 1000000) / 1000000.0) + "s</td></tr></table>"
      topnode.get(0).innerHTML = table;
    }
    </script>
    ''',
)

html_dict.set_value(
    "sql_plan_monitor_report_footer",
    '''
        <script>
        generate_graph("detail", detail_serial_v1, $('#detail_serial_v1'));
        generate_graph("detail", detail_serial_v2, $('#detail_serial_v2'));

        $(function () {
          $('.b').popover({ trigger: 'focus' })
          $('.graphrow').mouseover(function(){
            $(this).find('span').show();
          });
          $('.graphrow').mouseleave(function(){
            $(this).find('span').hide();
          });
        })

        $(function() {
          $('.enlarge').click(function(e) {
            if ($(e.target).parents('.graph-table').css('width') == '1000px') {
              $(e.target).parents('.graph-table').css('width', '3000px');
              $('body').css('min-width', '4000px');
            } else {
              $(e.target).parents('.graph-table').css('width', '1000px');
              $('body').css('min-width', '100%');
            }
            console.log($(e.target).parents('.graph-table').css('width'));
          });
        });
        </script>
        </body>
        </html>
        ''',
)

html_dict.set_value(
    "sql_plan_monitor_report_footer_obversion4",
    '''
        <script>
        var detail_load = false;
        if (detail_serial_v1.length > 4000) {
           $('.load_detail_graph').click(function(){
                if (!detail_load) {
                  generate_graph("detail", detail_serial_v1, $('#detail_serial_v1'));
                  generate_graph("detail", detail_serial_v2, $('#detail_serial_v2'));
                  detail_load = true;
                }
                return true;
           });
        } else if (!detail_load) {
            generate_graph("detail", detail_serial_v1, $('#detail_serial_v1'));
            generate_graph("detail", detail_serial_v2, $('#detail_serial_v2'));
            detail_load = true;
        }
        
        $(function () {
          $('.b').popover({ trigger: 'focus' })
          $('.graphrow').mouseover(function(){
            $(this).find('span').show();
          });
          $('.graphrow').mouseleave(function(){
            $(this).find('span').hide();
          });
        })
        
        $(function() {
          $('.enlarge').click(function(e) {
            if ($(e.target).parents('.graph-table').css('width') == '1000px') {
              $(e.target).parents('.graph-table').css('width', '3000px');
              $('body').css('min-width', '4000px');
            } else {
              $(e.target).parents('.graph-table').css('width', '1000px');
              $('body').css('min-width', '100%');
            }
            console.log($(e.target).parents('.graph-table').css('width'));
          });
        });
        </script>
        
        <div id='debug' class="fixed"><a target='_blank' href='https://www.oceanbase.com/'>帮助文档</a> </div>
        </body>
        </html>
        ''',
)
