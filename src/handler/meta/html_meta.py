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
    .statsWarning {
      background-color: orange;
      width: 100%;
      text-align: left;
      padding: 5px;
      box-sizing: border-box;
      margin-top: 10px;
      z-index: 1000;
      position: relative;
    }
    /* 长文本收缩和复制功能样式 */
    .long-text-container {
      position: relative;
      display: inline-block;
      width: 100%;
    }
    
    .long-text {
      max-width: 300px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      cursor: pointer;
      transition: all 0.3s ease;
    }
    
    .long-text.expanded {
      max-width: none;
      white-space: normal;
      word-break: break-all;
    }
    
    .text-controls {
      position: absolute;
      top: -20px;
      right: 0;
      display: flex;
      gap: 5px;
      opacity: 0;
      transition: opacity 0.3s ease;
    }
    
    .long-text-container:hover .text-controls {
      opacity: 1;
    }
    
    .text-btn {
      background: #007bff;
      color: white;
      border: none;
      padding: 2px 6px;
      border-radius: 3px;
      font-size: 10px;
      cursor: pointer;
      transition: background 0.3s ease;
    }
    
    .text-btn:hover {
      background: #0056b3;
    }
    
    .copy-btn {
      background: #28a745;
    }
    
    .copy-btn:hover {
      background: #1e7e34;
    }
    
    .copy-success {
      background: #28a745 !important;
    }
    
    .copy-success::after {
      content: "✓";
      margin-left: 3px;
    }
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

      // 初始化长文本收缩和复制功能
      initLongTextFeatures();
    });

    // 长文本收缩和复制功能
    function initLongTextFeatures() {
      // 查找所有包含长SQL语句的td元素
      $('td').each(function() {
        var $td = $(this);
        var text = $td.text().trim();
        
        // 检查是否包含SQL语句（简单的判断条件）
        if (text.length > 50 && (text.toLowerCase().includes('select') || text.toLowerCase().includes('insert') || text.toLowerCase().includes('update') || text.toLowerCase().includes('delete'))) {
          // 创建长文本容器
          var $container = $('<div class="long-text-container"></div>');
          var $textDiv = $('<div class="long-text" title="点击展开/收缩">' + text + '</div>');
          var $controls = $('<div class="text-controls"></div>');
          var $expandBtn = $('<button class="text-btn expand-btn" title="展开/收缩">展开</button>');
          var $copyBtn = $('<button class="text-btn copy-btn" title="复制">复制</button>');
          
          $controls.append($expandBtn, $copyBtn);
          $container.append($textDiv, $controls);
          $td.html($container);
          
          // 绑定展开/收缩事件
          $expandBtn.click(function(e) {
            e.stopPropagation();
            var $text = $textDiv;
            var $btn = $(this);
            
            if ($text.hasClass('expanded')) {
              $text.removeClass('expanded');
              $btn.text('展开');
            } else {
              $text.addClass('expanded');
              $btn.text('收缩');
            }
          });
          
          // 绑定复制事件
          $copyBtn.click(function(e) {
            e.stopPropagation();
            var $btn = $(this);
            
            // 创建临时textarea来复制文本
            var textarea = document.createElement('textarea');
            textarea.value = text;
            document.body.appendChild(textarea);
            textarea.select();
            
            try {
              document.execCommand('copy');
              $btn.addClass('copy-success');
              $btn.text('已复制');
              
              setTimeout(function() {
                $btn.removeClass('copy-success');
                $btn.text('复制');
              }, 2000);
            } catch (err) {
              console.error('复制失败:', err);
              $btn.text('复制失败');
              
              setTimeout(function() {
                $btn.text('复制');
              }, 2000);
            }
            
            document.body.removeChild(textarea);
          });
          
          // 点击文本本身也可以展开/收缩
          $textDiv.click(function() {
            $expandBtn.click();
          });
        }
      });
    }

    
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
        ext_header = "<td width='4%'>估行</td>";
        ext_footer = "<td></td>";
        break;
      case "detail":
        ext_header = "<td width='4%'>RESCAN</td>";
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
        var row = "<tr><td width='4%'>" +  serial[i].tid + "</td><td width='10%'>" + "&nbsp;".repeat(serial[i].depth) + serial[i].op + "(" + serial[i].opid + ")</td>" + ext_data + "<td width='4%' style='text-align:right'>" +  serial[i].rows + "</td>";
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
    "sql_plan_monitor_report_header_obversion4",
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
    .load_detail_graph {text-decoration:underline;color:blue; cursor:pointer;}
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
    .dbtime {display:inline-block;}
    .statsWarning {
      background-color: orange;
      width: 100%;
      text-align: left;
      padding: 5px;
      box-sizing: border-box;
      margin-top: 10px;
      z-index: 1000;
      position: relative;
    }
    /* 长文本收缩和复制功能样式 */
    .long-text-container {
      position: relative;
      display: inline-block;
      width: 100%;
    }
    
    .long-text {
      max-width: 300px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
      cursor: pointer;
      transition: all 0.3s ease;
    }
    
    .long-text.expanded {
      max-width: none;
      white-space: normal;
      word-break: break-all;
    }
    
    .text-controls {
      position: absolute;
      top: -20px;
      right: 0;
      display: flex;
      gap: 5px;
      opacity: 0;
      transition: opacity 0.3s ease;
    }
    
    .long-text-container:hover .text-controls {
      opacity: 1;
    }
    
    .text-btn {
      background: #007bff;
      color: white;
      border: none;
      padding: 2px 6px;
      border-radius: 3px;
      font-size: 10px;
      cursor: pointer;
      transition: background 0.3s ease;
    }
    
    .text-btn:hover {
      background: #0056b3;
    }
    
    .copy-btn {
      background: #28a745;
    }
    
    .copy-btn:hover {
      background: #1e7e34;
    }
    
    .copy-success {
      background: #28a745 !important;
    }
    
    .copy-success::after {
      content: "✓";
      margin-left: 3px;
    }
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
      $('#ash_anchor').click(function () {
        $('#ash').toggle();
      });
      setTimeout(function() {
        $('#debug').hide();
      }, 30*1000);

      // 初始化长文本收缩和复制功能
      initLongTextFeatures();

    });

    // 长文本收缩和复制功能
    function initLongTextFeatures() {
      // 查找所有包含长SQL语句的td元素
      $('td').each(function() {
        var $td = $(this);
        var text = $td.text().trim();
        
        // 检查是否包含SQL语句（简单的判断条件）
        if (text.length > 50 && (text.toLowerCase().includes('select') || text.toLowerCase().includes('insert') || text.toLowerCase().includes('update') || text.toLowerCase().includes('delete'))) {
          // 创建长文本容器
          var $container = $('<div class="long-text-container"></div>');
          var $textDiv = $('<div class="long-text" title="点击展开/收缩">' + text + '</div>');
          var $controls = $('<div class="text-controls"></div>');
          var $expandBtn = $('<button class="text-btn expand-btn" title="展开/收缩">展开</button>');
          var $copyBtn = $('<button class="text-btn copy-btn" title="复制">复制</button>');
          
          $controls.append($expandBtn, $copyBtn);
          $container.append($textDiv, $controls);
          $td.html($container);
          
          // 绑定展开/收缩事件
          $expandBtn.click(function(e) {
            e.stopPropagation();
            var $text = $textDiv;
            var $btn = $(this);
            
            if ($text.hasClass('expanded')) {
              $text.removeClass('expanded');
              $btn.text('展开');
            } else {
              $text.addClass('expanded');
              $btn.text('收缩');
            }
          });
          
          // 绑定复制事件
          $copyBtn.click(function(e) {
            e.stopPropagation();
            var $btn = $(this);
            
            // 创建临时textarea来复制文本
            var textarea = document.createElement('textarea');
            textarea.value = text;
            document.body.appendChild(textarea);
            textarea.select();
            
            try {
              document.execCommand('copy');
              $btn.addClass('copy-success');
              $btn.text('已复制');
              
              setTimeout(function() {
                $btn.removeClass('copy-success');
                $btn.text('复制');
              }, 2000);
            } catch (err) {
              console.error('复制失败:', err);
              $btn.text('复制失败');
              
              setTimeout(function() {
                $btn.text('复制');
              }, 2000);
            }
            
            document.body.removeChild(textarea);
          });
          
          // 点击文本本身也可以展开/收缩
          $textDiv.click(function() {
            $expandBtn.click();
          });
        }
      });
    }

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
      var cpumax = 0;
      var cpumin = 999999999999999;
      for (var i = 0; i < serial.length; ++i) {
        if (serial[i].start > 0) {
          max = Math.max(max, serial[i].end);
          min = Math.min(min, serial[i].start);

          cpumax = Math.max(cpumax, serial[i].cpu+serial[i].io);
          cpumin = Math.min(cpumin, serial[i].cpu+serial[i].io);
        }
      }

      var total = max - min;
      var cputotal = cpumax - cpumin;

      // normalize

      for (var i = 0; i < serial.length; ++i) {
        if (serial[i].start > 0) {
            serial[i].start_relative = serial[i].start - min;
            serial[i].length = serial[i].end - serial[i].start;

            serial[i].a = Math.round(serial[i].start_relative * 100 / total);
            serial[i].b = Math.max(0.1, Math.round(serial[i].length * 100 / total));
            serial[i].c = Math.round(100 - serial[i].a - serial[i].b);

            serial[i].cpu_pct = serial[i].cpu * 100 / cputotal;
            serial[i].io_pct = serial[i].io * 100 / cputotal;
        }
      }
      console.log(topnode, "my", serial);


      var c1 = (undefined == serial[0] || serial[0].tag == 'op') ? '线程ID' : '线程数';
      var timeline_title = (undefined == serial[0] || serial[0].tag != 'db_time') ? '执行吐行时间线' : '耗时';
      var ext_header = "";
      var ext_footer = "";
      var table = "";
      switch(type) {
      case "dfo":
      ext_header = "<td>算子</td><td width='4%'>估行</td><td>吐行</td><td width='4%' title='(最大耗时-最小耗时)/最大耗时'>倾斜度</td>";
        table = "<table class='graph-table'><tr class='lastline' style='line-height:1.4em;'><td class='b'>" + c1 + "</td>" + ext_header + "<td>DBTime</td><td>" +  timeline_title + "</td><td>Ext</td></tr>";
      ext_footer = "<td></td><td></td><td></td>";
      break;
      case "sqc":
      ext_header = "<td>算子</td><td>吐行</td><td width='4%' title='(最大耗时-最小耗时)/最大耗时'>倾斜度</td>";
        table = "<table class='graph-table'><tr class='lastline' style='line-height:1.4em;'><td class='b'>" + c1 + "</td>" + ext_header + "<td>DBTime</td><td>" +  timeline_title + "</td><td>Ext</td></tr>";
      ext_footer = "<td></td><td></td>";
        break;
      case "detail":
      ext_header = "<td>算子</td><td width='4%'>RESCAN</td><td>吐行</td>";
        table = "<table class='graph-table'><tr class='lastline' style='line-height:1.4em;'><td class='b'>" + c1 + "</td>" + ext_header + "<td>DBTime</td><td>" +  timeline_title + "</td><td>Ext</td></tr>";
      ext_footer = "<td></td><td></td></td>";
      break;
      default:
      break;
      }
      for (var i = 0; i < serial.length - 1; ++i) {
        var skew_highlight = '';
        if (total > 0 && serial[i].length / total > 0.2 && serial[i].skewness > 0.3)
            skew_highlight = 'color:red;';
        var row = "<tr><td width='4%'>" +  serial[i].tid + "</td><td width='10%'>" + "&nbsp;".repeat(serial[i].depth) + serial[i].op + "(" + serial[i].opid + ")</td>";
        row += (serial[i].est_rows === undefined ? "" : "<td>" + serial[i].est_rows + "</td>");
        row += (serial[i].rescan === undefined ? "" : "<td>" + serial[i].rescan + "</td>");
        row += "<td width='4%' style='text-align:right'>" +  serial[i].rows + "</td>";
        row += (serial[i].skewness === undefined ? "" : "<td style='text-align:right;" + skew_highlight + "'>" + serial[i].skewness+ "</td>");
        if (serial[i].cpu + serial[i].io > 0) {
          row += "<td width='16%'><div class='dbtime b graphrow' style='width:" + serial[i].cpu_pct + "%;background-color:#339933'><span class='diff'>" + serial[i].cpu + "s</span></div><div class='dbtime b graphrow' style='width:" + serial[i].io_pct + "%;background-color:red'><span class='diff'>" + serial[i].io + "s</span></div> </td>";
        } else {
          row += "<td><div class='empty' style='width:100%;'></div></td>";
        }
        if (serial[i].start > 0) {
          row += "<td width='64%'><div tabindex='" + i + "' class='b graphrow' data-toggle='popover' data-trigger='focus' data-placement='bottom' data-content='" + JSON.stringify(serial[i]).replace(/,/g,'\\n') + "' style='margin-left:" + serial[i].a + "%;width:" + serial[i].b + "%;background-color:" + colors[serial[i].opid] + "' title='" + serial[i].op + "(" + serial[i].diff + "s, " + serial[i].svr_ip +")'><span class='diff'>" +  serial[i].op  + "(" + serial[i].opid + ")" + ' ' + serial[i].diff + "s</span></div></td>";
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
      table += "<tr><td class='lastline'><button style='line-height:1.4em' class='enlarge'>缩放</button></td>"+ext_footer+"<td>总时间</td><td class='b' style='text-align:center;'>" + (Math.round(cputotal * 1000000) / 1000000.0) + "s</td><td class='b' style='text-align:center;'>" + (Math.round(total * 1000000) / 1000000.0) + "s</td></tr></table>"
      topnode.get(0).innerHTML = table;
    }


    function generate_db_time_graph(type, serial, topnode) {
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

            serial[i].a = Math.round(serial[i].my_cpu_time * 100 / total);
            serial[i].b = Math.round(serial[i].my_io_time * 100 / total);
        }
      }
      console.log(topnode, "my", serial);


      var c1 = (undefined == serial[0] || serial[0].tag == 'op') ? '线程ID' : '线程数';
      var timeline_title = (undefined == serial[0] || serial[0].tag != 'db_time') ? '执行吐行时间线' : '耗时';
      var ext_header = "";
      var ext_footer = "";
      switch(type) {
      case "dfo":
      ext_header = "<td width='4%'>估行</td>";
      ext_footer = "<td></td>";
      break;
      case "detail":
      ext_header = "<td width='4%'>RESCAN</td>";
      ext_footer = "<td></td>";
      break;
      default:
      break;
      }
      var table = "<table class='graph-table'><tr class='lastline' style='line-height:1.4em;'><td class='b'>" + c1 + "</td><td>算子</td>" + ext_header + "<td>吐行</td><td>" + timeline_title + "</td><td>Ext</td></tr>";
      for (var i = 0; i < serial.length - 1; ++i) {
        var ext_data = "";
        ext_data += (serial[i].est_rows === undefined ? "" : "<td>" + serial[i].est_rows + "</td>");
        ext_data += (serial[i].rescan === undefined ? "" : "<td>" + serial[i].rescan + "</td>");
        var row = "<tr><td width='4%'>" +  serial[i].tid + "</td><td width='10%'>" + "&nbsp;".repeat(serial[i].depth) + serial[i].op + "(" + serial[i].opid + ")</td>" + ext_data + "<td width='4%' style='text-align:right'>" +  serial[i].rows + "</td>";
        if (serial[i].diff > 0) {
          row += "<td width='80%'><div tabindex='" + i + "' class='dbtime b graphrow' data-toggle='popover' data-trigger='focus' data-placement='bottom' style='width:" + serial[i].a + "%;background-color:#339933' title='" + serial[i].op + "(cpu_time=" + serial[i].my_cpu_time + "s)'><span class='diff'>" +  serial[i].op  + "(" + serial[i].opid + ") cpu_time=" + serial[i].my_cpu_time + "s</span></div><div tabindex='" + i + "' class='dbtime b graphrow' data-toggle='popover' data-trigger='focus' data-placement='bottom' style='width:" + serial[i].b + "%;background-color:red' title='" + serial[i].op + "(io_wait=" + serial[i].my_io_time + "s)'><span class='diff'>" +  serial[i].op  + "(" + serial[i].opid + ") io_wait_time=" + serial[i].my_io_time + "s</span></div> </td>";
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

html_dict.set_value(
    "sql_review_html_head_template",
    '''
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <title>SQL Review报告</title>
    <style>
        table {
            width: 100%;
            border-collapse: collapse;
        }

        th,
        td {
            border: 1px solid black;
            padding: 8px;
            text-align: left;
        }

        th {
            background-color: #f2f2f2;
            font-weight: bold;
        }

        .merge {
            background-color: #f9f9f9;
        }

        .critical {
            color: red;
        }

        .warn {
            color: orange;
        }

        .notice {
            color: goldenrod;
        }
        .ok {
            color: green;
        }
    </style>
</head>
</body>
    <h1>SQL Review报告</h1>
    ''',
)


html_dict.set_value(
    "analyze_sql_html_head_template",
    '''
<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <title>租户SQL诊断报告</title>
    <style>
        table,
        th,
        td {
            border: 1px solid black;
            border-collapse: collapse;
            padding: 8px;
            text-align: left;
        }

        th {
            background-color: #f2f2f2;
            font-weight: bold;
        }

        .merge {
            background-color: #f9f9f9;
        }

        .critical {
            color: red;
        }

        .warn {
            color: orange;
        }

        .notice {
            color: goldenrod;
        }

        .ok {
            color: green;
        }

        tr+tr td:first-child {
            /* 为非首行的第一个单元格设置边框 */
            border-top: none;
        }

        .markdown-code-block {
            background-color: #f8f8f8;
            /* 背景颜色，模拟代码块背景 */
            border: 1px solid #ddd;
            /* 边框，模拟代码块边框 */
            padding: 16px;
            /* 内边距，使代码块内容与边框有一定距离 */
            overflow-x: auto;
            /* 自动滚动条，如果内容过宽 */
            font-family: 'Courier New', monospace;
            /* 使用等宽字体，模拟代码字体 */
            font-size: 0.9em;
            /* 字体大小，可根据需要调整 */
            max-height: 500px;
            overflow-y: auto;
        }
      /* 添加此规则以防止倒数三列的单元格内容换行 */
      td:last-child, td:nth-last-child(2), td:nth-last-child(3) {
          white-space: nowrap;
      }
      /* 添加滚动条到倒数第五列 */
      td:nth-last-child(5) {
          width: 300px;
          /* 设置列宽 */
          overflow: auto;
          /* 显示滚动条 */
      }
        #collapsibleSection h3.header {
            cursor: pointer;
            color: blue;
            /* Change text color to a light blue */
            text-decoration: none;
            /* Remove underline by default */
            transition: color 0.3s, text-decoration 0.3s;
        }

        #collapsibleSection h3.header:hover {
            color: blue;
            /* Change text color to a darker blue on hover */
            text-decoration: underline;
            /* Underline the text on hover */
        }

        #collapsibleSection h3.header.active {
            background-color: blue;
            /* Change background color to blue */
            color: white;
            /* Change text color to white */
        }

        #collapsibleSection .content {
            max-height: 0;
            overflow: hidden;
            transition: max-height 0.5s ease-out;
        }

        #collapsibleSection.expanded .content {
            max-height: 500px;
            /* Adjust this value based on your content size */
            transition: max-height 0.5s ease-in;
        }
    </style>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
</head>

</body>
    <h1>SQL Diagnostic Result</h1>
    ''',
)

html_dict.set_value(
    "html_footer_temple",
    '''
</body>

</html>
    ''',
)

html_dict.set_value(
    "html_script_templete",
    '''
  <script>
    $(document).ready(function () {
        $("#collapsibleSection h3.header").click(function () {
            $(this).toggleClass("active");
            $(this).parent().toggleClass("expanded");
        });
    });
  </script>
  ''',
)
